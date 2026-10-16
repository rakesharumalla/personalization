# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Personalization — review notebook
# MAGIC
# MAGIC One place to inspect every layer of the build:
# MAGIC 1. Jobs & pipeline status
# MAGIC 2. Unity Catalog volumes and landing files
# MAGIC 3. Sample records — bronze, silver, gold (note the masked `email`)
# MAGIC 4. Lakebase tables (synced + app-owned)
# MAGIC 5. Model Serving — query the recommender (champion & challenger)
# MAGIC 6. Vector Search — semantic product lookup
# MAGIC 7. Genie — the seeded business questions
# MAGIC 8. Experiment results & offline evaluation
# MAGIC
# MAGIC Run on **serverless** compute. Read-only: nothing here modifies data.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.150.0" "psycopg[binary]>=3.2"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "fevm_catalog_ra_test")
dbutils.widgets.text("lakebase_project", "pers-serving")
dbutils.widgets.text("serving_endpoint", "pers-recommender")
dbutils.widgets.text("genie_title", "Personalization Insights")
C = dbutils.widgets.get("catalog")
PROJECT = dbutils.widgets.get("lakebase_project")
ENDPOINT = dbutils.widgets.get("serving_endpoint")
GENIE_TITLE = dbutils.widgets.get("genie_title")

import json
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
print("workspace:", w.config.host, "| catalog:", C)

# COMMAND ----------

# MAGIC %md ## 1. Jobs & pipeline status

# COMMAND ----------

rows = []
for j in w.jobs.list(name=None):
    name = j.settings.name or ""
    if "pers-" not in name:
        continue
    runs = list(w.jobs.list_runs(job_id=j.job_id, limit=1))
    r = runs[0] if runs else None
    rows.append({
        "job": name, "job_id": j.job_id,
        "schedule": (j.settings.schedule.quartz_cron_expression + " (" + j.settings.schedule.pause_status.value + ")")
                    if j.settings.schedule else "manual",
        "last_run_state": (r.state.result_state.value if r and r.state.result_state else
                           (r.state.life_cycle_state.value if r else "never run")),
        "last_run_start": pd.to_datetime(r.start_time, unit="ms") if r else None,
        "last_run_url": r.run_page_url if r else None,
    })
display(pd.DataFrame(rows).sort_values("job"))

# COMMAND ----------

pipes = []
for p in w.pipelines.list_pipelines(filter="name LIKE '%pers-personalization-pipeline%'"):
    for u in (p.latest_updates or [])[:3]:
        pipes.append({"pipeline": p.name, "state": p.state.value if p.state else None,
                      "update_state": u.state.value if u.state else None, "update_created": u.creation_time})
display(pd.DataFrame(pipes))

# COMMAND ----------

# MAGIC %md ## 2. Unity Catalog volumes & landing files

# COMMAND ----------

vols = [{"volume": v.full_name, "type": v.volume_type.value, "comment": v.comment}
        for s in ("bronze", "ml") for v in w.volumes.list(C, s)]
display(pd.DataFrame(vols))

# COMMAND ----------

landing = f"/Volumes/{C}/bronze/raw_landing"
summary = []
for d in dbutils.fs.ls(landing):
    files = dbutils.fs.ls(d.path)
    summary.append({"folder": d.name, "files": len(files), "bytes": sum(f.size for f in files),
                    "latest_file": max((f.name for f in files), default=None)})
display(pd.DataFrame(summary))

# COMMAND ----------

# MAGIC %md ## 3. Sample records — bronze / silver / gold
# MAGIC `email` is tagged `pii=email` and masked by `silver.mask_email` unless you are in `pers_engineers`.

# COMMAND ----------

def show(table, n=5, order=None):
    full = f"{C}.{table}"
    df = spark.table(full)
    print(f"{full}: {df.count():,} rows")
    if order:
        df = df.orderBy(order, ascending=False)
    display(df.limit(n))

# COMMAND ----------

show("bronze.clickstream_events", order="_ingest_ts")

# COMMAND ----------

show("bronze.users_cdc")

# COMMAND ----------

show("bronze.quarantine_events")

# COMMAND ----------

show("silver.events", order="ts")

# COMMAND ----------

# SCD2 history: users with more than one version (loyalty tier upgrades); email masked
display(spark.sql(f"""
  SELECT user_id, email, loyalty_tier, country, __START_AT, __END_AT
  FROM {C}.silver.users
  WHERE user_id IN (SELECT user_id FROM {C}.silver.users GROUP BY 1 HAVING count(*) > 1 LIMIT 3)
  ORDER BY user_id, __START_AT"""))

# COMMAND ----------

show("silver.sessions", order="session_start")

# COMMAND ----------

show("gold.user_features")

# COMMAND ----------

show("gold.product_features")

# COMMAND ----------

show("gold.session_kpis_daily", n=10, order="session_date")

# COMMAND ----------

show("gold.precomputed_recs")

# COMMAND ----------

show("gold.recommendation_outcomes")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {C}.gold.pipeline_health ORDER BY snapshot_ts DESC LIMIT 5"))

# COMMAND ----------

# MAGIC %md ## 4. Lakebase tables
# MAGIC Synced from gold (`serving.*`) and written directly by the app (`app.*`).

# COMMAND ----------

import psycopg

EP = f"projects/{PROJECT}/branches/production/endpoints/primary"
host = w.postgres.get_endpoint(EP).status.hosts.host
token = w.postgres.generate_database_credential(endpoint=EP).token
conn = psycopg.connect(host=host, dbname="databricks_postgres", user=w.current_user.me().user_name,
                       password=token, sslmode="require", autocommit=True)


def pg(sql, params=None):
    cur = conn.execute(sql, params)
    return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])


display(pg("""SELECT table_schema, table_name,
                     (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %I.%I', table_schema, table_name), false, true, '')))[1]::text::bigint AS row_count
              FROM information_schema.tables WHERE table_schema IN ('serving', 'app') ORDER BY 1, 2"""))

# COMMAND ----------

display(pg("SELECT * FROM serving.user_features_online WHERE has_history LIMIT 5"))

# COMMAND ----------

display(pg("SELECT product_id, title, category, brand, price, price_tier, popularity_score FROM serving.product_catalog ORDER BY popularity_score DESC LIMIT 5"))

# COMMAND ----------

display(pg("SELECT user_id, left(recs_json, 200) AS recs_json_preview, model_version, generated_ts FROM serving.precomputed_recs WHERE user_id LIKE 'segment:%%'"))

# COMMAND ----------

display(pg("SELECT * FROM app.session_state ORDER BY ts DESC LIMIT 10"))

# COMMAND ----------

display(pg("""SELECT event, served_by, model_variant, count(*) AS n, round(avg(latency_ms)::numeric, 1) AS avg_latency_ms
              FROM app.recommendation_log GROUP BY 1, 2, 3 ORDER BY n DESC"""))

# COMMAND ----------

display(pg("SELECT * FROM app.explanation_cache ORDER BY created_ts DESC LIMIT 5"))

# COMMAND ----------

# Point-lookup latency (what the app pays per request)
import time
uid = pg("SELECT user_id FROM serving.user_features_online WHERE has_history LIMIT 1").user_id[0]
lat = []
for _ in range(20):
    t = time.perf_counter()
    conn.execute("SELECT * FROM serving.user_features_online WHERE user_id = %s", (uid,)).fetchone()
    lat.append((time.perf_counter() - t) * 1000)
print(f"feature lookup from this notebook: p50={sorted(lat)[10]:.1f} ms  max={max(lat):.1f} ms")

# COMMAND ----------

# MAGIC %md ## 5. Model Serving — query the recommender

# COMMAND ----------

ep = w.serving_endpoints.get(ENDPOINT)
print("endpoint:", ep.name, "| ready:", ep.state.ready, "| served:",
      [(e.name, e.entity_name, e.entity_version) for e in ep.config.served_entities])

uf = pg("SELECT * FROM serving.user_features_online WHERE has_history AND top_category = 'electronics' LIMIT 1").iloc[0].to_dict()
uf = {k: (list(v) if isinstance(v, list) else (str(v) if hasattr(v, "isoformat") else v)) for k, v in uf.items()}
session_items = pg("SELECT product_id FROM serving.product_catalog WHERE category = 'sports_outdoors' LIMIT 2").product_id.tolist()
catalog = pg("SELECT product_id, title, category, price FROM serving.product_catalog").set_index("product_id")


def ask_model(variant, user_features, items):
    path = f"/serving-endpoints/{ENDPOINT}/" + ("invocations" if variant == "champion" else f"served-models/{variant}/invocations")
    t = time.perf_counter()
    r = w.api_client.do("POST", path, body={"dataframe_records": [{
        "user_id": user_features.get("user_id", "NEW"), "session_items": json.dumps(items),
        "user_features": json.dumps(user_features, default=str), "k": 8}]})
    ms = (time.perf_counter() - t) * 1000
    pred = r["predictions"][0]
    pred = pred.get("predictions", pred) if isinstance(pred, dict) else pred
    recs = pd.DataFrame(json.loads(pred)["recommendations"]).join(catalog, on="product_id")
    print(f"{variant}: {ms:.0f} ms round trip")
    return recs[["product_id", "title", "category", "price", "score", "source"]]


print("shopper", uf["user_id"], "likes", uf["top_category"], "| session just viewed:", catalog.loc[session_items, "title"].tolist())
display(ask_model("champion", uf, session_items))

# COMMAND ----------

display(ask_model("challenger", uf, session_items))

# COMMAND ----------

# Cold start: no features at all, only what was clicked in this session
display(ask_model("champion", {}, session_items))

# COMMAND ----------

# Inference tables written by AI Gateway
for t in spark.catalog.listTables(f"{C}.ml"):
    if t.name.startswith("recommender"):
        print(t.name)
        display(spark.table(f"{C}.ml.{t.name}").orderBy("request_time", ascending=False).limit(3))

# COMMAND ----------

# MAGIC %md ## 6. Vector Search — semantic catalog lookup

# COMMAND ----------

res = w.vector_search_indexes.query_index(index_name=f"{C}.gold.product_index",
                                          columns=["product_id", "title", "category", "price"],
                                          query_text="gift for a trail runner", num_results=5)
display(pd.DataFrame(res.result.data_array, columns=[c.name for c in res.manifest.columns]))

# COMMAND ----------

# MAGIC %md ## 7. Genie — seeded business questions

# COMMAND ----------

space = next(s for s in w.genie.list_spaces().spaces if s.title == GENIE_TITLE)
print("Genie space:", space.space_id, f"{w.config.host}/genie/rooms/{space.space_id}")


def ask_genie(q):
    m = w.genie.start_conversation_and_wait(space.space_id, q)
    print("Q:", q)
    for a in m.attachments or []:
        if a.text:
            print("A:", a.text.content)
        if a.query:
            print("SQL:\n", a.query.query)
            sr = w.genie.get_message_attachment_query_result(space.space_id, m.conversation_id, m.id, a.attachment_id).statement_response
            display(pd.DataFrame(sr.result.data_array or [], columns=[c.name for c in sr.manifest.schema.columns]))


ask_genie("How do cold-start users convert vs. returning users?")

# COMMAND ----------

ask_genie("Which product categories have the highest recommendation CTR this week?")

# COMMAND ----------

ask_genie("Show me the lift of the challenger model over champion by country")

# COMMAND ----------

# MAGIC %md ## 8. Experiment results & offline evaluation

# COMMAND ----------

display(spark.sql(f"""
  SELECT model_variant, is_cold_start, sum(sessions) AS sessions,
         round(sum(clicks) / sum(impressions), 4) AS ctr,
         round(sum(converted_sessions) / sum(sessions), 4) AS conversion,
         round(sum(revenue) / sum(converted_sessions), 2) AS aov
  FROM {C}.gold.experiment_results GROUP BY ALL ORDER BY is_cold_start, model_variant"""))

# COMMAND ----------

display(spark.table(f"{C}.ml.offline_evaluation"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {C}.gold.simulate_campaign('electronics', 'champion', 10)"))

# COMMAND ----------

conn.close()
