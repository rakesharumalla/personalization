"""Reverse-sync the app's operational recommendation_log (Lakebase, native Postgres)
into Unity Catalog (gold.recommendation_log) for analytics and lineage.

Incremental on log_id: only rows newer than the max already exported are pulled.
"""
import argparse

import pandas as pd
import psycopg
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--project", default="pers-serving")
ap.add_argument("--root", default=None)
args, _ = ap.parse_known_args()
spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()
EP = f"projects/{args.project}/branches/production/endpoints/primary"
table = f"{args.catalog}.gold.recommendation_log"

hwm = 0
if spark.catalog.tableExists(table):
    hwm = spark.sql(f"SELECT coalesce(max(log_id), 0) FROM {table}").first()[0]
host = w.postgres.get_endpoint(EP).status.hosts.host
token = w.postgres.generate_database_credential(endpoint=EP).token
with psycopg.connect(host=host, dbname="databricks_postgres", user=w.current_user.me().user_name,
                     password=token, sslmode="require") as conn:
    has = conn.execute("SELECT to_regclass('app.recommendation_log')").fetchone()[0]
    if not has:
        print("app.recommendation_log does not exist yet (app not started) — nothing to export")
        raise SystemExit(0)
    cur = conn.execute("""SELECT log_id, request_id, ts, user_id, session_id, event, product_id, position,
                                 model_variant, source, latency_ms, served_by, app_user
                          FROM app.recommendation_log WHERE log_id > %s ORDER BY log_id LIMIT 500000""", (hwm,))
    cols = [d.name for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
print("new rows", len(df), "after log_id", hwm)
if len(df):
    sdf = spark.createDataFrame(df)
    sdf.write.mode("append").option("mergeSchema", "true").saveAsTable(table)
    spark.sql(f"COMMENT ON TABLE {table} IS 'Impression/click/purchase log written by the Databricks App to "
              "Lakebase (app.recommendation_log) and exported incrementally to UC.'")
