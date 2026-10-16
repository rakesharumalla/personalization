"""Simulated A/B experiment: popularity baseline vs. personalized (champion / challenger).

Each simulated session starts with the shopper's first organic views (the
real-time session signal), then a 10-slot recommendation rail is rendered by the
session's arm. Clicks and purchases are drawn from the simulator's hidden
ground-truth preferences, so lift is *earned* by ranking quality.

Arms: control 50% -> popularity, treatment 50% -> champion 25% / challenger 25%.

Writes gold.recommendation_outcomes (impression grain) and
gold.experiment_results (day x variant x segment x country x cold-start).
"""
import argparse
import hashlib
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--root", default=None)
ap.add_argument("--days", type=int, default=14)
ap.add_argument("--sessions_per_day", type=int, default=900)
ap.add_argument("--products", type=int, default=2000)
ap.add_argument("--seed", type=int, default=7)
args, _ = ap.parse_known_args()
if args.root:
    sys.path.insert(0, args.root)

import mlflow  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from data_gen.simulator import (COLD_START_SHARE, World, make_products, make_users,  # noqa: E402
                                organic_conversion_prob, respond_to_recs, simulate_session, stable_rng)
from ml.recommender import parse_user_features, popularity_recs, recommend  # noqa: E402

C = args.catalog
spark = SparkSession.builder.getOrCreate()
mlflow.set_registry_uri("databricks-uc")
models = {a: mlflow.pyfunc.load_model(f"models:/{C}.ml.recommender@{a}").unwrap_python_model()
          for a in ("champion", "challenger")}
art = models["champion"].art

truth_df = spark.read.table(f"{C}.ml.sim_user_truth").toPandas()
truth = {r.user_id: {"persona": r.persona, "affinity": json.loads(r.affinity_json),
                     "price_sensitivity": r.price_sensitivity, "fav_brands": list(r.fav_brands),
                     "activity": r.activity} for r in truth_df.itertuples()}
world = World(products=make_products(args.products, args.seed), users=[], truth=truth)
price_of = {p["product_id"]: p["price"] for p in world.products}
cat_of = {p["product_id"]: p["category"] for p in world.products}

uf = spark.read.table(f"{C}.gold.user_features").toPandas()
uf = uf[uf.has_history & uf.user_id.isin(truth.keys())]
uf_records = {r["user_id"]: r for r in uf.to_dict("records")}
returning_ids = np.array(list(uf_records))
act = np.array([truth[u]["activity"] for u in returning_ids])
act = act / act.sum()

today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
imp_rows = []
new_index = 5_000_000
for d in range(args.days, 0, -1):
    day = today - timedelta(days=d - 1)
    rng = stable_rng("ab", args.seed, day.date())
    for s in range(args.sessions_per_day):
        start = day + timedelta(seconds=float(rng.uniform(0, 86_000)))
        sid = uuid.UUID(int=int(rng.integers(0, 2**63))).hex
        bucket = int(hashlib.md5(sid.encode()).hexdigest(), 16) % 100
        variant = "popularity" if bucket < 50 else ("champion" if bucket < 75 else "challenger")
        cold = rng.random() < COLD_START_SHARE
        if cold:
            nu, nt = make_users(1, world.products, args.seed + 1234, start_index=new_index,
                                signup_start=start - timedelta(minutes=3), signup_end=start)
            new_index += 1
            world.truth.update(nt)
            u = nu[0]
            uid, feats = u["user_id"], parse_user_features({})
            segment, country, channel = "new", u["country"], u["acquisition_channel"]
        else:
            uid = str(returning_ids[int(rng.choice(len(returning_ids), p=act))])
            r = dict(uf_records[uid])
            r["last_items"] = list(r["last_items"]) if r["last_items"] is not None else []
            feats = parse_user_features(r)
            segment, country, channel = r["segment"], r["country"], r["acquisition_channel"]
        # the shopper's first 1-3 organic product views form the real-time session context
        events, _ = simulate_session(world, uid, start, rng, is_cold_start=cold)
        views = [e["product_id"] for e in events if e["event_type"] == "view"]
        ctx = list(dict.fromkeys(reversed(views[: int(rng.integers(1, 4))])))
        if variant == "popularity":
            recs = popularity_recs(art, 10, exclude=set(ctx))
        else:
            m = models[variant]
            recs = recommend(m.art, m.booster, m.features, feats, ctx, k=10)
        resp = respond_to_recs(world, uid, [r["product_id"] for r in recs], rng, cold)
        organic = rng.random() < organic_conversion_prob(cold)
        organic_rev = float(price_of[views[-1]]) if (organic and views) else 0.0
        for x in resp:
            imp_rows.append({
                "impression_id": f"{sid}-{x['position']}", "session_id": sid, "event_date": start.date(),
                "ts": start, "user_id": uid, "model_variant": variant,
                "experiment_arm": "control" if variant == "popularity" else "treatment",
                "segment": segment, "country": country, "acquisition_channel": channel, "is_cold_start": cold,
                "product_id": x["product_id"], "category": cat_of[x["product_id"]], "position": x["position"],
                "clicked": x["clicked"], "purchased": x["purchased"],
                "revenue": round(price_of[x["product_id"]], 2) if x["purchased"] else 0.0,
                "organic_converted": organic, "organic_revenue": organic_rev if x["position"] == 0 else 0.0,
            })
    print(day.date(), "impressions so far", len(imp_rows))

out = pd.DataFrame(imp_rows)
(spark.createDataFrame(out).write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{C}.gold.recommendation_outcomes"))
spark.sql(f"COMMENT ON TABLE {C}.gold.recommendation_outcomes IS 'Impression-level outcomes of recommendation "
          "rails (simulated A/B). One row per (session, slot). clicked/purchased are per impression; "
          "organic_converted marks sessions that converted without the rail.'")

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.experiment_results
COMMENT 'Daily A/B results by model_variant (popularity = control baseline; champion/challenger = personalized). \
ctr = clicks/impressions; conversion_rate = converted_sessions/sessions; aov = revenue/converted_sessions'
AS
WITH s AS (
  SELECT session_id, event_date, model_variant, experiment_arm, segment, country, is_cold_start,
         count(*) AS impressions, sum(CAST(clicked AS INT)) AS clicks,
         max(CAST(purchased AS INT)) AS rec_converted, sum(revenue) AS rec_revenue,
         max(CAST(organic_converted AS INT)) AS organic_converted, sum(organic_revenue) AS organic_revenue
  FROM {C}.gold.recommendation_outcomes GROUP BY ALL)
SELECT event_date, model_variant, experiment_arm, segment, country, is_cold_start,
       count(*) AS sessions, sum(impressions) AS impressions, sum(clicks) AS clicks,
       sum(rec_converted) AS rec_attributed_conversions,
       sum(greatest(rec_converted, organic_converted)) AS converted_sessions,
       round(sum(rec_revenue + organic_revenue), 2) AS revenue,
       round(sum(clicks) / sum(impressions), 5) AS ctr,
       round(sum(greatest(rec_converted, organic_converted)) / count(*), 5) AS conversion_rate,
       round(sum(rec_revenue + organic_revenue) / greatest(sum(greatest(rec_converted, organic_converted)), 1), 2) AS aov
FROM s GROUP BY ALL""")

summary = spark.sql(f"""
SELECT model_variant, is_cold_start, sum(sessions) sessions, round(sum(clicks)/sum(impressions),4) ctr,
       round(sum(converted_sessions)/sum(sessions),4) conversion, round(sum(revenue)/sum(converted_sessions),2) aov
FROM {C}.gold.experiment_results GROUP BY ALL ORDER BY 2, 1""").toPandas()
print(summary.to_string())
