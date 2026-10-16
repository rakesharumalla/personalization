"""Batch inference -> gold.precomputed_recs (synced to Lakebase as the fallback path).

One row per known user (homepage recs, empty session) plus one row per segment
key (``segment:<acquisition_channel>`` and ``segment:global``) holding popular
items for cold-start shoppers when Model Serving is slow or unavailable.
"""
import argparse
import json
import sys

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--root", default=None)
ap.add_argument("--k", type=int, default=20)
args, _ = ap.parse_known_args()
if args.root:
    sys.path.insert(0, args.root)

import mlflow  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ml.recommender import parse_user_features, popularity_recs, recommend  # noqa: E402

C = args.catalog
spark = SparkSession.builder.getOrCreate()
mlflow.set_registry_uri("databricks-uc")
name = f"{C}.ml.recommender"
mv = mlflow.MlflowClient().get_model_version_by_alias(name, "champion")
model = mlflow.pyfunc.load_model(f"models:/{name}@champion").unwrap_python_model()  # load_context already ran

uf = spark.read.table(f"{C}.gold.user_features").toPandas()
rows = []
for r in uf.to_dict("records"):
    r["last_items"] = list(r["last_items"]) if r["last_items"] is not None else []
    f = parse_user_features(r)
    recs = recommend(model.art, model.booster, model.features, f, [], k=args.k)
    rows.append({"user_id": r["user_id"], "recs_json": json.dumps(recs), "rec_type": "user"})

pop = popularity_recs(model.art, args.k)
rows.append({"user_id": "segment:global", "recs_json": json.dumps(pop), "rec_type": "segment"})
# per acquisition channel: most popular among that channel's users' recent items
chan = spark.sql(f"""
    SELECT u.acquisition_channel AS ch, e.product_id, count(*) AS n
    FROM {C}.silver.events e JOIN {C}.gold.user_features u USING (user_id)
    WHERE e.product_id IS NOT NULL AND e.ts >= current_timestamp() - INTERVAL 14 DAYS
    GROUP BY 1, 2""").toPandas()
for ch, g in chan.groupby("ch"):
    top = g.sort_values("n", ascending=False).product_id.head(args.k).tolist()
    recs = [{"product_id": p, "score": float(n), "source": "segment_popular", "signals": {}}
            for p, n in zip(top, g.sort_values("n", ascending=False).n.head(args.k))]
    rows.append({"user_id": f"segment:{ch}", "recs_json": json.dumps(recs), "rec_type": "segment"})

df = (spark.createDataFrame(pd.DataFrame(rows))
      .withColumn("model_version", F.lit(str(mv.version)))
      .withColumn("generated_ts", F.current_timestamp()))
table = f"{C}.gold.precomputed_recs"
if not spark.catalog.tableExists(table):
    df.limit(0).write.saveAsTable(table)
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN user_id SET NOT NULL")
    spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT precomputed_recs_pk PRIMARY KEY (user_id)")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    spark.sql(f"COMMENT ON TABLE {table} IS 'Champion top-{args.k} per user + segment popularity; "
              "Lakebase fallback when Model Serving is slow/unavailable'")
df.createOrReplaceTempView("src")
spark.sql(f"""MERGE INTO {table} t USING src s ON t.user_id = s.user_id
              WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
              WHEN NOT MATCHED BY SOURCE THEN DELETE""")
print("precomputed recs:", len(rows), "model version", mv.version)
