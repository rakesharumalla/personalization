"""Feature refresh: gold.user_features, gold.product_features, gold.user_segments.

The two feature tables are Delta tables with a PRIMARY KEY (=> UC feature
tables) and Change Data Feed enabled (=> Lakebase continuous/triggered sync and
Vector Search delta sync). They are upserted with MERGE + a row hash so only
rows that actually changed flow downstream.
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--root", default=None)
args, _ = ap.parse_known_args()
C = args.catalog
spark = SparkSession.builder.getOrCreate()

EVENT_WEIGHT = F.expr("CASE event_type WHEN 'purchase' THEN 5.0 WHEN 'add_to_cart' THEN 3.0 ELSE 1.0 END")

ev = spark.read.table(f"{C}.silver.events").where("product_id IS NOT NULL")
prod = spark.read.table(f"{C}.silver.products")
orders = spark.read.table(f"{C}.silver.orders")
users = spark.read.table(f"{C}.silver.users").where("__END_AT IS NULL").drop("email")
sessions = spark.read.table(f"{C}.silver.sessions")
now = F.current_timestamp()

# ----------------------------------------------------------------------------- product features
p_stats = (ev.where(F.col("ts") >= now - F.expr("INTERVAL 30 DAYS"))
           .groupBy("product_id")
           .agg(F.sum(F.expr("CAST(event_type = 'view' AS INT)")).alias("views_30d"),
                F.sum(F.expr("CAST(event_type = 'add_to_cart' AS INT)")).alias("carts_30d"),
                F.sum(F.expr("CAST(event_type = 'purchase' AS INT)")).alias("purchases_30d")))
cat_median = prod.groupBy("category").agg(F.percentile_approx("price", 0.5).alias("cat_median_price"))
product_features = (
    prod.join(p_stats, "product_id", "left").join(cat_median, "category", "left")
    .na.fill(0, ["views_30d", "carts_30d", "purchases_30d"])
    .withColumn("tags", F.array_join("tags", ","))
    .withColumn("price_tier", F.when(F.col("price") < F.col("cat_median_price") * 0.75, "budget")
                .when(F.col("price") > F.col("cat_median_price") * 1.4, "premium").otherwise("mid"))
    .withColumn("popularity_score", F.round(F.log1p(F.col("views_30d") + 3 * F.col("purchases_30d")), 4))
    .withColumn("conversion_rate", F.round((F.col("purchases_30d") + 0.1) / (F.col("views_30d") + 10), 5))
    .withColumn("search_text", F.concat_ws(". ", "title", "description",
                                          F.concat(F.lit("Category: "), F.regexp_replace("category", "_", " ")),
                                          F.concat(F.lit("Tags: "), "tags")))
    .select("product_id", "title", "description", "category", "brand", "price", "image_url", "tags",
            "price_tier", "views_30d", "carts_30d", "purchases_30d", "popularity_score", "conversion_rate",
            "search_text")
)

# ----------------------------------------------------------------------------- user features
evp = ev.join(prod.select("product_id", "category", "brand", "price"), "product_id", "left")
w_recent = Window.partitionBy("user_id").orderBy(F.col("last_ts").desc())
last_items = (evp.groupBy("user_id", "product_id").agg(F.max("ts").alias("last_ts"))
              .withColumn("rn", F.row_number().over(w_recent)).where("rn <= 20")
              .groupBy("user_id")
              .agg(F.transform(F.array_sort(F.collect_list(F.struct("rn", "product_id"))),
                               lambda x: x["product_id"]).alias("last_items")))
cat_w = (evp.withColumn("w", EVENT_WEIGHT).groupBy("user_id", "category").agg(F.sum("w").alias("w")))
cat_tot = cat_w.groupBy("user_id").agg(F.sum("w").alias("tot"))
cat_aff = (cat_w.join(cat_tot, "user_id")
           .withColumn("share", F.round(F.col("w") / F.col("tot"), 4))
           .groupBy("user_id")
           .agg(F.to_json(F.map_from_entries(F.collect_list(F.struct("category", "share")))).alias("cat_affinity_json"),
                F.max_by("category", "w").alias("top_category")))
brand_top = (evp.withColumn("w", EVENT_WEIGHT).groupBy("user_id", "brand").agg(F.sum("w").alias("w"))
             .groupBy("user_id").agg(F.max_by("brand", "w").alias("top_brand")))
activity = (evp.groupBy("user_id")
            .agg(F.max("ts").alias("last_event_ts"),
                 F.sum(F.expr("CAST(event_type = 'view' AND ts >= current_timestamp() - INTERVAL 30 DAYS AS INT)"))
                 .alias("n_views_30d"),
                 F.sum(F.expr("CAST(event_type = 'add_to_cart' AND ts >= current_timestamp() - INTERVAL 30 DAYS AS INT)"))
                 .alias("n_carts_30d"),
                 F.round(F.avg(F.when(F.col("event_type") == "view", F.col("price"))), 2).alias("avg_price_viewed")))
sess = (sessions.where(F.col("session_start") >= now - F.expr("INTERVAL 30 DAYS"))
        .groupBy("user_id").agg(F.count("*").alias("n_sessions_30d")))
money = (orders.where(F.col("ts") >= now - F.expr("INTERVAL 90 DAYS"))
         .groupBy("user_id").agg(F.countDistinct("order_id").alias("n_orders_90d"),
                                 F.round(F.sum("line_revenue"), 2).alias("monetary_90d")))

uf = (users.select("user_id", "signup_ts", "country", "age_band", "acquisition_channel", "loyalty_tier")
      .join(activity, "user_id", "left").join(sess, "user_id", "left").join(money, "user_id", "left")
      .join(last_items, "user_id", "left").join(cat_aff, "user_id", "left").join(brand_top, "user_id", "left")
      .na.fill(0, ["n_views_30d", "n_carts_30d", "n_sessions_30d", "n_orders_90d"])
      .na.fill(0.0, ["monetary_90d"])
      .withColumn("last_items", F.coalesce("last_items", F.array().cast("array<string>")))
      .withColumn("cat_affinity_json", F.coalesce("cat_affinity_json", F.lit("{}")))
      .withColumn("recency_days", F.round((F.unix_timestamp(now) - F.unix_timestamp("last_event_ts")) / 86400, 2))
      .withColumn("has_history", F.size("last_items") > 0)
      .withColumn("segment",
                  F.when(~F.col("has_history"), "new")
                  # recent buyers, or high-tier loyalty members who are active right now
                  .when(((F.col("n_orders_90d") >= 1) & (F.col("recency_days") <= 10))
                        | (F.col("loyalty_tier").isin("gold", "platinum") & (F.col("recency_days") <= 14)
                           & (F.col("n_sessions_30d") >= 2)), "champion")
                  .when(F.col("n_orders_90d") >= 1, "buyer")
                  .when((F.col("n_sessions_30d") >= 3), "engaged_browser")
                  .when(F.col("recency_days") > 14, "at_risk")
                  .otherwise("casual_browser")))
user_features = uf.select("user_id", "country", "age_band", "acquisition_channel", "loyalty_tier", "segment",
                          "signup_ts", "last_event_ts", "recency_days", "n_sessions_30d", "n_views_30d",
                          "n_carts_30d", "n_orders_90d", "monetary_90d", "avg_price_viewed", "top_category",
                          "top_brand", "cat_affinity_json", "last_items", "has_history")


def upsert(df, table: str, key: str, comment: str):
    cols = [c for c in df.columns if c != key]
    df = df.withColumn("row_hash", F.sha2(F.to_json(F.struct(*cols)), 256)).withColumn("updated_ts", now)
    if not spark.catalog.tableExists(table):
        (df.limit(0).write.option("delta.enableChangeDataFeed", "true").saveAsTable(table))
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {key} SET NOT NULL")
        pk = table.split(".")[-1] + "_pk"
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {pk} PRIMARY KEY ({key})")
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
    df.createOrReplaceTempView("src")
    spark.sql(f"""
        MERGE INTO {table} t USING src s ON t.{key} = s.{key}
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    print(table, spark.read.table(table).count(), "rows")


upsert(product_features, f"{C}.gold.product_features", "product_id",
       "Product feature table (PK product_id): catalog attributes + 30d engagement. Synced to Lakebase product_catalog.")
upsert(user_features, f"{C}.gold.user_features", "user_id",
       "User feature table (PK user_id): RFM, category affinities, last-20 items. Synced to Lakebase user_features_online.")

(user_features.select("user_id", "segment", "top_category", "country", "age_band", "acquisition_channel",
                      "loyalty_tier", "n_orders_90d", "monetary_90d", "recency_days", "has_history")
 .withColumnRenamed("top_category", "primary_category")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{C}.gold.user_segments"))
spark.sql(f"COMMENT ON TABLE {C}.gold.user_segments IS 'One row per user: rule-based lifecycle segment "
          "(new, champion, buyer, engaged_browser, casual_browser, at_risk) and primary category.'")
print("user_segments refreshed")

# ----------------------------------------------------------------------------- pipeline health (for the console)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C}.gold.pipeline_health (
  snapshot_ts TIMESTAMP, bronze_events BIGINT, silver_events BIGINT, quarantined_events BIGINT,
  null_user_id BIGINT, invalid_event_type BIGINT, missing_product_id BIGINT, duplicate_events BIGINT,
  bronze_orders BIGINT, silver_orders BIGINT, invalid_orders BIGINT, users_current BIGINT, users_scd2_rows BIGINT,
  last_event_ts TIMESTAMP, last_ingest_ts TIMESTAMP, freshness_seconds DOUBLE)
COMMENT 'Data-quality and freshness snapshots of the Lakeflow pipeline (expectation drop counts by rule)'""")
spark.sql(f"""
INSERT INTO {C}.gold.pipeline_health
WITH b AS (
  SELECT count(*) AS bronze_events,
         sum(CAST(user_id IS NULL AS INT)) AS null_user_id,
         sum(CAST(event_type IS NULL OR event_type NOT IN ('view','add_to_cart','purchase','search') AS INT)) AS invalid_event_type,
         sum(CAST(product_id IS NULL AND event_type <> 'search' AS INT)) AS missing_product_id,
         count(*) - count(DISTINCT event_id) AS duplicate_events
  FROM {C}.bronze.clickstream_events),
s AS (SELECT count(*) AS silver_events, max(ts) AS last_event_ts, max(_ingest_ts) AS last_ingest_ts FROM {C}.silver.events),
q AS (SELECT count(*) AS quarantined_events FROM {C}.bronze.quarantine_events),
o AS (SELECT count(*) AS bronze_orders, sum(CAST(coalesce(unit_price, 0) <= 0 OR coalesce(qty, 0) <= 0 AS INT)) AS invalid_orders FROM {C}.bronze.orders),
so AS (SELECT count(*) AS silver_orders FROM {C}.silver.orders),
u AS (SELECT sum(CAST(__END_AT IS NULL AS INT)) AS users_current, count(*) AS users_scd2_rows FROM {C}.silver.users)
SELECT current_timestamp(), bronze_events, silver_events, quarantined_events, null_user_id, invalid_event_type,
       missing_product_id, duplicate_events, bronze_orders, silver_orders, invalid_orders, users_current,
       users_scd2_rows, last_event_ts, last_ingest_ts,
       unix_timestamp(current_timestamp()) - unix_timestamp(last_ingest_ts)
FROM b, s, q, o, so, u""")
print("pipeline_health snapshot written")
