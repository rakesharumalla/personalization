"""Silver: typed, deduplicated, quality-checked, PII-masked, sessionized."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

C = spark.conf.get("catalog")  # noqa: F821

EVENT_RULES = {
    "valid_user_id": "user_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL OR event_type = 'search'",
    "valid_event_type": "event_type IN ('view', 'add_to_cart', 'purchase', 'search')",
}
TS_RULE = {"ts_in_bounds": "ts >= TIMESTAMP'2020-01-01' AND ts <= current_timestamp() + INTERVAL 1 DAY"}


def _typed_events():
    return (spark.readStream.table(f"{C}.bronze.clickstream_events")  # noqa: F821
            .withColumn("ts", F.to_timestamp("ts")))


@dp.table(name=f"{C}.silver.events",
          comment="Clean clickstream: typed, deduped on event_id, expectations enforced",
          cluster_by=["event_date", "user_id"])
@dp.expect_all_or_drop(EVENT_RULES)
@dp.expect_all(TS_RULE)
def events():
    return (_typed_events()
            .withWatermark("ts", "30 days")
            .dropDuplicatesWithinWatermark(["event_id"])
            .withColumn("event_date", F.to_date("ts"))
            .select("event_id", "user_id", "session_id", "event_type", "product_id", "ts", "event_date",
                    "device", "referrer", "search_query", "_ingest_ts"))


@dp.table(name=f"{C}.bronze.quarantine_events",
          comment="Clickstream rows that violated a silver expectation (kept for triage)")
def quarantine_events():
    bad = " OR ".join(f"NOT ({r})" for r in EVENT_RULES.values())
    return (_typed_events()
            .where(f"({bad}) OR user_id IS NULL OR event_type IS NULL")
            .withColumn("_quarantined_at", F.current_timestamp()))


@dp.table(name=f"{C}.silver.orders", comment="Order lines, typed and validated")
@dp.expect_or_drop("positive_price", "unit_price > 0")
@dp.expect_or_drop("positive_qty", "qty > 0")
@dp.expect_or_drop("valid_keys", "user_id IS NOT NULL AND product_id IS NOT NULL")
def orders():
    return (spark.readStream.table(f"{C}.bronze.orders")  # noqa: F821
            .withColumn("ts", F.to_timestamp("ts"))
            .withWatermark("ts", "30 days")
            .dropDuplicatesWithinWatermark(["order_id", "product_id"])
            .withColumn("line_revenue", F.col("qty") * F.col("unit_price"))
            .drop("_rescued_data", "_source_file"))


# --- users: SCD2 with column mask on email and a regional row filter ---------

@dp.temporary_view()
def users_changes():
    return (spark.readStream.table(f"{C}.bronze.users_cdc")  # noqa: F821
            .where("user_id IS NOT NULL")
            .withColumn("signup_ts", F.to_timestamp("signup_ts"))
            .withColumn("change_ts", F.to_timestamp("change_ts")))


dp.create_streaming_table(
    name=f"{C}.silver.users",
    comment="SCD2 user registry. email is masked (silver.mask_email); EMEA analysts are row-filtered.",
    schema=f"""
        user_id STRING,
        email STRING MASK {C}.silver.mask_email,
        signup_ts TIMESTAMP,
        country STRING,
        age_band STRING,
        acquisition_channel STRING,
        loyalty_tier STRING,
        change_ts TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """,
    row_filter=f"ROW FILTER {C}.silver.users_region_filter ON (country)",
)
dp.create_auto_cdc_flow(
    target=f"{C}.silver.users",
    source="users_changes",
    keys=["user_id"],
    sequence_by="change_ts",
    stored_as_scd_type=2,
    column_list=["user_id", "email", "signup_ts", "country", "age_band", "acquisition_channel",
                 "loyalty_tier", "change_ts"],
    track_history_column_list=["loyalty_tier", "country"],
)


# --- products: SCD1 ------------------------------------------------------------

@dp.temporary_view()
def products_changes():
    return (spark.readStream.table(f"{C}.bronze.products")  # noqa: F821
            .where("product_id IS NOT NULL AND price > 0"))


dp.create_streaming_table(name=f"{C}.silver.products", comment="Current product catalog (SCD1)")
dp.create_auto_cdc_flow(
    target=f"{C}.silver.products",
    source="products_changes",
    keys=["product_id"],
    sequence_by="_ingest_ts",
    column_list=["product_id", "title", "description", "category", "brand", "price", "image_url", "tags",
                 "_ingest_ts"],
)


# --- sessions --------------------------------------------------------------------

@dp.materialized_view(name=f"{C}.silver.sessions",
                      comment="One row per session with funnel counts; cold start = user signed up < 24h before")
def sessions():
    ev = spark.read.table(f"{C}.silver.events")  # noqa: F821
    prod = spark.read.table(f"{C}.silver.products").select("product_id", "price")  # noqa: F821
    users = (spark.read.table(f"{C}.silver.users")  # noqa: F821
             .where("__END_AT IS NULL").select("user_id", "signup_ts", "country", "acquisition_channel"))
    s = (ev.join(prod, "product_id", "left")
         .groupBy("session_id", "user_id")
         .agg(F.min("ts").alias("session_start"), F.max("ts").alias("session_end"),
              F.first("device").alias("device"), F.first("referrer").alias("referrer"),
              F.count("*").alias("n_events"),
              F.sum(F.expr("CAST(event_type = 'view' AS INT)")).alias("n_views"),
              F.sum(F.expr("CAST(event_type = 'add_to_cart' AS INT)")).alias("n_add_to_cart"),
              F.sum(F.expr("CAST(event_type = 'purchase' AS INT)")).alias("n_purchased_items"),
              F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("revenue")))
    return (s.join(users, "user_id", "left")
            .withColumn("session_date", F.to_date("session_start"))
            .withColumn("converted", F.col("n_purchased_items") > 0)
            .withColumn("is_cold_start",
                        F.coalesce(F.col("session_start") < F.col("signup_ts") + F.expr("INTERVAL 24 HOURS"),
                                   F.lit(True))))
