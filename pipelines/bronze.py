"""Bronze: raw Auto Loader ingestion from the landing volume (schema-on-read)."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

C = spark.conf.get("catalog")  # noqa: F821
LANDING = spark.conf.get("landing")  # noqa: F821


def _autoload(subdir: str, hints: str):
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaHints", hints)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING}/{subdir}")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(name=f"{C}.bronze.clickstream_events",
          comment="Raw clickstream events (view/add_to_cart/purchase/search) landed every few seconds",
          table_properties={"layer": "bronze"})
def clickstream_events():
    return _autoload("events", "event_id string, user_id string, session_id string, event_type string, "
                               "product_id string, ts string, device string, referrer string, search_query string")


@dp.table(name=f"{C}.bronze.orders", comment="Raw order lines", table_properties={"layer": "bronze"})
def orders():
    return _autoload("orders", "order_id string, user_id string, product_id string, qty int, "
                               "unit_price double, ts string")


@dp.table(name=f"{C}.bronze.users_cdc", comment="User registry change feed (INSERT/UPDATE); contains PII (email)",
          table_properties={"layer": "bronze"})
def users_cdc():
    return _autoload("users", "user_id string, email string, signup_ts string, country string, age_band string, "
                              "acquisition_channel string, loyalty_tier string, op string, change_ts string")


@dp.table(name=f"{C}.bronze.products", comment="Raw product catalog drops", table_properties={"layer": "bronze"})
def products():
    return _autoload("products", "product_id string, title string, description string, category string, "
                                 "brand string, price double, image_url string, tags array<string>")
