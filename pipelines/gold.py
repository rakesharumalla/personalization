"""Gold: business KPI tables and real-time session context (materialized views).

Feature tables (gold.user_features / gold.product_features) are produced by the
feature job instead (``features/build_features.py``): they are Delta tables with
primary keys + Change Data Feed so they can be registered as UC feature tables
and continuously synced to Lakebase.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

C = spark.conf.get("catalog")  # noqa: F821


@dp.materialized_view(name=f"{C}.gold.session_kpis_daily",
                      comment="Daily funnel KPIs by device and cold-start flag. "
                              "conversion = converted sessions / sessions; aov = revenue / converted sessions")
def session_kpis_daily():
    s = spark.read.table(f"{C}.silver.sessions")  # noqa: F821
    return (s.groupBy("session_date", "device", "is_cold_start")
            .agg(F.count("*").alias("sessions"),
                 F.sum(F.col("converted").cast("int")).alias("converted_sessions"),
                 F.sum("n_views").alias("views"),
                 F.sum("n_add_to_cart").alias("add_to_carts"),
                 F.sum("n_purchased_items").alias("purchased_items"),
                 F.round(F.sum("revenue"), 2).alias("revenue"))
            .withColumn("conversion_rate", F.round(F.col("converted_sessions") / F.col("sessions"), 5))
            .withColumn("aov", F.round(F.col("revenue") / F.greatest(F.col("converted_sessions"), F.lit(1)), 2)))


@dp.materialized_view(name=f"{C}.gold.product_performance",
                      comment="Organic product funnel over the full history plus last-7-day views")
def product_performance():
    ev = spark.read.table(f"{C}.silver.events").where("product_id IS NOT NULL")  # noqa: F821
    prod = spark.read.table(f"{C}.silver.products")  # noqa: F821
    agg = (ev.groupBy("product_id")
           .agg(F.sum(F.expr("CAST(event_type = 'view' AS INT)")).alias("views"),
                F.sum(F.expr("CAST(event_type = 'view' AND ts >= current_timestamp() - INTERVAL 7 DAYS AS INT)"))
                .alias("views_7d"),
                F.sum(F.expr("CAST(event_type = 'add_to_cart' AS INT)")).alias("add_to_carts"),
                F.sum(F.expr("CAST(event_type = 'purchase' AS INT)")).alias("purchases")))
    return (prod.select("product_id", "title", "category", "brand", "price")
            .join(agg, "product_id", "left").na.fill(0, ["views", "views_7d", "add_to_carts", "purchases"])
            .withColumn("revenue", F.round(F.col("purchases") * F.col("price"), 2))
            .withColumn("view_to_purchase_rate",
                        F.round(F.col("purchases") / F.greatest(F.col("views"), F.lit(1)), 5)))


@dp.materialized_view(name=f"{C}.gold.session_context",
                      comment="Real-time session context: last 5 product events per session in the last 24h")
def session_context():
    ev = (spark.read.table(f"{C}.silver.events")  # noqa: F821
          .where("product_id IS NOT NULL AND ts >= current_timestamp() - INTERVAL 24 HOURS"))
    prod = spark.read.table(f"{C}.silver.products").select("product_id", "category")  # noqa: F821
    w = Window.partitionBy("session_id").orderBy(F.col("ts").desc())
    last5 = (ev.join(prod, "product_id", "left")
             .withColumn("rn", F.row_number().over(w)).where("rn <= 5"))
    return (last5.groupBy("session_id", "user_id")
            .agg(F.max("ts").alias("last_event_ts"),
                 F.transform(F.array_sort(F.collect_list(F.struct("rn", "product_id"))),
                             lambda x: x["product_id"]).alias("last_5_products"),
                 F.transform(F.array_sort(F.collect_list(F.struct("rn", "category"))),
                             lambda x: x["category"]).alias("last_5_categories")))
