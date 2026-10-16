"""Unity Catalog governance setup.

--stage pre   schemas, volumes, masking/row-filter functions (before the pipeline runs;
              the SDP pipeline references the functions in its silver.users schema)
--stage post  tags + classification on pipeline/feature tables, grants to groups and
              to the app service principal (after tables exist)

Every statement is idempotent and best-effort: a failure on an optional object
(e.g. a group that does not exist at account level) is logged, not fatal.
"""
import argparse

from pyspark.sql import SparkSession

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--stage", choices=["pre", "post"], default="pre")
ap.add_argument("--app_sp", default="", help="app service principal application id")
ap.add_argument("--root", default=None)
args, _ = ap.parse_known_args()
C = args.catalog
spark = SparkSession.builder.getOrCreate()
failures = []


def run(sql: str, optional: bool = False):
    try:
        spark.sql(sql)
        print("ok  ", sql.splitlines()[0][:140])
    except Exception as e:  # noqa: BLE001
        msg = str(e).splitlines()[0][:220]
        print("FAIL" if not optional else "skip", sql.splitlines()[0][:140], "->", msg)
        if not optional:
            failures.append((sql, msg))


LAYERS = {
    "bronze": "Raw, schema-on-read landing of synthetic clickstream, orders, users CDC and catalog",
    "silver": "Deduped, typed, PII-masked, sessionized events and SCD2 users",
    "gold": "Features, KPIs, experiment results and recommendation outcomes (Genie + Lakebase sources)",
    "ml": "Models, training sets, simulator ground truth and batch inference outputs",
    "app": "Operational exports from the Databricks App (recommendation log)",
    "serving": "Lakebase synced tables (UC entries of Postgres tables in schema serving)",
}

if args.stage == "pre":
    for s, comment in LAYERS.items():
        run(f"CREATE SCHEMA IF NOT EXISTS {C}.{s} COMMENT '{comment}'")
        run(f"ALTER SCHEMA {C}.{s} SET TAGS ('data_product' = 'personalization', 'layer' = '{s}')", optional=True)
    run(f"CREATE VOLUME IF NOT EXISTS {C}.bronze.raw_landing COMMENT 'Synthetic JSON landing zone (Auto Loader source)'")
    run(f"CREATE VOLUME IF NOT EXISTS {C}.ml.model_artifacts COMMENT 'Recommender artifacts and checkpoints'")

    # Column mask: only engineers see raw email. Everyone else (incl. analysts) sees sh***@example.com
    run(f"""CREATE OR REPLACE FUNCTION {C}.silver.mask_email(email STRING)
RETURNS STRING
COMMENT 'PII mask for email: raw value for pers_engineers, masked for everyone else'
RETURN CASE WHEN is_account_group_member('pers_engineers') THEN email
            ELSE concat(substr(email, 1, 2), '***@', element_at(split(email, '@'), 2)) END""")
    # Row filter: the regional (EMEA) analyst group only sees EMEA users
    run(f"""CREATE OR REPLACE FUNCTION {C}.silver.users_region_filter(country STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: pers_analysts_emea restricted to GB/DE/FR; all other principals unrestricted'
RETURN is_account_group_member('pers_engineers')
    OR NOT is_account_group_member('pers_analysts_emea')
    OR country IN ('GB', 'DE', 'FR')""")

else:
    tables = {
        "bronze": ["clickstream_events", "orders", "users_cdc", "products", "quarantine_events"],
        "silver": ["events", "orders", "users", "products", "sessions"],
        "gold": ["user_features", "product_features", "session_context", "session_kpis_daily",
                 "product_performance", "user_segments", "recommendation_outcomes", "experiment_results",
                 "precomputed_recs", "recommendation_log"],
    }
    for layer, names in tables.items():
        for t in names:
            sens = "confidential" if t in ("users_cdc", "users") else "internal"
            run(f"ALTER TABLE {C}.{layer}.{t} SET TAGS ('data_product' = 'personalization', "
                f"'layer' = '{layer}', 'sensitivity' = '{sens}')", optional=True)
    for t in ("bronze.users_cdc", "silver.users"):
        run(f"ALTER TABLE {C}.{t} ALTER COLUMN email SET TAGS ('pii' = 'email')", optional=True)

    # Access model ---------------------------------------------------------
    run(f"GRANT USE CATALOG ON CATALOG {C} TO `pers_engineers`", optional=True)
    run(f"GRANT ALL PRIVILEGES ON CATALOG {C} TO `pers_engineers`", optional=True)
    run(f"GRANT USE CATALOG ON CATALOG {C} TO `pers_analysts`", optional=True)
    run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.gold TO `pers_analysts`", optional=True)
    run(f"GRANT USE CATALOG ON CATALOG {C} TO `pers_analysts_emea`", optional=True)
    run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.silver TO `pers_analysts_emea`", optional=True)
    if args.app_sp:
        sp = f"`{args.app_sp}`"
        run(f"GRANT USE CATALOG ON CATALOG {C} TO {sp}")
        run(f"GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA {C}.gold TO {sp}")
        run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.ml TO {sp}")
        run(f"GRANT USE SCHEMA, MODIFY, SELECT, CREATE TABLE ON SCHEMA {C}.app TO {sp}")

if failures:
    raise SystemExit(f"{len(failures)} required statements failed: {failures}")
