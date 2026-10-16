"""Runtime configuration (env vars are injected by the bundle's app config)."""
import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient

CATALOG = os.getenv("PERS_CATALOG", "fevm_catalog_ra_test")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")  # injected from the sql-warehouse app resource
RECOMMENDER_ENDPOINT = os.getenv("RECOMMENDER_ENDPOINT", "pers-recommender")
EXPLAIN_LLM = os.getenv("EXPLAIN_LLM", "databricks-claude-haiku-4-5")
AGENT_LLM = os.getenv("AGENT_LLM", "databricks-claude-sonnet-4-5")
EMBEDDING_ENDPOINT = os.getenv("EMBEDDING_ENDPOINT", "databricks-gte-large-en")
VS_INDEX = os.getenv("VS_INDEX", f"{CATALOG}.gold.product_index")
GENIE_SPACE_TITLE = os.getenv("GENIE_SPACE_TITLE", "Personalization Insights")
LAKEBASE_PROJECT = os.getenv("LAKEBASE_PROJECT", "pers-serving")
LAKEBASE_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT", f"projects/{LAKEBASE_PROJECT}/branches/production/endpoints/primary")
PIPELINE_NAME = os.getenv("PIPELINE_NAME", "pers-personalization-pipeline")
SERVING_SCHEMA = "serving"   # Postgres schema of the synced tables
APP_SCHEMA = "app"           # Postgres schema owned by the app's service principal
MODEL_TIMEOUT_S = float(os.getenv("MODEL_TIMEOUT_S", "1.5"))


@lru_cache
def ws() -> WorkspaceClient:
    return WorkspaceClient()
