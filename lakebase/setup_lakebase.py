"""Lakebase serving layer: synced tables (UC gold -> Postgres) and app grants.

Synced tables (Postgres schema ``serving``):
    user_features_online <- gold.user_features      CONTINUOUS
    product_catalog      <- gold.product_features   TRIGGERED
    precomputed_recs     <- gold.precomputed_recs   TRIGGERED

Idempotent: existing synced tables get a refresh (triggered ones) instead of
being recreated. With ``--app`` it also grants the app's service principal
read access to the synced schema.
"""
import argparse
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.postgres import (NewPipelineSpec, SyncedTable, SyncedTableSyncedTableSpec,
                                             SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as Policy)

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default="fevm_catalog_ra_test")
ap.add_argument("--project", default="pers-serving")
ap.add_argument("--lakebase_catalog", default="pers_serving_lb")
ap.add_argument("--app", default="")
ap.add_argument("--wait_minutes", type=int, default=25)
ap.add_argument("--root", default=None)
args, _ = ap.parse_known_args()

w = WorkspaceClient()
BRANCH = f"projects/{args.project}/branches/production"
ENDPOINT = f"{BRANCH}/endpoints/primary"
PG_DB = "databricks_postgres"
PG_SCHEMA = "serving"
SYNCS = [
    ("user_features_online", f"{args.catalog}.gold.user_features", ["user_id"], Policy.CONTINUOUS),
    ("product_catalog", f"{args.catalog}.gold.product_features", ["product_id"], Policy.TRIGGERED),
    ("precomputed_recs", f"{args.catalog}.gold.precomputed_recs", ["user_id"], Policy.TRIGGERED),
]

for table, source, pk, policy in SYNCS:
    full = f"{args.lakebase_catalog}.{PG_SCHEMA}.{table}"
    try:
        st = w.postgres.get_synced_table(f"synced_tables/{full}")
        pid = st.status.pipeline_id if st.status else None
        print(f"exists {full} state={st.status.detailed_state if st.status else None}")
        if policy != Policy.CONTINUOUS and pid:
            try:
                w.pipelines.start_update(pid)
                print("  triggered refresh", pid)
            except Exception as e:  # noqa: BLE001  (an update may already be running)
                print("  refresh skipped:", str(e)[:160])
    except NotFound:
        print(f"creating {full} <- {source} ({policy.value})")
        w.postgres.create_synced_table(
            synced_table=SyncedTable(spec=SyncedTableSyncedTableSpec(
                source_table_full_name=source, primary_key_columns=pk, scheduling_policy=policy,
                branch=BRANCH, postgres_database=PG_DB, create_database_objects_if_missing=True,
                new_pipeline_spec=NewPipelineSpec(storage_catalog=args.catalog, storage_schema="ml"))),
            synced_table_id=full)

# wait for the initial snapshots to land
deadline = time.time() + args.wait_minutes * 60
pending = {f"{args.lakebase_catalog}.{PG_SCHEMA}.{t}" for t, *_ in SYNCS}
while pending and time.time() < deadline:
    for full in list(pending):
        st = w.postgres.get_synced_table(f"synced_tables/{full}")
        state = str(st.status.detailed_state) if st.status else "?"
        if any(s in state for s in ("ONLINE", "ACTIVE")):
            print("online", full, state)
            pending.discard(full)
        elif "FAILED" in state:
            raise SystemExit(f"{full} failed: {st.status.message}")
    if pending:
        time.sleep(20)
if pending and args.wait_minutes > 0:
    print("still provisioning (continuing):", pending)

if args.app:
    import psycopg
    sp = w.apps.get(args.app).service_principal_client_id
    host = w.postgres.get_endpoint(ENDPOINT).status.hosts.host
    token = w.postgres.generate_database_credential(endpoint=ENDPOINT).token
    me = w.current_user.me().user_name
    with psycopg.connect(host=host, dbname=PG_DB, user=me, password=token, sslmode="require",
                         autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp,)).fetchone()
        if not exists:
            raise SystemExit(f"Postgres role for app SP {sp} not found — deploy the app (postgres resource) first")
        for stmt in (f'GRANT USAGE ON SCHEMA {PG_SCHEMA} TO "{sp}"',
                     f'GRANT SELECT ON ALL TABLES IN SCHEMA {PG_SCHEMA} TO "{sp}"',
                     f'ALTER DEFAULT PRIVILEGES IN SCHEMA {PG_SCHEMA} GRANT SELECT ON TABLES TO "{sp}"'):
            conn.execute(stmt)
            print("ok", stmt)
