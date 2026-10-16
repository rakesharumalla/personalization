#!/usr/bin/env bash
# Reset the synthetic data so the generator + pipeline can be replayed from scratch.
# Deletes landing files and the pipeline's tables (the next pers-setup run full-refreshes the pipeline).
# Usage: scripts/reset.sh [profile] [catalog]
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
[ -f "$ROOT_DIR/.env" ] && { set -a; . "$ROOT_DIR/.env"; set +a; }
PROFILE="${1:-${DATABRICKS_CONFIG_PROFILE:?set DATABRICKS_CONFIG_PROFILE in .env or pass a profile}}"
export DATABRICKS_CONFIG_PROFILE="$PROFILE"
CATALOG="${2:-fevm_catalog_ra_test}"
read -r -p "This deletes /Volumes/$CATALOG/bronze/raw_landing/* and gold feature tables. Continue? [y/N] " ok
[ "$ok" = "y" ] || exit 1
for d in events orders users products; do
  databricks fs rm -r "dbfs:/Volumes/$CATALOG/bronze/raw_landing/$d" -p "$PROFILE" || true
done
for t in gold.user_features gold.product_features gold.precomputed_recs gold.pipeline_health; do
  databricks experimental aitools tools query "DROP TABLE IF EXISTS $CATALOG.$t" -p "$PROFILE" || true
done
echo "reset done — rerun: databricks bundle run pers_setup -p $PROFILE"
