#!/usr/bin/env bash
# One-shot deploy to a fresh workspace:  scripts/deploy.sh [profile] [target]
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
[ -f "$ROOT_DIR/.env" ] && { set -a; . "$ROOT_DIR/.env"; set +a; }
PROFILE="${1:-${DATABRICKS_CONFIG_PROFILE:?set DATABRICKS_CONFIG_PROFILE in .env or pass a profile}}"
export DATABRICKS_CONFIG_PROFILE="$PROFILE"
TARGET="${2:-dev}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
scripts/build_app.sh
databricks bundle deploy -t "$TARGET" -p "$PROFILE"
databricks bundle run pers_setup -t "$TARGET" -p "$PROFILE"
databricks bundle run pers_app -t "$TARGET" -p "$PROFILE"
