#!/usr/bin/env bash
# Build the React bundle into app/backend/static and copy the docs the About page renders.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/app/frontend"
[ -d node_modules ] || npm ci
npm run build
mkdir -p "$ROOT/app/backend/content"
cp "$ROOT/docs/about.md" "$ROOT/docs/decisions.md" "$ROOT/docs/spec.md" "$ROOT/app/backend/content/"
cp "$ROOT/docs/architecture.svg" "$ROOT/app/backend/static/architecture.svg"
echo "app built -> app/backend/static"
