#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
project=${LOCAL_PIPELINE_PROJECT:-coin-market-local-pipeline}
export HARNESS_CLUSTER_ID=${HARNESS_CLUSTER_ID:-oDx21XmKSJWnw817gv1YsQ}
export HARNESS_REPORT_DIR=${HARNESS_REPORT_DIR:-/tmp/coin-market-local-pipeline-reports}
mkdir -p "$HARNESS_REPORT_DIR"

dc() {
  docker compose --project-name "$project" \
    -f "$here/compose.yaml" -f "$here/compose.local-pipeline.yaml" "$@"
}

case "${1:-}" in
  up) dc up -d --build init-preprocessed-connector spark-average collector ;;
  ps) dc ps ;;
  logs) dc logs --tail=100 "${@:2}" ;;
  verify) python3 "$here/verify_local_pipeline.py" ;;
  down) dc down ;;
  *) echo "usage: $0 up|ps|logs [service...]|verify|down" >&2; exit 2 ;;
esac
