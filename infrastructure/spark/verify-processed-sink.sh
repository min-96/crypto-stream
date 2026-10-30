#!/bin/bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
here="$root/infrastructure/tests/kafka/harness"
compose="$here/compose.yaml"
project="coin-market-kaf08-$(date +%s)-$$"
case "$project" in
  coin-market-kaf08-*) ;;
  *) echo 'unsafe project name' >&2; exit 2 ;;
esac
export HARNESS_CLUSTER_ID
HARNESS_CLUSTER_ID=$(python3 -c 'import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
export HARNESS_REPORT_DIR="$here/reports/$project"
mkdir -p "$HARNESS_REPORT_DIR"
chmod 777 "$HARNESS_REPORT_DIR"
stage=startup

dc() { docker compose --project-name "$project" -f "$compose" "$@"; }
cleanup() {
  result=$?
  printf '{"exit_code":%d,"stage":"%s","project":"%s"}\n' \
    "$result" "$stage" "$project" > "$HARNESS_REPORT_DIR/result.json"
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  echo "KAF08_RESULT=$result PROJECT=$project REPORT=$HARNESS_REPORT_DIR"
}
trap cleanup EXIT

echo "KAF08_PROJECT=$project CLUSTER_ID=$HARNESS_CLUSTER_ID"
dc up -d --wait --wait-timeout 240 kafka-1 kafka-2 kafka-3
dc run --rm --no-deps init-topics
dc build spark-test tester

stage=spark_sink
dc run --rm --no-deps \
  -v "$root/infrastructure/spark:/kaf08:ro" \
  -v "$root/preprocess_schema.py:/schema/preprocess_schema.py:ro" \
  --entrypoint /opt/spark/bin/spark-submit \
  spark-test --master 'local[2]' /kaf08/verify_processed_sink.py

stage=consumer
dc run --rm --no-deps \
  -v "$root/infrastructure/spark:/kaf08:ro" \
  -v "$here/check_processed_sink.py:/check_processed_sink.py:ro" \
  --entrypoint python tester /check_processed_sink.py

stage=complete
echo 'KAF08_HARNESS_OK'
