#!/bin/bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../../.." && pwd)
compose="$here/compose.yaml"
project="coin-market-kaf09-$(date +%s)-$$"
case "$project" in
  coin-market-kaf09-*) ;;
  *) echo 'unsafe Compose project name' >&2; exit 2 ;;
esac
export HARNESS_CLUSTER_ID HARNESS_REPORT_DIR
HARNESS_CLUSTER_ID=$(python3 -c 'import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
HARNESS_REPORT_DIR="$here/reports/$project"
mkdir -p "$HARNESS_REPORT_DIR"
chmod 777 "$HARNESS_REPORT_DIR"
stage=startup

dc() { docker compose --project-name "$project" -f "$compose" "$@"; }
cleanup() {
  result=$?
  printf '{"exit_code":%d,"stage":"%s","project":"%s"}\n' "$result" "$stage" "$project" > "$HARNESS_REPORT_DIR/result.json"
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  echo "KAF09_RESULT=$result PROJECT=$project REPORT=$HARNESS_REPORT_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

tester() {
  dc run --rm --no-deps -v "$root/infrastructure/spark:/kaf08:ro" -v "$here/check_processed_archive.py:/harness/check_processed_archive.py:ro" --entrypoint python tester /harness/check_processed_archive.py "$1"
}
spark_sink() {
  dc run --rm --no-deps -v "$root/infrastructure/spark:/kaf08:ro" -v "$root/preprocess_schema.py:/schema/preprocess_schema.py:ro" --entrypoint /opt/spark/bin/spark-submit spark-test --master 'local[2]' /kaf08/verify_processed_sink.py
}

echo "KAF09_PROJECT=$project CLUSTER_ID=$HARNESS_CLUSTER_ID"
dc up -d --build --wait --wait-timeout 240 connect minio
dc build tester spark-test

stage=before_restart
tester setup
spark_sink
tester before

stage=after_restart
tester restart
spark_sink
tester after

stage=complete
echo 'KAF09_HARNESS_OK'
