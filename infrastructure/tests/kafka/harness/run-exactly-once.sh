#!/bin/bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
compose="$here/compose.yaml"
project="coin-market-kaf-eos-$(date +%s)-$$"
case "$project" in
  coin-market-kaf-eos-*) ;;
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
  echo "EOS_RESULT=$result PROJECT=$project REPORT=$HARNESS_REPORT_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "EOS_PROJECT=$project CLUSTER_ID=$HARNESS_CLUSTER_ID"
dc up -d --build --wait --wait-timeout 240 connect minio
dc build tester spark-test

stage=transaction
dc run --rm --no-deps -v "$here/check_exactly_once.py:/harness/check_exactly_once.py:ro" --entrypoint python tester /harness/check_exactly_once.py transaction

stage=archive
dc run --rm --no-deps -v "$here/check_exactly_once.py:/harness/check_exactly_once.py:ro" --entrypoint python tester /harness/check_exactly_once.py archive

stage=average
dc run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-test --master 'local[2]' /harness/check_exactly_once_spark.py

stage=complete
echo 'EOS_AVERAGE_HARNESS_OK'
