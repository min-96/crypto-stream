#!/bin/bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
project="coin-market-kaf11-$(date +%s)-$$"
export HARNESS_CLUSTER_ID HARNESS_REPORT_DIR
HARNESS_CLUSTER_ID=$(python3 -c 'import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
HARNESS_REPORT_DIR="$here/reports/$project"
mkdir -p "$HARNESS_REPORT_DIR"
chmod 777 "$HARNESS_REPORT_DIR"
compose="$here/compose.yaml"
override="$here/compose.retention.yaml"
dc() { docker compose --project-name "$project" -f "$compose" -f "$override" "$@"; }
stage=startup
cleanup() {
  result=$?
  printf '{"exit_code":%d,"stage":"%s","project":"%s"}\n' "$result" "$stage" "$project" > "$HARNESS_REPORT_DIR/result.json"
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  echo "KAF11_RESULT=$result PROJECT=$project REPORT=$HARNESS_REPORT_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "KAF11_PROJECT=$project"
dc up -d --wait --wait-timeout 240 kafka-1 kafka-2 kafka-3
dc run --rm init-topics
dc build tester spark-test
dc run --rm --no-deps checkpoint-init

stage=seed
dc run --rm --no-deps --entrypoint python tester /harness/check_retention.py setup
dc run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-test --master 'local[2]' /harness/check_retention_spark.py first
dc run --rm --no-deps --entrypoint python tester /harness/check_retention.py prepare

stage=retention
for topic in asiaSocketDataInBTC-ticker RegionAsia_TickerPreprocessing; do
  dc exec -T kafka-1 kafka-configs --bootstrap-server kafka-1:9092 --entity-type topics --entity-name "$topic" --alter --add-config retention.ms=1000,segment.ms=1000,file.delete.delay.ms=1000
done
sleep 3
dc run --rm --no-deps --entrypoint python tester /harness/check_retention.py roll
dc run --rm --no-deps --entrypoint python tester /harness/check_retention.py wait

stage=recovery
dc run --rm --no-deps --entrypoint python tester /harness/check_retention.py verify
dc run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-test --master 'local[2]' /harness/check_retention_spark.py resume
stage=complete
echo 'KAF11_RETENTION_HARNESS_OK'
