#!/bin/bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
compose="$here/harness/compose.yaml"
project="coin-market-kaf01-bench-$(date +%s)-$$"
case "$project" in coin-market-kaf01-bench-*) ;; *) exit 2 ;; esac
export HARNESS_CLUSTER_ID HARNESS_REPORT_DIR
HARNESS_CLUSTER_ID=$(python3 -c 'import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
HARNESS_REPORT_DIR="$here/harness/reports/$project"
mkdir -p "$HARNESS_REPORT_DIR"
chmod 777 "$HARNESS_REPORT_DIR"
printf 'HARNESS_CLUSTER_ID=%s\nHARNESS_REPORT_DIR=%s\n' \
  "$HARNESS_CLUSTER_ID" "$HARNESS_REPORT_DIR" > "$HARNESS_REPORT_DIR/harness.env"
dc() { docker compose --project-name "$project" -f "$compose" "$@"; }
stage=startup
cleanup() {
  result=$?
  printf '{"exit_code":%d,"stage":"%s","project":"%s"}\n' \
    "$result" "$stage" "$project" > "$HARNESS_REPORT_DIR/result.json"
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  echo "KAF01_BENCH_RESULT=$result PROJECT=$project REPORT=$HARNESS_REPORT_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

dc up -d --wait --wait-timeout 240 kafka-1 kafka-2 kafka-3
dc run --rm --no-deps init-topics
dc build tester
stage=measurement
for partitions in 1 3 6; do
  if (( partitions > 1 )); then
    for topic in koreaSocketDataInBTC-ticker koreaSocketDataInBTC-orderbook; do
      dc exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
        --alter --topic "$topic" --partitions "$partitions"
    done
  fi
  for kind in ticker orderbook; do
    for rate in 100 400; do
      dc run --rm --no-deps --entrypoint python tester \
        /harness/bench_websocket_raw.py "$kind" "$partitions" "$rate" 8
    done
  done
done
python3 "$here/harness/summarize_websocket_bench.py" "$HARNESS_REPORT_DIR"
stage=complete
echo 'KAF01_WEBSOCKET_BENCH_OK'
