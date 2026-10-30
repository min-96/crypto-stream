#!/bin/bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
compose="$here/harness/compose.yaml"
override="$here/harness/compose-315-memory.yaml"
project="coin-market-kaf315-$(date +%s)-$$"
case "$project" in coin-market-kaf315-*) ;; *) exit 2 ;; esac
export HARNESS_CLUSTER_ID HARNESS_REPORT_DIR
HARNESS_CLUSTER_ID=$(python3 -c 'import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
HARNESS_REPORT_DIR="$here/harness/reports/$project"
mkdir -p "$HARNESS_REPORT_DIR"
chmod 777 "$HARNESS_REPORT_DIR"
printf 'HARNESS_CLUSTER_ID=%s\nHARNESS_REPORT_DIR=%s\n' \
  "$HARNESS_CLUSTER_ID" "$HARNESS_REPORT_DIR" > "$HARNESS_REPORT_DIR/harness.env"
dc() { docker compose --project-name "$project" -f "$compose" -f "$override" "$@"; }
stage=startup
cleanup() {
  result=$?
  printf '{"exit_code":%d,"stage":"%s","project":"%s"}\n' \
    "$result" "$stage" "$project" > "$HARNESS_REPORT_DIR/result.json"
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  echo "KAF315_RESULT=$result PROJECT=$project REPORT=$HARNESS_REPORT_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

dc up -d --build --wait --wait-timeout 240 connect minio
dc build tester spark-test
for kind in ticker orderbook; do
  if [[ "$kind" == ticker ]]; then partitions=3; kind_title=Ticker; else partitions=6; kind_title=Orderbook; fi
  for region in Korea NE Asia; do
    case "$region" in Korea) raw_prefix=korea ;; NE) raw_prefix=ne ;; Asia) raw_prefix=asia ;; esac
    raw_topic="${raw_prefix}SocketDataInBTC-${kind}"
    pre_topic="Region${region}_${kind_title}Preprocessing"
    dc exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
      --alter --topic "$raw_topic" --partitions "$partitions"
    dc exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
      --alter --topic "$pre_topic" --partitions "$partitions"
  done
done

stage=raw
dc run --rm --no-deps -v "$here/harness/bench_315_pipeline.py:/harness/bench_315_pipeline.py:ro" \
  --entrypoint python tester /harness/bench_315_pipeline.py raw
stage=preprocessed
dc run --rm --no-deps -v "$here/harness/bench_315_pipeline.py:/harness/bench_315_pipeline.py:ro" \
  --entrypoint python tester /harness/bench_315_pipeline.py preprocessed
stage=spark
dc run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-test \
  --master 'local[2]' /harness/check_315_spark.py
python3 "$here/harness/summarize_315.py" "$HARNESS_REPORT_DIR"
stage=complete
echo 'KAF315_DOWNSTREAM_HARNESS_OK'
