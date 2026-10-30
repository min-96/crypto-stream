#!/bin/bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
compose="$here/harness/compose.yaml"
project="coin-market-kaf10-$(date +%s)-$$"
case "$project" in coin-market-kaf10-*) ;; *) exit 2 ;; esac
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
  dc start kafka-2 kafka-3 >/dev/null 2>&1 || true
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  echo "KAF10_RESULT=$result PROJECT=$project REPORT=$HARNESS_REPORT_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

dc up -d --wait --wait-timeout 240 kafka-1 kafka-2 kafka-3
dc run --rm --no-deps init-topics
dc build tester
stage=baseline
dc exec -T kafka-1 kafka-metadata-quorum --bootstrap-server kafka-1:9092 describe --status > "$HARNESS_REPORT_DIR/kraft_baseline_status.txt"
dc exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic asiaSocketDataInBTC-ticker > "$HARNESS_REPORT_DIR/kraft_baseline_topic.txt"
dc run --rm --no-deps tester baseline > "$HARNESS_REPORT_DIR/kraft_baseline_producer.txt"

stage=one_down
dc stop kafka-3
for attempt in $(seq 1 30); do
  if dc exec -T kafka-1 kafka-metadata-quorum --bootstrap-server kafka-1:9092 describe --status > "$HARNESS_REPORT_DIR/kraft_one_down_status.txt" 2>&1 \
    && dc exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic asiaSocketDataInBTC-ticker > "$HARNESS_REPORT_DIR/kraft_one_down_topic.txt" 2>&1 \
    && python3 -c 'import re,sys; s=open(sys.argv[1]).read(); m=re.search(r"\bIsr:\s*([0-9,]+)",s); sys.exit(0 if m and set(m.group(1).split(","))=={"1","2"} else 1)' "$HARNESS_REPORT_DIR/kraft_one_down_topic.txt"; then
    break
  fi
  sleep 2
done
dc run --rm --no-deps tester one-down > "$HARNESS_REPORT_DIR/kraft_one_down_producer.txt"

stage=two_down
dc stop kafka-2
dc ps --services --status running > "$HARNESS_REPORT_DIR/kraft_two_down_running.txt"
if dc exec -T kafka-1 timeout 12 kafka-metadata-quorum --bootstrap-server kafka-1:9092 describe --status > "$HARNESS_REPORT_DIR/kraft_two_down_status.txt" 2>&1; then
  printf '0\n' > "$HARNESS_REPORT_DIR/kraft_two_down_status_exit.txt"
else
  printf '%s\n' "$?" > "$HARNESS_REPORT_DIR/kraft_two_down_status_exit.txt"
fi
dc run --rm --no-deps tester two-down > "$HARNESS_REPORT_DIR/kraft_two_down_producer.txt"

stage=recovery
dc start kafka-2 kafka-3
dc up -d --wait --wait-timeout 180 kafka-2 kafka-3
for attempt in $(seq 1 30); do
  if dc exec -T kafka-1 kafka-metadata-quorum --bootstrap-server kafka-1:9092 describe --status > "$HARNESS_REPORT_DIR/kraft_recovered_status.txt" 2>&1 \
    && dc exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic asiaSocketDataInBTC-ticker > "$HARNESS_REPORT_DIR/kraft_recovered_topic.txt" 2>&1 \
    && python3 -c 'import re,sys; s=open(sys.argv[1]).read(); m=re.search(r"\bIsr:\s*([0-9,]+)",s); sys.exit(0 if m and set(m.group(1).split(","))=={"1","2","3"} else 1)' "$HARNESS_REPORT_DIR/kraft_recovered_topic.txt"; then
    break
  fi
  sleep 2
done
dc run --rm --no-deps tester recovered > "$HARNESS_REPORT_DIR/kraft_recovered_producer.txt"
python3 "$here/harness/verify_kraft_failover.py" "$HARNESS_REPORT_DIR"
stage=complete
