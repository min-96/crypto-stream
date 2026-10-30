#!/bin/bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
compose="$here/compose.yaml"
keep=0
if [[ ${1:-} == --keep ]]; then
  keep=1
elif [[ $# -ne 0 ]]; then
  echo "usage: $0 [--keep]" >&2
  exit 2
fi

project="coin-market-kaf-harness-$(date +%s)-$$"
stage=setup
case "$project" in
  coin-market-kaf-harness-*) ;;
  *) echo 'unsafe Compose project name' >&2; exit 2 ;;
esac
export HARNESS_CLUSTER_ID
HARNESS_CLUSTER_ID=$(python3 -c 'import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
export HARNESS_REPORT_DIR="$here/reports/$project"
mkdir -p "$HARNESS_REPORT_DIR"
chmod 777 "$HARNESS_REPORT_DIR"
printf 'HARNESS_CLUSTER_ID=%s\nHARNESS_REPORT_DIR=%s\n' \
  "$HARNESS_CLUSTER_ID" "$HARNESS_REPORT_DIR" > "$HARNESS_REPORT_DIR/harness.env"

dc() { docker compose --project-name "$project" -f "$compose" "$@"; }
cleanup() {
  result=$?
  printf '{"exit_code":%d,"stage":"%s","project":"%s"}\n' \
    "$result" "$stage" "$project" > "$HARNESS_REPORT_DIR/result.json"
  # Restore nodes even when a check fails; only this dedicated project is touched.
  dc start kafka-2 kafka-3 >/dev/null 2>&1 || true
  if (( keep == 0 )); then
    dc down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  echo "HARNESS_RESULT=$result PROJECT=$project REPORT=$HARNESS_REPORT_DIR KEEP=$keep"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "HARNESS_PROJECT=$project CLUSTER_ID=$HARNESS_CLUSTER_ID"
echo 'Starting isolated Kafka, MinIO and Connect services'
stage=startup
dc up -d --build --wait --wait-timeout 240 connect minio
dc build tester spark-test
dc run --rm --no-deps checkpoint-init

echo 'Checking Topic contract against the live isolated cluster'
stage=topics
dc exec -T kafka-1 bash -s -- --check < "$here/../../../kafka/init-topics.sh"

before=$(dc exec -T kafka-1 kafka-metadata-quorum --bootstrap-server kafka-1:9092 describe --status)
echo "$before"
cluster_id=$(printf '%s\n' "$before" | awk '/^ClusterId:/ {print $2}')
[[ -n "$cluster_id" ]]
printf '%s\n' "$before" | grep -Eq '^CurrentVoters:[[:space:]]+\[1,2,3\]$'

echo 'Checking Raw Ticker/Orderbook -> KRW Preprocessed records'
stage=preprocessing
dc run --rm --no-deps --entrypoint python tester /harness/check_preprocessing.py

echo 'Checking preprocessing ACK, commit and restart boundaries'
stage=preprocessing_offsets
dc run --rm --no-deps --entrypoint python tester /harness/check_offsets.py

echo 'Checking fake WebSocket -> Collector -> Kafka and Connect -> MinIO'
stage=collector_connect_minio
dc run --rm --no-deps tester pipeline

echo 'Checking Spark Preprocessed source and checkpoint resume'
stage=spark
dc run --rm --no-deps spark-test first
dc run --rm --no-deps spark-test resume
dc run --rm --no-deps tester spark-archive

echo 'Checking KRaft baseline and one-node write availability'
stage=baseline
dc run --rm --no-deps tester baseline
stage=one_node_down
dc stop kafka-3
one_down_quorum=''
one_down_leader=''
for attempt in $(seq 1 20); do
  if one_down_quorum=$(dc exec -T kafka-1 kafka-metadata-quorum --bootstrap-server kafka-1:9092 describe --status 2>/dev/null); then
    one_down_leader=$(printf '%s\n' "$one_down_quorum" | awk '/^LeaderId:/ {print $2}')
    if [[ "$one_down_leader" == 1 || "$one_down_leader" == 2 ]]; then
      break
    fi
  fi
  sleep 2
done
echo "$one_down_quorum"
[[ "$one_down_leader" == 1 || "$one_down_leader" == 2 ]]
dc exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic asiaSocketDataInBTC-ticker
dc run --rm --no-deps tester one-down

echo 'Checking two-node quorum loss and acks=all failure'
stage=two_nodes_down
dc stop kafka-2
dc run --rm --no-deps tester two-down

echo 'Restoring both nodes and checking Cluster ID and messages'
stage=recovery
dc start kafka-2 kafka-3
dc up -d --wait --wait-timeout 180 kafka-2 kafka-3
after=''
for attempt in $(seq 1 20); do
  if after=$(dc exec -T kafka-1 kafka-metadata-quorum --bootstrap-server kafka-1:9092 describe --status 2>/dev/null); then
    break
  fi
  sleep 2
done
[[ -n "$after" ]]
echo "$after"
recovered_id=$(printf '%s\n' "$after" | awk '/^ClusterId:/ {print $2}')
[[ "$cluster_id" == "$recovered_id" ]]
dc run --rm --no-deps tester recovered
stage=complete
echo 'KAFKA_HARNESS_OK'
