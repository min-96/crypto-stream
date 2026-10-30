#!/bin/bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
compose="$here/harness/compose.yaml"
override="$here/harness/compose-315-memory.yaml"
memory_mode=${KAF315_MEMORY_MODE:-bounded}
batch_size=${BENCH_PREPROCESS_BATCH_SIZE:-32}
batch_linger_ms=${BENCH_PREPROCESS_BATCH_LINGER_MS:-250}
if [[ "$memory_mode" == candidate || "$memory_mode" == batch ]]; then
  bench_duration=${BENCH_DURATION_SECONDS:-80}
  bench_updates=${BENCH_UPDATES_PER_KEY:-12}
else
  bench_duration=${BENCH_DURATION_SECONDS:-75}
  bench_updates=${BENCH_UPDATES_PER_KEY:-2}
fi
case "$memory_mode" in bounded|default|candidate|batch) ;; *) echo "KAF315_MEMORY_MODE must be bounded, default, candidate, or batch" >&2; exit 2 ;; esac
compose_files=(-f "$compose")
if [[ "$memory_mode" == bounded ]]; then compose_files+=(-f "$override"); fi
if [[ "$memory_mode" == candidate || "$memory_mode" == batch ]]; then compose_files+=(-f "$here/harness/compose-315-candidate.yaml"); fi
if [[ "$memory_mode" == batch ]]; then compose_files+=(-f "$here/harness/compose-315-batch.yaml"); fi
project="coin-market-kaf315-sustained-$(date +%s)-$$"
case "$project" in coin-market-kaf315-sustained-*) ;; *) exit 2 ;; esac
export HARNESS_CLUSTER_ID HARNESS_REPORT_DIR
HARNESS_CLUSTER_ID=$(python3 -c 'import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
HARNESS_REPORT_DIR="$here/harness/reports/$project"
mkdir -p "$HARNESS_REPORT_DIR"
chmod 777 "$HARNESS_REPORT_DIR"
printf 'HARNESS_CLUSTER_ID=%s\nHARNESS_REPORT_DIR=%s\nKAF315_MEMORY_MODE=%s\nBENCH_PREPROCESS_BATCH_SIZE=%s\nBENCH_PREPROCESS_BATCH_LINGER_MS=%s\n' \
  "$HARNESS_CLUSTER_ID" "$HARNESS_REPORT_DIR" "$memory_mode" "$batch_size" "$batch_linger_ms" > "$HARNESS_REPORT_DIR/harness.env"
dc() { docker compose --project-name "$project" "${compose_files[@]}" "$@"; }
stage=startup
monitor_pid=
diag_memory_pid=
diag_jvm_pid=
diag_tasks_pid=
connect_id=
minio_id=
cleanup() {
  result=$?
  if [[ -n "$monitor_pid" ]]; then kill "$monitor_pid" 2>/dev/null || true; wait "$monitor_pid" 2>/dev/null || true; fi
  for pid in "$diag_memory_pid" "$diag_jvm_pid" "$diag_tasks_pid"; do
    if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  done
  if [[ -n "$connect_id" ]]; then
    docker inspect --format '{{json .State}}' "$connect_id" > "$HARNESS_REPORT_DIR/connect_state.json" 2>/dev/null || true
    docker logs "$connect_id" > "$HARNESS_REPORT_DIR/connect.log" 2>&1 || true
  fi
  printf '{"exit_code":%d,"stage":"%s","project":"%s","memory_mode":"%s"}\n' \
    "$result" "$stage" "$project" "$memory_mode" > "$HARNESS_REPORT_DIR/result.json"
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  echo "KAF315_SUSTAINED_RESULT=$result PROJECT=$project REPORT=$HARNESS_REPORT_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

dc up -d --build --wait --wait-timeout 240 connect minio
dc build tester
connect_id=$(dc ps -q connect)
minio_id=$(dc ps -q minio)
if [[ "${KAF315_DIAGNOSTICS:-0}" == 1 ]]; then
  docker exec "$connect_id" bash /monitor-connect.sh memory > "$HARNESS_REPORT_DIR/connect_cgroup_memory.tsv" 2> "$HARNESS_REPORT_DIR/connect_cgroup_memory.err" &
  diag_memory_pid=$!
  docker exec "$connect_id" bash /monitor-connect.sh jvm > "$HARNESS_REPORT_DIR/connect_jvm_gc.tsv" 2> "$HARNESS_REPORT_DIR/connect_jvm_gc.err" &
  diag_jvm_pid=$!
  docker exec "$connect_id" bash /monitor-connect.sh tasks > "$HARNESS_REPORT_DIR/connect_tasks.tsv" 2> "$HARNESS_REPORT_DIR/connect_tasks.err" &
  diag_tasks_pid=$!
fi
(
  while true; do
    timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    stats=$(docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}|{{.BlockIO}}' "$connect_id" "$minio_id" 2>/dev/null || true)
    while IFS='|' read -r name cpu memory memory_percent block_io; do
      case "$name" in
        "$project"-connect-*) service=connect; printf '%s|%s|%s\n' "$timestamp" "$memory" "$memory_percent" >> "$HARNESS_REPORT_DIR/connect_memory.tsv" ;;
        "$project"-minio-*) service=minio ;;
        *) continue ;;
      esac
      printf '%s|%s|%s|%s|%s\n' "$timestamp" "$service" "$cpu" "$memory" "$block_io" >> "$HARNESS_REPORT_DIR/resource_usage.tsv"
    done <<< "$stats"
    sleep 5
  done
) &
monitor_pid=$!
if [[ "$memory_mode" != candidate && "$memory_mode" != batch ]]; then
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
fi

stage=sustained
dc run --rm --no-deps \
  -e BENCH_PREPROCESS_BATCH_SIZE="$batch_size" \
  -e BENCH_PREPROCESS_BATCH_LINGER_MS="$batch_linger_ms" \
  -e BENCH_RESTART_CONNECT_SECONDS="${BENCH_RESTART_CONNECT_SECONDS:-0}" \
  -e BENCH_DURATION_SECONDS="$bench_duration" \
  -e BENCH_UPDATES_PER_KEY="$bench_updates" \
  -v "$here/harness/bench_315_pipeline.py:/harness/bench_315_pipeline.py:ro" \
  -v "$here/harness/verify_batch_archive.py:/harness/verify_batch_archive.py:ro" \
  -v "$here/harness/bench_315_sustained.py:/harness/bench_315_sustained.py:ro" \
  --entrypoint python tester /harness/bench_315_sustained.py
stage=complete
echo 'KAF315_SUSTAINED_HARNESS_OK'
