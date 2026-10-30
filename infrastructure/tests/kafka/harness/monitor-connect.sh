#!/bin/bash
set -u

case "${1:-}" in
  memory)
    printf 'timestamp|memory_current_bytes|memory_peak_bytes|memory_events\n'
    while true; do
      timestamp=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
      current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || true)
      peak=$(cat /sys/fs/cgroup/memory.peak 2>/dev/null || true)
      events=$(tr '\n' ',' < /sys/fs/cgroup/memory.events 2>/dev/null || true)
      printf '%s|%s|%s|%s\n' "$timestamp" "$current" "$peak" "$events"
      sleep 0.25
    done
    ;;
  jvm)
    java_pid=$(jcmd -l 2>/dev/null | awk '$2 ~ /ConnectDistributed/ {print $1; exit}')
    java_pid=${java_pid:-1}
    printf 'timestamp|jstat_gc\n'
    while true; do
      timestamp=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
      gc=$(jstat -gc "$java_pid" 2>/dev/null | tail -n 1 | awk '{$1=$1; print}')
      printf '%s|%s\n' "$timestamp" "$gc"
      sleep 0.5
    done
    ;;
  tasks)
    printf 'timestamp|connector_status\n'
    while true; do
      timestamp=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
      status=$(curl -fsS --max-time 0.5 http://localhost:8083/connectors/preprocessed-to-minio/status 2>/dev/null | tr -d '\n' || true)
      printf '%s|%s\n' "$timestamp" "$status"
      sleep 0.5
    done
    ;;
  *) echo 'usage: monitor-connect.sh memory|jvm|tasks' >&2; exit 2 ;;
esac
