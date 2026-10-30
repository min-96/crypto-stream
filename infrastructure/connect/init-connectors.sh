#!/bin/bash
set -euo pipefail
for name in preprocessed-to-minio processed-to-minio; do
  for attempt in $(seq 1 30); do
    if curl --fail --silent --show-error --max-time 15 \
      -X PUT -H 'Content-Type: application/json' \
      --data-binary "@/connector-configs/${name}.json" \
      "http://connect:8083/connectors/${name}/config" >/dev/null; then
      echo "Registered ${name}"
      break
    fi
    if [ "$attempt" -eq 30 ]; then
      echo "Failed to register ${name}" >&2
      exit 1
    fi
    sleep 2
  done
done
