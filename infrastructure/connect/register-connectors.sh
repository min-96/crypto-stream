#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
for name in preprocessed-to-minio processed-to-minio; do
  docker compose exec -T connect curl -fsS -X PUT -H 'Content-Type: application/json' \
    --data-binary "@/connector-configs/$name.json" \
    "http://localhost:8083/connectors/$name/config"
  printf '\n'
done
