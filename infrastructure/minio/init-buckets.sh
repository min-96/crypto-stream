#!/bin/sh
set -eu
attempt=0
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 30 ] || exit 1
  sleep 2
done
mc mb --ignore-existing local/preprocessed
mc mb --ignore-existing local/processed
