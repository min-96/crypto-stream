#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/verify_connections.py
