#!/usr/bin/env bash
# Run apps/average tests inside the project Spark image (pyspark 3.5.3, Python 3.8).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
exec docker run --rm -v "$repo:/repo:ro" -w /repo/apps/average \
    -e PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    -e PYTHONDONTWRITEBYTECODE=1 \
    coin-market-spark:3.5.3-kafka-mysql \
    python3 -m unittest discover -s tests -v "$@"
