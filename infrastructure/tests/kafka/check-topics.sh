#!/bin/bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
# Read-only inspection of the running cluster. No topic is created or altered.
docker compose -f "$root/compose.yaml" exec -T kafka-1 bash -s -- --check < "$root/kafka/init-topics.sh"
