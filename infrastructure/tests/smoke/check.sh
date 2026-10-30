#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/../.."
docker compose ps
docker compose exec -T kafka-1 kafka-metadata-quorum --bootstrap-server kafka-1:9092 describe --status
docker compose exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe
docker compose exec -T mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SHOW TABLES"'
curl -fsS http://localhost:8083/connector-plugins
printf '\nSpark master UI: http://localhost:8080\n'
