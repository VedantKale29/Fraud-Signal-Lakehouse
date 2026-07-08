#!/usr/bin/env bash
# Boot the free local dev stack and smoke-check it (Stage-0 gate SS3.2)
set -euo pipefail
docker compose up -d
echo "waiting for kafka..." && sleep 8
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic onchain.transactions --partitions 3
echo "kafka topic OK. minio console -> http://localhost:9001"
