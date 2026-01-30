#!/usr/bin/env bash
set -euo pipefail

# Creates Kafka topics inside the docker-compose stack.
# Idempotent: does not fail if topics already exist.

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

TOPICS=(
  "query_metrics_raw"
  "query_metrics_processed"
)

PARTITIONS="${KAFKA_PARTITIONS:-3}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"

echo "Using container: ${KAFKA_CONTAINER}"
echo "Bootstrap: ${BOOTSTRAP}"

for topic in "${TOPICS[@]}"; do
  echo "Ensuring topic exists: ${topic}"
  docker exec "${KAFKA_CONTAINER}" \
    kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}" >/dev/null
done

echo "✅ Topics ready: ${TOPICS[*]}"