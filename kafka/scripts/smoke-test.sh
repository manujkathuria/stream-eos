#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TOPIC="smoke-test-$(date +%s)"
BOOTSTRAP_SERVER="localhost:9092"
MESSAGE="hello-kafka-from-stream-eos-$(date +%s)"

echo "=== Running Kafka Smoke Test ==="

# Check broker connectivity
if ! nc -z localhost 9092 > /dev/null 2>&1; then
    echo "Error: Kafka broker is not responding on ${BOOTSTRAP_SERVER}."
    echo "Please start Kafka first using: make start"
    exit 1
fi

echo "1. Creating topic: ${TOPIC} (replication-factor 1, partitions 1)..."
kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --topic "${TOPIC}" \
    --partitions 1 \
    --replication-factor 1

echo "2. Producing message: '${MESSAGE}'..."
echo "${MESSAGE}" | kafka-console-producer \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${TOPIC}"

echo "3. Consuming message from topic..."
RECEIVED=$(kafka-console-consumer \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${TOPIC}" \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 10000 2>/dev/null | grep "${MESSAGE}" || true)

if [ "${RECEIVED}" = "${MESSAGE}" ]; then
    echo "SUCCESS: Message received matches produced message: '${RECEIVED}'"
else
    echo "ERROR: Received message '${RECEIVED}' does not match expected '${MESSAGE}'."
    exit 1
fi

echo "4. Cleaning up smoke-test topic..."
kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --delete --topic "${TOPIC}" 2>/dev/null || true

echo "=== Smoke Test PASSED Successfully! ==="
