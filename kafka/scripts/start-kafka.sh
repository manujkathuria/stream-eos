#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONFIG_FILE="${PROJECT_ROOT}/config/kraft-broker.properties"
DATA_DIR="${PROJECT_ROOT}/data/kafka"
LOGS_DIR="${PROJECT_ROOT}/logs"
PID_FILE="${PROJECT_ROOT}/.kafka.pid"
CONSOLE_LOG="${LOGS_DIR}/kafka-server.log"

# Check if Kafka is already running
if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
    if ps -p "${PID}" > /dev/null 2>&1; then
        echo "Kafka is already running with PID ${PID}."
        exit 0
    else
        echo "Stale PID file found. Removing..."
        rm -f "${PID_FILE}"
    fi
fi

# Ensure KRaft storage is initialized before startup
if [ ! -f "${DATA_DIR}/meta.properties" ]; then
    echo "KRaft storage not found. Initializing..."
    "${SCRIPT_DIR}/init-kraft.sh"
fi

mkdir -p "${LOGS_DIR}"

# Modest JVM Heap limit: 512 MB
export KAFKA_HEAP_OPTS="-Xms512M -Xmx512M"
export LOG_DIR="${LOGS_DIR}"

echo "Starting Apache Kafka (KRaft mode, 512MB heap)..."
nohup kafka-server-start "${CONFIG_FILE}" > "${CONSOLE_LOG}" 2>&1 &
KAFKA_PID=$!
echo "${KAFKA_PID}" > "${PID_FILE}"

echo "Kafka started with PID ${KAFKA_PID}."
echo "Logs are being written to: ${CONSOLE_LOG}"

# Wait up to 15 seconds for broker listener port (9092) to become active
echo "Waiting for Kafka to bind to localhost:9092..."
MAX_WAIT=15
WAIT_COUNT=0
while ! nc -z localhost 9092 > /dev/null 2>&1; do
    if ! ps -p "${KAFKA_PID}" > /dev/null 2>&1; then
        echo "Error: Kafka failed to start. Check ${CONSOLE_LOG} for details."
        rm -f "${PID_FILE}"
        exit 1
    fi
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
    if [ ${WAIT_COUNT} -ge ${MAX_WAIT} ]; then
        echo "Warning: Kafka process is running (PID ${KAFKA_PID}), but port 9092 is not answering yet."
        exit 0
    fi
done

echo "Kafka is healthy and listening on localhost:9092."
