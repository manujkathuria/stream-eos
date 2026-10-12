#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PID_FILE="${PROJECT_ROOT}/.kafka.pid"

if [ ! -f "${PID_FILE}" ]; then
    # Double check if any kafka process is running using this project's config
    RUNNING_PID=$(pgrep -f "kraft-broker.properties" || true)
    if [ -n "${RUNNING_PID}" ]; then
        echo "Found Kafka process without PID file: PID ${RUNNING_PID}."
        PID="${RUNNING_PID}"
    else
        echo "Kafka is not running (no PID file found)."
        exit 0
    fi
else
    PID=$(cat "${PID_FILE}")
fi

if ps -p "${PID}" > /dev/null 2>&1; then
    echo "Stopping Kafka (PID ${PID})..."
    kill -15 "${PID}" || true

    # Wait for process termination
    WAIT_COUNT=0
    MAX_WAIT=20
    while ps -p "${PID}" > /dev/null 2>&1; do
        sleep 1
        WAIT_COUNT=$((WAIT_COUNT + 1))
        if [ ${WAIT_COUNT} -ge ${MAX_WAIT} ]; then
            echo "Kafka did not shut down gracefully within ${MAX_WAIT}s. Forcing kill..."
            kill -9 "${PID}" || true
            break
        fi
    done
    echo "Kafka stopped."
else
    echo "Kafka process (PID ${PID}) was not running."
fi

rm -f "${PID_FILE}"
