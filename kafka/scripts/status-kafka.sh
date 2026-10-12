#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PID_FILE="${PROJECT_ROOT}/.kafka.pid"
LOGS_DIR="${PROJECT_ROOT}/logs"

echo "=== Kafka Status Check ==="

PID=""
if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
fi

if [ -n "${PID}" ] && ps -p "${PID}" > /dev/null 2>&1; then
    echo "• Process Status: RUNNING (PID: ${PID})"

    # Heap / JVM stats
    echo "• JVM Process Details:"
    ps -o pid,ppid,%cpu,%mem,rss,command -p "${PID}" | head -n 2
else
    # Check if running without PID file
    RUNNING_PID=$(pgrep -f "kraft-broker.properties" || true)
    if [ -n "${RUNNING_PID}" ]; then
        echo "• Process Status: RUNNING (PID: ${RUNNING_PID}, unmanaged PID file)"
    else
        echo "• Process Status: STOPPED"
        exit 0
    fi
fi

echo ""
echo "• Port Connectivity (localhost:9092):"
if nc -z localhost 9092 > /dev/null 2>&1; then
    echo "  -> SUCCESS: Port 9092 is OPEN and listening."
else
    echo "  -> WARNING: Port 9092 is NOT responding."
fi

echo ""
echo "• Broker Metadata & Topics Check:"
if kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "  -> SUCCESS: Broker is reachable via kafka-topics."
    echo "  -> Existing Topics:"
    kafka-topics --bootstrap-server localhost:9092 --list | sed 's/^/     /'
else
    echo "  -> FAILED: Could not query Kafka broker metadata."
fi
