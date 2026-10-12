#!/usr/bin/env bash
set -euo pipefail

# Determine Project Root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONFIG_FILE="${PROJECT_ROOT}/config/kraft-broker.properties"
DATA_DIR="${PROJECT_ROOT}/data/kafka"
LOGS_DIR="${PROJECT_ROOT}/logs"
CLUSTER_ID_FILE="${PROJECT_ROOT}/.cluster_id"

mkdir -p "${DATA_DIR}" "${LOGS_DIR}"

# 1. Check if storage is already formatted (meta.properties existence in data dir)
if [ -f "${DATA_DIR}/meta.properties" ]; then
    echo "KRaft storage is already initialized in ${DATA_DIR}."
    if [ -f "${CLUSTER_ID_FILE}" ]; then
        echo "Cluster ID: $(cat "${CLUSTER_ID_FILE}")"
    fi
    exit 0
fi

echo "Initializing KRaft storage in ${DATA_DIR}..."

# Generate cluster ID if not already cached
if [ ! -f "${CLUSTER_ID_FILE}" ]; then
    CLUSTER_ID=$(kafka-storage random-uuid | tail -n 1 | tr -d '\r\n ')
    echo "${CLUSTER_ID}" > "${CLUSTER_ID_FILE}"
else
    CLUSTER_ID=$(cat "${CLUSTER_ID_FILE}" | tr -d '\r\n ')
fi

echo "Using Cluster ID: ${CLUSTER_ID}"

# In Kafka 4.x with controller.quorum.bootstrap.servers, single-node dynamic quorum uses --standalone (-s)
kafka-storage format \
    --cluster-id "${CLUSTER_ID}" \
    --config "${CONFIG_FILE}" \
    --standalone

echo "KRaft storage successfully formatted."
