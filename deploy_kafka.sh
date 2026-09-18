#!/usr/bin/env bash
#
# deploy_kafka.sh
#
# Deploys a single-node Apache Kafka broker as a Docker container on a Linux
# host, running in KRaft mode (no separate Zookeeper container needed).
#
# Usage:
#   ./deploy_kafka.sh [options]
#
# Options:
#   -n, --name NAME          Container name (default: kafka-broker)
#   -i, --image IMAGE        Kafka image to use (default: apache/kafka:3.8.0)
#   -p, --port PORT          Host port to expose the broker on (default: 9092)
#   -H, --host HOSTNAME      Advertised hostname clients will use to reach the
#                            broker (default: localhost). Set this to the
#                            machine's real hostname/IP if clients will connect
#                            from other machines on the network.
#   --data-dir DIR           Host directory to persist Kafka log data
#                            (default: ./kafka-data)
#   --recreate                If a container with this name already exists,
#                            stop and remove it before creating a new one.
#   -h, --help                Show this help text and exit
#
# Requirements: Docker must already be installed and the invoking user must
# be able to run docker commands (either as root or a member of the "docker"
# group).

set -euo pipefail

# ---- Defaults ---------------------------------------------------------
CONTAINER_NAME="kafka-broker"
IMAGE="apache/kafka:3.8.0"
HOST_PORT="9092"
ADVERTISED_HOST="localhost"
DATA_DIR="$(pwd)/kafka-data"
RECREATE=false

# ---- Argument parsing ---------------------------------------------------
print_help() {
    grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--name)
            CONTAINER_NAME="$2"; shift 2 ;;
        -i|--image)
            IMAGE="$2"; shift 2 ;;
        -p|--port)
            HOST_PORT="$2"; shift 2 ;;
        -H|--host)
            ADVERTISED_HOST="$2"; shift 2 ;;
        --data-dir)
            DATA_DIR="$2"; shift 2 ;;
        --recreate)
            RECREATE=true; shift ;;
        -h|--help)
            print_help; exit 0 ;;
        *)
            echo "Unknown option: $1" >&2
            print_help
            exit 1 ;;
    esac
done

# ---- Pre-flight checks --------------------------------------------------
if ! command -v docker &>/dev/null; then
    echo "Error: docker is not installed or not on PATH." >&2
    echo "Install Docker first, e.g.: curl -fsSL https://get.docker.com | sh" >&2
    exit 1
fi

if ! docker info &>/dev/null; then
    echo "Error: could not talk to the Docker daemon." >&2
    echo "Is Docker running, and does this user have permission to use it" >&2
    echo "(e.g. member of the 'docker' group, or run this script with sudo)?" >&2
    exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    if [[ "${RECREATE}" == true ]]; then
        echo "Removing existing container '${CONTAINER_NAME}'..."
        docker rm -f "${CONTAINER_NAME}" >/dev/null
    else
        echo "Error: a container named '${CONTAINER_NAME}' already exists." >&2
        echo "Re-run with --recreate to replace it, or pick a different --name." >&2
        exit 1
    fi
fi

mkdir -p "${DATA_DIR}"

# ---- Deploy --------------------------------------------------------------
echo "Pulling image ${IMAGE}..."
docker pull "${IMAGE}"

echo "Starting container '${CONTAINER_NAME}' on port ${HOST_PORT}..."

# KRaft mode runs Kafka without Zookeeper: the broker and controller roles
# are combined into a single process/node, which is enough for local dev
# and testing. CLUSTER_ID is a fixed, valid base64 UUID; any 16-byte
# base64-encoded value works, it just has to be consistent for the life of
# the cluster's data directory.
docker run -d \
    --name "${CONTAINER_NAME}" \
    -p "${HOST_PORT}:9092" \
    -v "${DATA_DIR}:/var/lib/kafka/data" \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" \
    -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${ADVERTISED_HOST}:${HOST_PORT}" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093" \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e CLUSTER_ID="ciWo7IWazngRchmPES6q5A==" \
    "${IMAGE}"

echo ""
echo "Waiting for the broker to become ready..."
ATTEMPTS=0
until docker exec "${CONTAINER_NAME}" /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 &>/dev/null; do
    ATTEMPTS=$((ATTEMPTS + 1))
    if [[ ${ATTEMPTS} -ge 30 ]]; then
        echo "Error: broker did not become ready in time. Check 'docker logs ${CONTAINER_NAME}'." >&2
        exit 1
    fi
    sleep 2
done

echo ""
echo "Kafka is up."
echo "  Container name:      ${CONTAINER_NAME}"
echo "  Bootstrap server:     ${ADVERTISED_HOST}:${HOST_PORT}"
echo "  Data persisted to:    ${DATA_DIR}"
echo ""
echo "Try it out:"
echo "  python3 kafka_producer.py --bootstrap-server ${ADVERTISED_HOST}:${HOST_PORT} --topic test-topic"
echo "  python3 kafka_consumer.py --bootstrap-server ${ADVERTISED_HOST}:${HOST_PORT} --topic test-topic"
echo ""
echo "To stop:   docker stop ${CONTAINER_NAME}"
echo "To remove: docker rm -f ${CONTAINER_NAME}"
