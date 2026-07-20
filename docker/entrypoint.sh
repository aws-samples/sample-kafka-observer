#!/usr/bin/env bash
# =============================================================================
# entrypoint.sh — render server.properties from env vars and start the broker.
#
# Required env:
#   BROKER_ID            unique integer id (1, 2, 3, ...)
#   ZOOKEEPER_CONNECT    e.g. zookeeper:2181
#   EXTERNAL_PORT        host-published port for the EXTERNAL listener
#   EXTERNAL_HOST        hostname clients on the host machine use (localhost)
#
# Listener layout:
#   INTERNAL://<container>:9092      inter-broker + in-network clients
#   EXTERNAL://localhost:<port>      host clients via published ports
#
# Observer wiring: the patched jars read /opt/kafka/observer.ids (path set via
# KAFKA_OBSERVER_IDS_FILE in the image, 5 s cache). docker-compose bind-mounts
# ./observer.ids there on every broker, so editing the file on the host
# promotes/demotes with zero restarts.
# =============================================================================
set -euo pipefail

: "${BROKER_ID:?BROKER_ID is required}"
: "${ZOOKEEPER_CONNECT:?ZOOKEEPER_CONNECT is required}"
: "${EXTERNAL_PORT:?EXTERNAL_PORT is required}"
EXTERNAL_HOST="${EXTERNAL_HOST:-localhost}"
CONTAINER_NAME="${CONTAINER_NAME:-$(hostname)}"
DATA_DIR=/opt/kafka/data
CONFIG=/opt/kafka/config/server.properties

# Wait for ZooKeeper before starting (broker exits fast if ZK is unreachable).
echo "Waiting for ZooKeeper at ${ZOOKEEPER_CONNECT} ..."
ZK_HOST="${ZOOKEEPER_CONNECT%%:*}"
ZK_PORT="${ZOOKEEPER_CONNECT##*:}"
for i in $(seq 1 60); do
  if (exec 3<>"/dev/tcp/${ZK_HOST}/${ZK_PORT}") 2>/dev/null; then
    exec 3>&- || true
    echo "ZooKeeper is reachable."
    break
  fi
  [ "$i" = 60 ] && { echo "ERROR: ZooKeeper not reachable after 60 s" >&2; exit 1; }
  sleep 1
done

mkdir -p "$DATA_DIR"

cat > "$CONFIG" <<EOF
# Rendered by entrypoint.sh — local verification cluster (ZK mode)
broker.id=${BROKER_ID}
zookeeper.connect=${ZOOKEEPER_CONNECT}

listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:${EXTERNAL_PORT}
advertised.listeners=INTERNAL://${CONTAINER_NAME}:9092,EXTERNAL://${EXTERNAL_HOST}:${EXTERNAL_PORT}
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL

log.dirs=${DATA_DIR}
num.partitions=1
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# Keep ISR reactions snappy for the promote/demote demo (native machinery).
replica.lag.time.max.ms=10000
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
EOF

echo "── server.properties ──"
cat "$CONFIG"
echo "── observer.ids (${KAFKA_OBSERVER_IDS_FILE:-/opt/kafka/observer.ids}) ──"
cat "${KAFKA_OBSERVER_IDS_FILE:-/opt/kafka/observer.ids}" 2>/dev/null || echo "(missing — no observers)"

export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xmx512m -Xms512m}"
exec /opt/kafka/bin/kafka-server-start.sh "$CONFIG"
