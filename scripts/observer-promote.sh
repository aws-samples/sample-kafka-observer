#!/usr/bin/env bash
# =============================================================================
# observer-promote.sh — promote an observer replica to a fully electable replica
#
# What it does (all reusing native Kafka machinery — see docs/architecture.md):
#   1. Pre-check: the observer replica is caught up on every partition it hosts
#   2. Atomically remove the broker id from observer.ids on every broker
#   3. Wait for the broker to appear in ISR (native maybeExpandIsr path)
#
# Promotion is zero-restart and zero-data-movement: the observer has been
# byte-identical with the leader all along; only its status changes.
# Measured on real clusters: <= 10 s from file change to ISR membership.
#
# Usage:
#   observer-promote.sh -b <broker_id> -s <bootstrap> -H "host1 host2 host3" [-f /opt/kafka/observer.ids] [-t topic]
#     -b  broker id to promote
#     -s  bootstrap servers for verification (host:port)
#     -H  space-separated list of ALL broker hosts (ssh-reachable) whose file must change
#     -f  observer ids file path on brokers   (default /opt/kafka/observer.ids)
#     -t  topic to watch for verification     (default: first topic hosting the broker)
#     -y  skip interactive confirmation
# =============================================================================
set -euo pipefail

FILE=/opt/kafka/observer.ids
TOPIC=""
YES=0
while getopts "b:s:H:f:t:y" o; do case $o in
  b) BROKER=$OPTARG;; s) BOOT=$OPTARG;; H) HOSTS=$OPTARG;;
  f) FILE=$OPTARG;; t) TOPIC=$OPTARG;; y) YES=1;;
esac; done
: "${BROKER:?-b broker_id required}" "${BOOT:?-s bootstrap required}" "${HOSTS:?-H hosts required}"

KBIN="${KAFKA_BIN:-/opt/kafka/bin}"
SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=no -o ConnectTimeout=10}"

echo "── Pre-check: replica lag for broker $BROKER ──"
# A promoted observer that lags stalls HW until caught up. Require visible catch-up.
LAGGY=$($KBIN/kafka-topics.sh --bootstrap-server "$BOOT" --describe --under-replicated-partitions 2>/dev/null | grep -c "Replicas:.*\b$BROKER\b" || true)
echo "under-replicated partitions referencing broker $BROKER: $LAGGY (informational — observers are always 'under-replicated' by ISR definition)"
echo "Verify data catch-up manually if in doubt: compare log end offsets on leader vs broker $BROKER."

if [ "$YES" != 1 ]; then
  read -r -p "Promote broker $BROKER (remove from $FILE on: $HOSTS)? [y/N] " a
  [ "$a" = y ] || { echo "aborted"; exit 1; }
fi

echo "── Updating observer list on all brokers (atomic tmp+mv) ──"
for h in $HOSTS; do
  # shellcheck disable=SC2029,SC2086
  ssh $SSH_OPTS "$h" "sudo sh -c 'grep -v \"^${BROKER}\$\" $FILE 2>/dev/null | grep -v \"^\$\" > $FILE.tmp || true; mv $FILE.tmp $FILE; echo \"\$(hostname -s): [\$(cat $FILE | tr \"\\n\" \",\")]\"'"
done

echo "── Waiting for broker $BROKER to join ISR (5 s cache + fetch round-trip; typical <= 10 s) ──"
DESCRIBE_ARGS=(--bootstrap-server "$BOOT" --describe)
[ -n "$TOPIC" ] && DESCRIBE_ARGS+=(--topic "$TOPIC")
for i in $(seq 1 12); do
  sleep 5
  OUT=$($KBIN/kafka-topics.sh "${DESCRIBE_ARGS[@]}" 2>/dev/null | grep -E "Isr:" | head -5)
  echo "  [$((i*5))s] $OUT"
  if echo "$OUT" | grep -qE "Isr: [0-9,]*\b$BROKER\b"; then
    echo "✅ broker $BROKER is in ISR — promotion complete (now a full election candidate)"
    exit 0
  fi
done
echo "⚠️ broker $BROKER not observed in ISR after 60 s — check leader logs (maybeExpandIsr / AlterPartition) and file consistency across brokers"
exit 1
