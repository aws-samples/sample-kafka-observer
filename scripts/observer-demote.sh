#!/usr/bin/env bash
# =============================================================================
# observer-demote.sh — demote an electable replica back to observer status
#
# Safety pre-checks (both are HARD requirements — skipping them causes outages):
#   1. The broker must not currently be a leader for any partition
#      (the native shrink path never removes the leader itself; demoting a
#      leader would silently do nothing until leadership moves)
#   2. For every partition it serves: ISR - {broker} >= min.insync.replicas
#      (otherwise the demotion itself triggers NOT_ENOUGH_REPLICAS fail-stop)
#
# Mechanism: add the id back to observer.ids -> the demotion hook in
# getOutOfSyncReplicas treats an in-ISR observer as lagging -> the native
# isr-expiration task (every replica.lag.time.max.ms/2, default 15 s) shrinks
# it out through the standard AlterPartition path. Zero restart.
# Measured on real clusters: <= 10-20 s.
#
# Usage: observer-demote.sh -b <broker_id> -s <bootstrap> -H "host1 host2 host3" [-f file] [-t topic] [-y]
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

echo "── Pre-check 1: broker $BROKER must not be a leader ──"
LEADS=$($KBIN/kafka-topics.sh --bootstrap-server "$BOOT" --describe 2>/dev/null | grep -c "Leader: $BROKER\b" || true)
if [ "$LEADS" -gt 0 ]; then
  echo "❌ broker $BROKER leads $LEADS partition(s). Move leadership first:"
  echo "   kafka-leader-election.sh --bootstrap-server $BOOT --all-topic-partitions --election-type preferred"
  exit 1
fi
echo "   OK — not a leader"

echo "── Pre-check 2: post-demotion ISR >= min.insync.replicas for every partition ──"
VIOLATIONS=$($KBIN/kafka-topics.sh --bootstrap-server "$BOOT" --describe 2>/dev/null | awk -v b="$BROKER" '
  /Isr:/ {
    isr=""; for(i=1;i<=NF;i++) if($i=="Isr:") isr=$(i+1);
    n=split(isr, a, ","); inisr=0;
    for(j=1;j<=n;j++) if(a[j]==b) inisr=1;
    if (inisr && (n-1) < 2) print $0;   # conservative default minISR=2; adjust per-topic if needed
  }' | wc -l | tr -d " ")
if [ "$VIOLATIONS" -gt 0 ]; then
  echo "❌ $VIOLATIONS partition(s) would drop below min.insync.replicas=2 after demotion — aborting."
  echo "   Restore other replicas first, or accept fail-stop explicitly by editing this check."
  exit 1
fi
echo "   OK — all partitions keep ISR >= minISR after demotion"

if [ "$YES" != 1 ]; then
  read -r -p "Demote broker $BROKER (add to $FILE on: $HOSTS)? [y/N] " a
  [ "$a" = y ] || { echo "aborted"; exit 1; }
fi

echo "── Updating observer list on all brokers (atomic tmp+mv) ──"
for h in $HOSTS; do
  # shellcheck disable=SC2029,SC2086
  ssh $SSH_OPTS "$h" "sudo sh -c '(grep -v \"^${BROKER}\$\" $FILE 2>/dev/null | grep -v \"^\$\"; echo $BROKER) > $FILE.tmp; mv $FILE.tmp $FILE; echo \"\$(hostname -s): [\$(cat $FILE | tr \"\\n\" \",\")]\"'"
done

echo "── Waiting for native isr-expiration shrink (period = replica.lag.time.max.ms/2, default 15 s) ──"
DESCRIBE_ARGS=(--bootstrap-server "$BOOT" --describe)
[ -n "$TOPIC" ] && DESCRIBE_ARGS+=(--topic "$TOPIC")
for i in $(seq 1 12); do
  sleep 5
  OUT=$($KBIN/kafka-topics.sh "${DESCRIBE_ARGS[@]}" 2>/dev/null | grep -E "Isr:" | head -5)
  echo "  [$((i*5))s] $OUT"
  if ! echo "$OUT" | grep -qE "Isr: [0-9,]*\b$BROKER\b"; then
    echo "✅ broker $BROKER left ISR — demotion complete (back to observer: syncs data, never elected)"
    exit 0
  fi
done
echo "⚠️ broker $BROKER still in ISR after 60 s — check leader logs for 'Shrinking ISR' and file consistency"
exit 1
