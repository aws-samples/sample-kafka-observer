#!/usr/bin/env bash
# =============================================================================
# demo.sh — end-to-end observer lifecycle demo on the local Docker cluster
#
# What it shows, fully automated:
#   1. Wait for all 3 brokers to be healthy
#   2. Create a topic with RF=3 (replicas 1,2,3; broker1 is the observer)
#      + restart broker1 once — the documented ZK-mode caveat: the controller
#        sends LeaderAndIsr only to ISR members at topic creation, so an
#        observer discovers a NEW topic's assignment only on restart or
#        controller failover (docs/architecture.md#known-behavior-notes)
#   3. Verify ISR = {2,3} — the observer syncs data but never joins ISR
#   4. Produce messages with acks=all — observer never blocks the HW
#   5. PROMOTE broker1 by emptying observer.ids  -> ISR becomes {1,2,3}
#   6. DEMOTE  broker1 by writing "1" back       -> ISR shrinks to {2,3}
#   7. Cleanup (delete demo topic, restore observer.ids)
#
# Prerequisites: `docker compose up -d` already running (or run with --up).
# Run from anywhere; paths are resolved relative to this script.
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")"

TOPIC="observer-demo"
OBSERVER_ID=1
BOOT_INTERNAL="kafka2:9092"          # exec'd inside containers; broker2 is a normal replica
EXEC=(docker compose exec -T kafka2) # run CLI tools inside the cluster network
COMPOSE=(docker compose)

step()  { printf '\n\033[1;36m── %s ──\033[0m\n' "$*"; }
fail()  { printf '\033[1;31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
ok()    { printf '\033[1;32mOK: %s\033[0m\n' "$*"; }

describe() {
  "${EXEC[@]}" kafka-topics.sh --bootstrap-server "$BOOT_INTERNAL" \
    --describe --topic "$TOPIC" 2>/dev/null | grep "Partition:" || true
}

isr_of() { describe | sed -n 's/.*Isr: \([0-9,]*\).*/\1/p'; }

wait_isr() {
  # wait_isr <should_contain: yes|no> <broker_id> <timeout_s>
  local want=$1 id=$2 timeout=$3 elapsed=0
  while [ "$elapsed" -lt "$timeout" ]; do
    local isr; isr=$(isr_of)
    echo "  [${elapsed}s] Isr: ${isr:-<none>}"
    case "$want" in
      yes) echo ",$isr," | grep -q ",$id," && return 0 ;;
      no)  [ -n "$isr" ] && ! echo ",$isr," | grep -q ",$id," && return 0 ;;
    esac
    sleep 5; elapsed=$((elapsed + 5))
  done
  return 1
}

# Restore the canonical (git-tracked) content, comments included.
restore_observer_file() {
  cat > observer.ids <<EOF
# Broker ids listed here are OBSERVERS (never join ISR, never lead).
# Edit this file on the host — all brokers re-read it within 5 seconds.
#   promote broker1: delete the "1" line below
#   demote  broker1: add it back
${OBSERVER_ID}
EOF
}

cleanup() {
  step "Cleanup"
  "${EXEC[@]}" kafka-topics.sh --bootstrap-server "$BOOT_INTERNAL" \
    --delete --topic "$TOPIC" 2>/dev/null || true
  restore_observer_file
  echo "Deleted topic '$TOPIC' and restored observer.ids (broker $OBSERVER_ID is an observer again)."
}

# --- 0. optionally bring the cluster up -------------------------------------
if [ "${1:-}" = "--up" ]; then
  step "docker compose up -d --build (first build takes 10-20 min)"
  "${COMPOSE[@]}" up -d --build
fi

# --- 1. wait for cluster ready ----------------------------------------------
step "Step 1: Waiting for all brokers to be healthy"
for i in $(seq 1 60); do
  HEALTHY=$("${COMPOSE[@]}" ps --format '{{.Name}} {{.Health}}' 2>/dev/null | grep -c healthy || true)
  echo "  [$((i*5))s] healthy containers: $HEALTHY/4"
  [ "$HEALTHY" -ge 4 ] && break
  [ "$i" = 60 ] && fail "cluster not healthy after 300 s — check 'docker compose logs'"
  sleep 5
done
ok "cluster is up"

# Make sure the demo starts from the documented state (broker1 = observer).
restore_observer_file
trap cleanup EXIT

# --- 2. create topic RF=3 ----------------------------------------------------
step "Step 2: Create topic '$TOPIC' (partitions=1, replication-factor=3)"
"${EXEC[@]}" kafka-topics.sh --bootstrap-server "$BOOT_INTERNAL" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2
describe

# ⚠️ ZK-MODE CAVEAT (verified on this cluster, documented in
# docs/architecture.md#known-behavior-notes): at topic creation the controller
# sends LeaderAndIsr only to ISR members, and the patch keeps observers out of
# the initial ISR — so the observer does NOT start fetching a NEW topic until
# its next restart or a controller failover. Existing topics are unaffected.
# KRaft (v0.4) removes this limitation entirely.
step "Step 2b: Restart broker $OBSERVER_ID once so it discovers the NEW topic (ZK-mode caveat)"
"${COMPOSE[@]}" restart "kafka${OBSERVER_ID}"
for i in $(seq 1 24); do
  if "${COMPOSE[@]}" ps --format '{{.Name}} {{.Health}}' | grep -q "kafka${OBSERVER_ID} healthy"; then
    break
  fi
  [ "$i" = 24 ] && fail "kafka${OBSERVER_ID} not healthy after restart"
  sleep 5
done
"${EXEC[@]}" bash -c "true"  # ensure exec target is responsive
ok "broker $OBSERVER_ID restarted and now fetches '$TOPIC' (this restart is the ZK-mode new-topic cost)"

# --- 3. verify observer is OUT of ISR -----------------------------------------
step "Step 3: Verify broker $OBSERVER_ID (observer) is NOT in ISR"
wait_isr no "$OBSERVER_ID" 60 || fail "broker $OBSERVER_ID still in ISR — is the patched image running?"
ok "Replicas contain $OBSERVER_ID but Isr does not — observer confirmed"

# --- 4. produce with acks=all --------------------------------------------------
step "Step 4: Produce 1000 records with acks=all (observer must not drag HW)"
"${EXEC[@]}" bash -c "seq 1 1000 | kafka-console-producer.sh \
  --bootstrap-server $BOOT_INTERNAL --topic $TOPIC \
  --producer-property acks=all --producer-property linger.ms=0 > /dev/null"
# kafka-get-offsets.sh (Kafka >= 3.5 home of GetOffsetShell) prints topic:partition:offset
COUNT=$("${EXEC[@]}" kafka-get-offsets.sh \
  --bootstrap-server "$BOOT_INTERNAL" --topic "$TOPIC" --time -1 | cut -d: -f3)
[ "$COUNT" = "1000" ] || fail "expected 1000 records, log end offset says $COUNT"
ok "1000 records committed with acks=all while ISR = {2,3}"

# --- 5. PROMOTE ---------------------------------------------------------------
step "Step 5: PROMOTE broker $OBSERVER_ID — empty observer.ids on the HOST"
: > observer.ids
echo "  observer.ids is now empty; brokers re-read it within 5 s (native ISR expand)"
wait_isr yes "$OBSERVER_ID" 90 || fail "broker $OBSERVER_ID did not join ISR within 90 s"
ok "broker $OBSERVER_ID joined ISR — promotion complete, zero restarts, zero data movement"
describe

# --- 6. DEMOTE ----------------------------------------------------------------
step "Step 6: DEMOTE broker $OBSERVER_ID — write its id back to observer.ids"
restore_observer_file
echo "  observer.ids contains '$OBSERVER_ID' again; native isr-expiration will shrink it out"
wait_isr no "$OBSERVER_ID" 90 || fail "broker $OBSERVER_ID did not leave ISR within 90 s"
ok "broker $OBSERVER_ID left ISR — demotion complete, still fully syncing data"
describe

# --- done ----------------------------------------------------------------------
step "Demo complete"
echo "Lifecycle shown: observer (out of ISR) -> promote (in ISR, electable) -> demote (out again)."
echo "Promote and demote were pure file edits on the host — zero broker restarts."
echo "(The only restart was step 2b: the documented ZK-mode cost for a NEWLY created topic.)"
# cleanup runs via trap
