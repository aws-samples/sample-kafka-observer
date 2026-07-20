#!/usr/bin/env bash
# =============================================================================
# observer-auto-promoter.sh — optional auto-promotion daemon (under-min-isr)
#
# POLICY (Confluent parity): observerPromotionPolicy=under-min-isr
#   * When a partition's ISR size drops below min.insync.replicas AND a
#     caught-up observer replica exists  -> promote the observer into ISR.
#   * When the original followers recover (ISR minus the observer would again
#     satisfy min.insync.replicas on every partition) -> demote the observer
#     back to observer status.
#
# PHILOSOPHY (see docs/auto-promotion.md):
#   * NOT in the Kafka kernel. This is an external watchdog that drives the
#     exact same atomic-file mechanism as the manual runbooks
#     (scripts/observer-promote.sh / observer-demote.sh). It can be killed,
#     dry-run, audited, and upgraded independently of the brokers.
#   * DEFAULT OFF. Refuses to act without the explicit -e flag; the systemd
#     unit template (deploy/observer-auto-promoter.service) is shipped
#     disabled. Financial deployments are advised to stay manual.
#   * EVERY decision is written to an append-only audit log.
#
# Usage:
#   observer-auto-promoter.sh -e -s <bootstrap> -H "host1 host2 host3" \
#       [-f /opt/kafka/observer.ids] [-t topic1,topic2] [-i 10] [-m 2] \
#       [-l 0] [-c 300] [-L /var/log/observer-promoter.log] \
#       [-S /var/lib/observer-promoter] [-n] [-1]
#
#     -e  ENABLE the policy (safety interlock; without it: print status, exit 0)
#     -s  bootstrap servers host:port                                (required)
#     -H  space-separated list of ALL broker hosts (ssh-reachable)   (required)
#     -f  observer ids file path on brokers    (default /opt/kafka/observer.ids)
#     -t  comma-separated topic allowlist      (default: ALL topics)
#     -i  scan interval seconds                (default 10)
#     -m  fallback min.insync.replicas when topic/broker config unreadable (default 2)
#     -l  max offsetLag to consider an observer "caught up"          (default 0)
#     -c  per-broker action cooldown seconds (anti-flapping)         (default 300)
#     -L  audit log file                  (default /var/log/observer-promoter.log)
#     -S  state dir (persists auto-promoted set across restarts)
#                                         (default /var/lib/observer-promoter)
#     -n  dry-run: detect + log decisions, never touch the cluster
#     -1  single scan then exit (for testing / cron-style operation)
#
# Env: KAFKA_BIN (default /opt/kafka/bin), SSH_OPTS
# =============================================================================
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

ENABLE=0; BOOT=""; HOSTS=""; FILE=/opt/kafka/observer.ids
TOPICS=""; INTERVAL=10; MIN_ISR_DEFAULT=2; MAX_LAG=0; COOLDOWN=300
LOG=/var/log/observer-promoter.log; STATE_DIR=/var/lib/observer-promoter
DRY_RUN=0; ONESHOT=0

while getopts "es:H:f:t:i:m:l:c:L:S:n1" o; do case $o in
  e) ENABLE=1;;      s) BOOT=$OPTARG;;   H) HOSTS=$OPTARG;;
  f) FILE=$OPTARG;;  t) TOPICS=$OPTARG;; i) INTERVAL=$OPTARG;;
  m) MIN_ISR_DEFAULT=$OPTARG;;           l) MAX_LAG=$OPTARG;;
  c) COOLDOWN=$OPTARG;; L) LOG=$OPTARG;; S) STATE_DIR=$OPTARG;;
  n) DRY_RUN=1;;     1) ONESHOT=1;;
  *) echo "Usage: $0 -e -s <bootstrap> -H \"hosts\" [-f file] [-t topics] [-i sec] [-m minisr] [-l lag] [-c sec] [-L log] [-S dir] [-n] [-1]" >&2; exit 2;;
esac; done

if [ "$ENABLE" != 1 ]; then
  echo "observer auto-promotion policy: OFF (default)."
  echo "This daemon only acts when started with -e. For financial workloads the"
  echo "recommended posture is manual operation via scripts/observer-promote.sh."
  exit 0
fi
: "${BOOT:?-s bootstrap required}" "${HOSTS:?-H hosts required}"

KBIN="${KAFKA_BIN:-/opt/kafka/bin}"
SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=no -o ConnectTimeout=10}"
FIRST_HOST=${HOSTS%% *}
STATE_FILE="$STATE_DIR/auto-promoted.list"   # broker ids this daemon promoted

# ---------------------------------------------------------------- audit log --
mkdir -p "$STATE_DIR" 2>/dev/null || { echo "cannot create state dir $STATE_DIR (use -S)" >&2; exit 1; }
touch "$STATE_FILE"
if ! touch "$LOG" 2>/dev/null; then
  echo "cannot write audit log $LOG (use -L). Auditability is mandatory — refusing to run." >&2
  exit 1
fi
audit() { printf '%s | %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*" | tee -a "$LOG"; }

trap 'audit "SHUTDOWN | signal received, exiting cleanly"; exit 0' INT TERM

# ------------------------------------------------------------------ helpers --
csv_count() { if [ -z "$1" ] || [ "$1" = "-" ]; then echo 0; else awk -F, '{print NF}' <<<"$1"; fi; }
in_csv()    { [[ ",$2," == *",$1,"* ]]; }

# Current observer ids (read from the first broker's file; single source of truth)
observer_ids() {
  # shellcheck disable=SC2086,SC2029  # SSH_OPTS must word-split; $FILE expands client-side by design
  ssh $SSH_OPTS "$FIRST_HOST" "cat $FILE 2>/dev/null" 2>/dev/null \
    | grep -E '^[0-9]+$' | paste -sd, - || true
}

declare -A MIN_ISR_CACHE=()
min_isr_of() {  # $1=topic — cached per scan
  local t=$1 v
  if [ -n "${MIN_ISR_CACHE[$t]:-}" ]; then echo "${MIN_ISR_CACHE[$t]}"; return; fi
  v=$("$KBIN/kafka-configs.sh" --bootstrap-server "$BOOT" --describe --topic "$t" --all 2>/dev/null \
      | grep -o 'min\.insync\.replicas=[0-9]*' | head -1 | cut -d= -f2) || true
  v=${v:-$MIN_ISR_DEFAULT}
  MIN_ISR_CACHE[$t]=$v
  echo "$v"
}

# offsetLag of a replica on a broker, via kafka-log-dirs (-1 = unknown)
replica_lag() {  # $1=broker $2=topic $3=partition
  local json lag
  json=$("$KBIN/kafka-log-dirs.sh" --bootstrap-server "$BOOT" --describe \
         --broker-list "$1" --topic-list "$2" 2>/dev/null | grep '^{') || { echo -1; return; }
  lag=$(grep -o "\"partition\":\"$2-$3\"[^}]*" <<<"$json" \
        | grep -o '"offsetLag":-\{0,1\}[0-9]*' | head -1 | cut -d: -f2)
  echo "${lag:--1}"
}

# Normalized partition table: one line per partition -> "topic part leader replicas isr"
describe_partitions() {
  local args=(--bootstrap-server "$BOOT" --describe)
  local out=""
  if [ -n "$TOPICS" ]; then
    local t
    for t in ${TOPICS//,/ }; do
      out+=$("$KBIN/kafka-topics.sh" "${args[@]}" --topic "$t" 2>/dev/null || true)$'\n'
    done
  else
    out=$("$KBIN/kafka-topics.sh" "${args[@]}" 2>/dev/null || true)
  fi
  awk '/Partition:/ {
    t=p=l=r=is="-"
    for (i=1;i<=NF;i++) {
      if      ($i=="Topic:")     t=$(i+1)
      else if ($i=="Partition:") p=$(i+1)
      else if ($i=="Leader:")    l=$(i+1)
      else if ($i=="Replicas:")  r=$(i+1)
      else if ($i=="Isr:")       is=$(i+1)
    }
    if (is !~ /^[0-9][0-9,]*$/) is="-"   # empty ISR renders as next token (Elr:)
    if (r  !~ /^[0-9][0-9,]*$/) r="-"
    print t, p, l, r, is
  }' <<<"$out"
}

declare -A LAST_ACTION=()   # broker -> epoch of last promote/demote (anti-flap)
cooldown_ok() {
  local last=${LAST_ACTION[$1]:-0} now; now=$(date +%s)
  [ $((now - last)) -ge "$COOLDOWN" ]
}

state_add()    { grep -qx "$1" "$STATE_FILE" || echo "$1" >> "$STATE_FILE"; }
state_remove() { grep -vx "$1" "$STATE_FILE" > "$STATE_FILE.tmp" || true; mv "$STATE_FILE.tmp" "$STATE_FILE"; }
state_list()   { cat "$STATE_FILE"; }

# ------------------------------------------------------------------ actions --
do_promote() {  # $1=broker $2=reason
  if [ "$DRY_RUN" = 1 ]; then
    audit "PROMOTE-DRYRUN | broker=$1 | $2 | no action taken"
    return 0
  fi
  audit "PROMOTE-BEGIN | broker=$1 | $2"
  if "$SCRIPT_DIR/observer-promote.sh" -b "$1" -s "$BOOT" -H "$HOSTS" -f "$FILE" -y >>"$LOG" 2>&1; then
    state_add "$1"; LAST_ACTION[$1]=$(date +%s)
    audit "PROMOTE-OK | broker=$1 | now a full ISR/election candidate"
  else
    LAST_ACTION[$1]=$(date +%s)   # cooldown applies to failures too
    audit "PROMOTE-FAIL | broker=$1 | observer-promote.sh exited non-zero, see log above"
    return 1
  fi
}

do_demote() {  # $1=broker $2=reason
  if [ "$DRY_RUN" = 1 ]; then
    audit "DEMOTE-DRYRUN | broker=$1 | $2 | no action taken"
    return 0
  fi
  audit "DEMOTE-BEGIN | broker=$1 | $2"
  if "$SCRIPT_DIR/observer-demote.sh" -b "$1" -s "$BOOT" -H "$HOSTS" -f "$FILE" -y >>"$LOG" 2>&1; then
    state_remove "$1"; LAST_ACTION[$1]=$(date +%s)
    audit "DEMOTE-OK | broker=$1 | back to observer status"
  else
    LAST_ACTION[$1]=$(date +%s)
    audit "DEMOTE-FAIL | broker=$1 | observer-demote.sh exited non-zero (pre-check or shrink timeout), will retry after cooldown"
    return 1
  fi
}

# demotion is safe for broker b iff EVERY partition where b is in ISR keeps
# ISR-{b} >= minISR  (i.e. the original followers have genuinely recovered)
demotion_safe() {  # $1=broker $2=partition_table  -> 0 safe / 1 not
  local b=$1 t p l r is n
  while read -r t p l r is; do
    [ "$is" = "-" ] && continue
    in_csv "$b" "$is" || continue
    n=$(csv_count "$is")
    if [ $((n - 1)) -lt "$(min_isr_of "$t")" ]; then return 1; fi
  done <<<"$2"
  return 0
}

leads_any() {  # $1=broker $2=partition_table
  local b=$1 t p l r is
  while read -r t p l r is; do
    [ "$l" = "$b" ] && return 0
  done <<<"$2"
  return 1
}

# --------------------------------------------------------------------- scan --
scan_once() {
  MIN_ISR_CACHE=()
  local observers table t p l r is n min b acted=0
  observers=$(observer_ids)
  table=$(describe_partitions)
  [ -z "$table" ] && { audit "SCAN-WARN | describe returned no partitions (cluster unreachable?)"; return 0; }

  # ---- phase 1: under-min-isr detection -> promote one caught-up observer --
  while read -r t p l r is; do
    [ "$acted" = 1 ] && break                       # max one action per scan
    n=$(csv_count "$is")
    min=$(min_isr_of "$t")
    [ "$n" -ge "$min" ] && continue
    audit "DETECT | under-min-isr | topic=$t partition=$p leader=$l replicas=$r isr=$is (size=$n < minISR=$min)"
    # candidate observers: in Replicas, not in ISR, listed in observer.ids
    for b in ${r//,/ }; do
      [ "$r" = "-" ] && break
      in_csv "$b" "$observers" || continue
      [ "$is" != "-" ] && in_csv "$b" "$is" && continue
      if ! cooldown_ok "$b"; then
        audit "SKIP | broker=$b in cooldown (${COOLDOWN}s anti-flap window)"; continue
      fi
      local lag; lag=$(replica_lag "$b" "$t" "$p")
      if [ "$lag" -ge 0 ] && [ "$lag" -le "$MAX_LAG" ]; then
        do_promote "$b" "topic=$t partition=$p isr=$is minISR=$min observerLag=$lag" || true
        acted=1; break
      else
        audit "SKIP | broker=$b not caught up (offsetLag=$lag > $MAX_LAG) — promoting a laggy observer would stall the HW"
      fi
    done
  done <<<"$table"
  [ "$acted" = 1 ] && return 0

  # ---- phase 2: recovery detection -> demote auto-promoted observers ------
  for b in $(state_list); do
    cooldown_ok "$b" || continue
    demotion_safe "$b" "$table" || continue
    # double-check: re-describe after a short delay; condition must still hold
    sleep 5
    local table2; table2=$(describe_partitions)
    demotion_safe "$b" "$table2" || { audit "SKIP | broker=$b recovery not stable across double-check, deferring demotion"; continue; }
    if leads_any "$b" "$table2"; then
      audit "DEMOTE-PREP | broker=$b currently leads partitions — running preferred leader election first"
      if [ "$DRY_RUN" != 1 ]; then
        "$KBIN/kafka-leader-election.sh" --bootstrap-server "$BOOT" \
          --all-topic-partitions --election-type preferred >>"$LOG" 2>&1 || true
        sleep 5
        table2=$(describe_partitions)
        if leads_any "$b" "$table2"; then
          audit "SKIP | broker=$b still leader after preferred election, deferring demotion"; continue
        fi
      fi
    fi
    do_demote "$b" "original followers recovered; ISR-{$b} >= minISR on all partitions" || true
    break                                            # max one action per scan
  done
}

# --------------------------------------------------------------------- main --
audit "START | policy=under-min-isr enabled=1 dry_run=$DRY_RUN interval=${INTERVAL}s cooldown=${COOLDOWN}s max_lag=$MAX_LAG topics=${TOPICS:-ALL} bootstrap=$BOOT hosts=[$HOSTS] file=$FILE"
[ "$DRY_RUN" = 1 ] && audit "MODE | DRY-RUN: decisions are logged, cluster is never modified"

if [ "$ONESHOT" = 1 ]; then scan_once; audit "STOP | single scan complete"; exit 0; fi
while true; do
  scan_once || audit "SCAN-ERROR | scan_once returned non-zero (transient?); continuing"
  sleep "$INTERVAL"
done
