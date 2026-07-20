# Timing analysis & automation design — the two most-asked questions

## Part 1: What is the actual downtime in each failure scenario?

Every number below was measured on real EC2 instances (Tokyo, Kafka 3.7.1/4.0, KRaft mode, `replica.lag.time.max.ms=10000`). See raw output in `evidence/scenario_matrix_evidence.md`.

### Failure-to-recovery timing summary

| Scenario | Write interruption | Recovery action | Recovery time | Data impact |
|---|---|---|---|---|
| **S1 — leader broker crash** | Writes stop until new leader elected | Automatic (Kafka native ISR election) | **~10.4 s** (dominated by `replica.lag.time.max.ms`) | Zero loss: new leader was in ISR |
| **S2 — ISR follower crash** | **None** (ISR shrinks but stays ≥ minISR) | Automatic (follower restarts, catches up, rejoins ISR) | Rejoin: **3.9 s** from restart | Zero loss |
| **S3 — observer crash** | **None** (observer is not in ISR) | Automatic (observer restarts, catches up) | Catch-up: verifies byte-identical after reconnect | Zero loss, zero write impact |
| **S4 — dual primary loss (fast-pair AZ gone)** | Writes stop immediately (ISR < minISR) | **Manual**: promote observer by editing `observer.ids` | Promotion **~4 s** + election **~5 s** = **~9 s** from operator action | Zero loss (observer was byte-identical) |
| **S4b — same, with auto-promoter enabled** | Writes stop; auto-promoter detects in ≤ scan interval (10 s) + promotes | Automatic (daemon) | Detection **≤10 s** + promotion **~4 s** = **≤14 s** total | Zero loss |
| **S5 — promote a lagging observer** | None (this is proactive promotion) | Operator promotes; observer must catch up before ISR admission | Catch-up: **5.4 s for 30K records** (15 MB); ISR admission only after LEO matches leader | HW does not regress; reads safe throughout |
| **S6 — file inconsistency (partial update)** | None (controller rejects with `INELIGIBLE_REPLICA`) | Self-heal when files synchronized | **5.8 s** after alignment | Zero: fail-safe direction |
| **S7 — observer.ids corrupted/unreadable** | None | Broker keeps last cached value + WARN log | Immediate (cache already in memory) | Zero: broker never crashes for config file |
| **S8 — controller failover** | Writes pause briefly (~3.7 s for new controller election) | Automatic (Kafka native) | **3.7 s** | Zero: new controller correctly filters observer |

### Key timing relationships

```
replica.lag.time.max.ms = 10000 (default 30000)
├── Determines: how long before a dead follower is declared out-of-sync
├── Appears in: S1 failover time (~10.4 s ≈ this value + election latency)
├── Appears in: S4 demotion trigger (isr-expiration period = value/2 = 5 s)
└── Tuning: lower = faster failover BUT higher risk of false ISR kicks on cross-AZ jitter
    → Our recommendation: 10000-30000 ms depending on network stability

observer.ids cache TTL = 5 s (hardcoded in ObserverIds.scala)
├── Appears in: promotion latency (file edit → effective: ≤5 s)
├── Appears in: demotion latency (file edit → effective: ≤5 s + next isr-expiration cycle)
└── Tuning: not currently configurable; 5 s balances hot-path performance vs responsiveness

auto-promoter scan interval = 10 s (configurable: -i flag)
├── Appears in: S4b total recovery time (scan + promotion)
├── Tuning: lower = faster detection, higher CLI/describe load
└── Recommended: 10 s for most workloads; 5 s for ultra-low-RTO requirements
```

### What "write interruption" actually looks like to the application

During fail-stop (S1/S4):
- `acks=all` producers receive `NOT_ENOUGH_REPLICAS` retriable exception
- With `retries=MAX_VALUE` + `delivery.timeout.ms=120000` (recommended): producer retries transparently; after recovery, all buffered messages land — **the application may not even notice** if total outage < delivery.timeout
- `acks=1` producers continue writing to any surviving leader (but lose the minISR durability guarantee)
- Consumers: continue reading committed data (HW doesn't regress); new data stalls until a leader can acknowledge

---

## Part 2: How does observer enter/leave ISR — automatic or manual?

### The core mechanism (always present, in every deployment)

Observer promotion and demotion work by **editing a file**. The file change is the **only** human/automation action needed — everything else is Kafka's native ISR machinery:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ PROMOTION: observer → electable ISR member                              │
│                                                                         │
│ 1. Edit file: remove broker id from /opt/kafka/observer.ids             │
│    (on ALL brokers + controller nodes, atomically: write tmp → mv)      │
│                                                                         │
│ 2. ≤5 s: ObserverIds cache refreshes (5s TTL, System.nanoTime-based)    │
│                                                                         │
│ 3. Next follower fetch (continuous under traffic, ≤500ms idle):         │
│    leader calls maybeExpandIsr() → canAddReplicaToIsr() → gate OPEN     │
│                                                                         │
│ 4. Kafka native: AlterPartition request → controller adds to ISR        │
│                                                                         │
│ 5. Done: observer is now a full ISR member, can be elected leader       │
│                                                                         │
│ Total measured time: ≤10 s (ZK), ~4 s (KRaft)                          │
│ Zero restart. Zero data movement. Data was byte-identical all along.    │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│ DEMOTION: ISR member → observer                                         │
│                                                                         │
│ 1. Edit file: add broker id back to /opt/kafka/observer.ids             │
│    (on ALL nodes, same atomic pattern)                                  │
│                                                                         │
│ 2. ≤5 s: cache refreshes                                               │
│                                                                         │
│ 3. Leader's periodic isr-expiration task (every replica.lag.time.max.ms │
│    / 2, default 5–15 s) runs getOutOfSyncReplicas():                    │
│    our hook reports the observer as "out of sync" → native shrink path  │
│                                                                         │
│ 4. Kafka native: AlterPartition request → controller removes from ISR   │
│                                                                         │
│ 5. Done: broker continues syncing data but is back to observer status   │
│                                                                         │
│ Total measured time: ~9–12 s (KRaft), ≤20 s (ZK)                       │
│ Zero restart. Zero data loss. Replication continues uninterrupted.      │
│                                                                         │
│ ⚠️ EXCEPTION: if the observer is currently the LEADER, demotion does    │
│ NOT take effect hot (leader never self-removes from ISR). You must      │
│ move leadership first: kafka-leader-election.sh --election-type         │
│ preferred, or restart that one broker.                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Three operational modes (you choose)

| Mode | Who edits the file | When to use | Trade-off |
|---|---|---|---|
| **Manual** (recommended for financial) | Human operator via `scripts/observer-promote.sh` / `observer-demote.sh` (with pre-checks) | When every ISR state change must be a deliberate human decision | Slower RTO (human reaction time), maximum determinism |
| **Auto-promoter daemon** | `scripts/observer-auto-promoter.sh` (external watchdog) | When sub-minute RTO matters more than manual control | Adds an operational component; every decision is audit-logged; can be killed at any time |
| **Hybrid** (recommended for most) | Daemon in dry-run mode (`-n`) → detects + alerts → human confirms → daemon executes | Benefit of automated detection + human in the loop for the action | Best of both: fast detection, deliberate execution |

### Why the auto-promoter is an EXTERNAL tool, not in-kernel

| Dimension | External (our design) | In-kernel (hypothetical) |
|---|---|---|
| **Auditable** | Plain text log file; every decision reconstructable | Would need a new internal audit system |
| **Killable** | `systemctl stop` / `kill` — instant, guaranteed | Can't stop kernel logic without broker restart |
| **Testable** | `--dry-run` mode; single-scan mode (`-1`) for CI | Would need test hooks inside Kafka |
| **Upgrade-decoupled** | Script version independent of Kafka version | Tied to Kafka upgrade cycle |
| **Fault domain** | If daemon crashes, cluster continues fine (manual fallback) | Bug in auto-logic could affect all partitions |
| **Policy iteration** | Edit a shell script, restart daemon | Requires new patch, recompile, rolling restart |

This is a deliberate architectural choice — **the blast radius of a bug in a shell script is zero partitions; the blast radius of a bug in a Kafka controller hook is all partitions**. For a feature whose purpose is handling failures, this separation matters.

### The auto-promoter's decision loop (simplified)

```
every 10 seconds:
  for each topic:
    ISR_size = describe topic → parse Isr field → count
    minISR = topic config or broker default
    
    if ISR_size < minISR:
      for each observer broker (from observer.ids):
        lag = kafka-log-dirs → offsetLag for that broker
        if lag <= threshold (default 0):
          → PROMOTE (edit observer.ids to remove this id)
          → log: "PROMOTE broker 3 because topic X partition 0 ISR=1 < minISR=2, lag=0"
          → cooldown 300s before next action on this broker
    
    if ISR_size >= minISR AND this broker was auto-promoted:
      if ISR without this broker still >= minISR (on ALL its partitions):
        → DEMOTE (add id back to observer.ids)
        → log: "DEMOTE broker 3 because ISR recovered to {1,2,3}, {1,2} suffices"
```

Key safety rules:
- **Only demotes what it promoted** (ownership tracking in persistent file)
- **Never promotes a lagging observer** (lag > threshold → SKIP + log)
- **Never demotes if it would trigger fail-stop** (checks ALL partitions of that broker)
- **Cooldown** between actions (default 5 min per broker, anti-flap)
- **One action per scan** (prevents cascading decisions on incomplete information)

### Best practice timeline for a production deployment

```
Day 1:  Deploy patched Kafka + observer.ids (manual mode)
        → verify ISR behavior matches expectations
        → run promote/demote manually a few times with runbook

Week 1: Deploy auto-promoter in DRY-RUN mode (-n)
        → observe audit log: would it have promoted at the right moments?
        → tune scan interval, lag threshold, cooldown

Week 2: Enable auto-promoter in REAL mode (-e) but NOT systemd-enabled
        → manually start/stop around maintenance windows
        → verify full cycle: fail → auto-promote → recover → auto-demote

Week 3+: systemctl enable (if desired) — now it's a standing policy
         → or keep it as an assisted tool: it detects and alerts, human confirms
```

This graduated approach is documented in `docs/auto-promotion.md` and `deploy/observer-auto-promoter.service`.
