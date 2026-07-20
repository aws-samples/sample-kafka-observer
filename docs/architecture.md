# Architecture — how ~60 lines give Kafka a third replica state

## The bookkeeping-team analogy

Think of a Kafka partition as a bookkeeping team:

- **Leader** = the head bookkeeper; every new entry is written there first.
- **ISR followers** = full team members. For every entry, the head must wait until all full members have copied it before declaring it "confirmed" (`acks=all`). The confirmation line (**high-watermark, HW**) is set by the *slowest full member*. When the head fails, the new head can only be chosen from full members.

Now you want a copyist in a distant office (a slow AZ) as a backup. If it is a *full member*, every entry waits for it — the whole team slows down. That is exactly what "a cross-AZ replica drags the HW" means.

**Our change is one thing**: allow an *auditor* status (**observer**) — copies every entry, but is not a full member:

- Copies **all** the data (full replication via the native fetch protocol)
- The confirmation line **never waits for it** (measured: acks=all stays at fast-pair latency, 2.04–2.35 ms, with the observer in the slowest AZ)
- Never eligible to become head (excluded from all election paths)

## Why "not in ISR" implies everything else

Kafka's own rules do the derivation for us:

```
not in ISR
 ├─→ HW = min(LEO of ISR members)      → HW never waits for it  → no latency drag
 ├─→ acks=all waits for ISR members    → producers never wait for it
 ├─→ minISR counts ISR members         → it can't mask a real availability loss
 └─→ leader candidates come from ISR   → it can never be elected
```

We invented **no new mechanism**. We installed gates on existing conveyor belts.

## The 5 hook points (Kafka 3.7.1, ZooKeeper mode)

| # | Location | Belt we gate | Purpose |
|---|---|---|---|
| 1 | `Partition.canAddReplicaToIsr` | Every follower fetch → leader asks "may this replica join ISR?" (`maybeExpandIsr`) | **The core gate.** Observer id in list → `return false` → never joins ISR |
| 2 | `Partition.getOutOfSyncReplicas` | Leader's periodic `isr-expiration` task (every `replica.lag.time.max.ms/2`) | **Demotion hook** (v0.3): an in-ISR observer is reported as lagging → native shrink flow ejects it. Net +1 line of logic |
| 3 | `Partition.maybeIncrementLeaderHW` | HW advancement | Closes a subtle gap: HW advancement waits for replicas that are *caught-up and eligible* via `isReplicaIsrEligible` — a different function from #1. Without this gate an observer inside the 30 s lag window could theoretically stall HW. With it, "never drags HW" is structural, not just empirical |
| 4 | `PartitionStateMachine.initializeLeaderAndIsrForPartitions` | Topic creation (controller side) | v0.1 lesson: initial ISR construction bypasses `maybeExpandIsr` entirely — the controller stuffs all live replicas into the initial ISR. Filter observers here, pick leader from non-observers |
| 5 | `PartitionStateMachine.offlinePartitionLeaderElection` (unclean branch) | Last-resort election | Even with `unclean.leader.election.enable=true` and all ISR members dead, never elect an observer. Verified: kill all ISR → `Leader: none`. Electing an un-promotable leader would deadlock the partition |

## Dynamic identity: the file

`kafka.observer.ObserverIds` (new, self-contained object in its own package — call sites use the fully-qualified name, so no import-block churn):

- Source: `/opt/kafka/observer.ids` (override via env `KAFKA_OBSERVER_IDS_FILE`); one id per line or comma-separated; `#` comments allowed
- Fallback: env `KAFKA_OBSERVER_BROKER_IDS` (compatibility with v0.1/v0.2 deployments)
- **5-second TTL cache** (`System.nanoTime`-based; `@volatile`, lock-free read) — hook #1 sits on the fetch hot path and must not hit disk per call
- **Fail-safe**: unreadable/corrupt file → keep last cached value + WARN; the broker never fails to start because of this file

## Promotion & demotion — reusing native conveyor belts

**Promotion** (observer → electable): delete the id from the file. Within 5 s (cache TTL) + one fetch round-trip (≤500 ms under traffic), the gate at #1 opens, the native `maybeExpandIsr → AlterPartition` flow adds it to ISR, and it automatically regains election eligibility. **Measured: ≤10 s, zero restart, zero data movement** — its log was byte-identical all along.

**Demotion** (electable → observer): add the id back. The periodic `isr-expiration` task sees it as out-of-sync via hook #2 and runs the standard shrink flow (log lines, AlterPartition, HW recomputation). **Measured: ≤10–20 s.** The native path never shrinks the leader itself — a safety property; move leadership before demoting a leader.

## EOS preservation — why it is free

Follower replication calls `appendAsFollower` with `validateAndAssignOffsets=false` — the source comment reads *"we are taking the offsets we are given"*. The entire `LogValidator` (offset assignment, dedup) runs **only on the leader path**. RecordBatch headers — `producerId`, `producerEpoch`, `baseSequence`, transactional COMMIT/ABORT markers — travel inside the copied bytes; producer state and LSO are rebuilt deterministically on every replica.

**Verified on real machines:** per-batch CRC identical across leader and observer (5 001 batches); `read_committed` view identical (3 committed txns visible, 1 aborted txn invisible on both). Control group: MirrorMaker 2 under a `kill -9` during the offset-flush window re-produced **20 000 duplicates** (target PID=-1 — no idempotence, no dedup basis). Replication-of-the-log vs consume-then-reproduce is a structural difference, not a tuning difference. Details: [eos-semantics.md](eos-semantics.md), raw evidence in [`evidence/`](../evidence/).

## Known behavior notes

- **ZK mode, new topics** (Docker-demo verified, stronger than initially documented): the controller sends `LeaderAndIsr` only to ISR members at topic creation. Because the patch excludes observers from the initial ISR, **even a running observer never receives the new topic's assignment** — the partition directory does not appear on disk and a naive promotion would fail. The observer learns the assignment only on its next restart or a controller failover. Existing topics are unaffected (assignments load from ZK at startup). Operational rule: **after creating topics that span an observer, restart the observer broker once** (scripted in `docker/demo.sh` Step 2b). KRaft mode does not have this limitation — brokers learn all assignments from the metadata log (probe-verified, `evidence/kraft_probe_evidence.md`).
- The observer list must be consistent across brokers **and** the controller host; push it with one script and verify checksums. The inconsistency window is bounded (rollout + 5 s) and fails toward the safe side for promotion (controller still treats it as observer → still excluded from unclean election).
