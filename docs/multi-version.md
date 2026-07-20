# Multi-version & dual-mode support

**Decision: support both ZooKeeper and KRaft modes, with an asymmetric strategy.**
ZK mode (v0.3) is frozen at 3.7.1 as the verified release for existing clusters; KRaft mode (v0.4) is the mainline going forward. This mirrors how Confluent rolled out MRC Observers: first on ZK (CP 5.4), KRaft added later (CP 7.5), ZK removed at CP 8.0.

## Why both

| Audience | Mode they need | Why |
|---|---|---|
| Existing financial/trading clusters (the users who need observers most) | ZooKeeper | Large fleets on pre-4.0 versions; can't migrate quickly; upstream will never backport this capability |
| New clusters | KRaft | Kafka 4.0 removed ZooKeeper entirely — there is no ZK option anymore |
| Migrating clusters | **Both** | Observer semantics, `observer.ids` file format, SOPs and runbooks are identical across modes → observer capability survives the ZK→KRaft migration with no gap |

## Verified capability matrix (real-machine evidence)

| Hook | File | ZK 3.7.1 | KRaft 3.7.1 (probe) |
|---|---|---|---|
| [1] Promotion gate `canAddReplicaToIsr` | `Partition.scala` (broker) | ✅ verified | ✅ **verified** — observer stays out of ISR; file edit promotes within 5 s |
| [2] Demotion hook `getOutOfSyncReplicas` | `Partition.scala` (broker) | ✅ verified | ✅ **verified** — native shrink ejects observer |
| [2b] HW gate `maybeIncrementLeaderHW` | `Partition.scala` (broker) | ✅ verified | ✅ code path shared (low-load probe; no anomalies) |
| [3] Initial-ISR exclusion | `PartitionStateMachine.scala` (ZK controller) | ✅ verified | ❌ **does not fire** — probe measured initial `Isr: 2,3,1` including the observer |
| [4] Unclean-election exclusion | `PartitionStateMachine.scala` (ZK controller) | ✅ verified | ❌ ZK controller code never runs under KRaft |
| Dynamic file (`observer.ids`, 5 s cache) | `kafka.observer.ObserverIds` | ✅ verified | ✅ verified ("Observer id set changed" logged) |

Probe evidence with raw logs: [`evidence/kraft_probe_evidence.md`](../evidence/kraft_probe_evidence.md).

**Interim behavior of the v0.3 patch on KRaft** (useful to know, not a supported configuration): a new topic starts with the observer *inside* the ISR for ~15–30 s until the demotion hook ejects it; after that, behavior matches ZK mode. The gap windows are: initial ISR membership at creation, and unclean-election exposure. v0.4 closes both.

## v0.4 KRaft patch design (source-verified against tags 3.7.1 / 4.0.0)

Broker side — **reused verbatim** from v0.3 (~40 lines, `Partition.scala` is shared by both modes; `canAddReplicaToIsr` returning false prevents the AlterPartition request from ever being sent).

Controller side — new, in the `metadata` module (Java, ~70 lines), and *more convergent* than the ZK version:

1. **`ObserverReplicas.java`** (new, ~35 lines): static helper reading the same `observer.ids` file with an mtime cache.
2. **`ReplicationControlManager.buildPartitionRegistration`** (3.7.1 L823): filter observers from the initial ISR; if the filter empties the ISR, keep the original and WARN (never block topic creation). Covers all three call sites (createTopic manual/auto placement, createPartitions).
3. **`LeaderAcceptor.test`** (RCM L2275): `if (isObserver(brokerId)) return false;` — **one line covers all seven election entry points**, including unclean, preferred, reassignment, fence/unfence, controlled shutdown. The ZK version needed two separate hooks for the same coverage.
4. **`ReplicationControlManager.ineligibleReplicasForIsr`** (L1258): reject observers in AlterPartition as `INELIGIBLE_REPLICA` — defense-in-depth if a broker-side gate is missing.
5. **ELR (KIP-966)** — only needed when `eligible.leader.replicas` is enabled (default **off** in 3.7 via double gate; off in 4.0; default-on expected from 4.1 new clusters): filter observers in `PartitionChangeBuilder.maybePopulateTargetElr` (targetElr + targetLastKnownElr chains, ~4 lines).

Total: ~115 lines vs ~60 for ZK.

**KRaft-specific deployment note**: the controller quorum may run on separate machines — the patched jar **and** `observer.ids` must be deployed to controller nodes too, and the promotion SOP should update controller nodes *first* (if a broker's gate opens before the controller's, the AlterPartition is rejected with `INELIGIBLE_REPLICA` until the controller file catches up — fail-safe direction, but adds latency).

**A KRaft-native win**: brokers learn assignments from the shared metadata log (`TopicDelta.localChanges` checks replicas, not ISR), so the ZK-mode limitation "observer discovers a new topic only after restart" **does not exist** under KRaft.

## Support matrix

| Kafka version | ZK mode | KRaft mode |
|---|---|---|
| 3.7.1 | ✅ v0.3 (verified, frozen) | 🔄 v0.4 target — broker hooks probe-verified |
| 3.9.x | 🤝 community (same patch expected to apply; not CI-verified yet) | 🔄 v0.4 secondary target (KIP-853 dynamic quorum) |
| 4.0.x | n/a (ZK removed upstream) | 🔄 v0.4 (same hook points confirmed at 4.0.0: `Partition.scala` L1038/L1052/L1153) |
| 4.1+ | n/a | v0.5 — must include ELR exclusion (ELR default-on for new clusters at MV ≥ 4.1-IV0) |

Out of scope: clusters **mid-migration** (ZK→KRaft dual-write) — the two controller planes coexist and both would need consistent patching; finish the migration first.

## Known upstream oddity (recorded during source verification)

3.7.1 `PartitionChangeBuilder.canElectLastKnownLeader` (L319-322) appears to log "not alive" yet still return true — a suspected missing negation upstream. Harmless while ELR is off; re-check when building against 3.9/4.x.
