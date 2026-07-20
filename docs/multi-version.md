# Multi-version & dual-mode support

**Decision: support both ZooKeeper and KRaft modes, with an asymmetric strategy.**
ZK mode (v0.3) is frozen at 3.7.1 as the verified release for existing clusters; KRaft mode (v0.5 for 3.7.1, v0.6 for 4.0/4.1) is the mainline going forward. This mirrors how Confluent rolled out MRC Observers: first on ZK (CP 5.4), KRaft added later (CP 7.5), ZK removed at CP 8.0.

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

## Kafka 4.x differences (v0.6, real-machine verified)

Kafka 4.0 removed the ZooKeeper controller entirely, requires JDK 17+, and is Scala 2.13-only. The 4.x patches (`patches/kafka-4.0.0-kraft/`, `patches/kafka-4.1.0-kraft/`) are pure-KRaft ports of the 3.7.1 combined patch. Full port log: [`evidence/kafka40_port_evidence.md`](../evidence/kafka40_port_evidence.md).

### Hunk-by-hunk port result (3.7.1 combined → 4.0.0)

| File / hunk | 3.7.1 line | 4.0.0 result |
|---|---|---|
| `Partition.scala` — canAddReplicaToIsr / shouldWaitForReplicaToJoinIsr / getOutOfSyncReplicas (3 hunks) | @1044 / @1176 / @1312 | clean apply at @1036 / @1170 / @1310 — **line drift only (-8/-13/-11), anchor code verbatim-identical** |
| `PartitionStateMachine.scala` — ZK initial ISR + ZK unclean election (2 hunks) | present | **dropped** — file does not exist in 4.0 (ZK controller deleted); KRaft equivalents below cover the same semantics |
| `ObserverIds.scala` (new file, 76 lines) | — | copied unchanged (`kafka.utils.Logging` dependency still present; code is Scala-2.13-clean) |
| `ObserverReplicas.java` (new file, 157 lines) | — | copied unchanged (`org.apache.kafka.controller` package unchanged) |
| `ReplicationControlManager.java` — buildPartitionRegistration / ineligibleReplicasForIsr / LeaderAcceptor.test (3 hunks) | @824 / @1260 / @2272 | clean apply at @855 / @1337 / @2445 — **line drift only (+31/+74/+166)** |

Net: 10 hunks → 8 hunks, **zero hand edits**. The 4.1.0 patch is byte-identical to the 4.0.0 one (hunk offsets shift by +4/-1/-1 and -1/-1/+30); it also applied 8/8 clean and compiled.

Build note: `./gradlew :metadata:jar :core:jar :storage:jar -x test` with JDK 17; the default gradle heap is 4 GB — on small hosts `-Dorg.gradle.jvmargs="-Xmx2g"` is sufficient (verified).

### ELR (KIP-966) — verified compatible, no code needed

- 4.0.0: ELR supported but **off by default** (`eligible.leader.replicas.version` finalized=0); can be enabled with `kafka-features.sh upgrade --feature eligible.leader.replicas.version=1`. 4.1.0 new clusters (4.1-IV1): **default-on**. So "ELR needs 4.1+" is imprecise — 4.1 is the *default-on* watershed.
- The v0.4 design item "filter observers in `maybePopulateTargetElr`" turned out to be **unnecessary**: the ELR candidate set is `ELR ∪ ISR`, and observers never enter the ISR (broker gate + controller gate), so they **structurally never enter ELR or LastKnownElr**. Verified on real clusters on both 4.0 (ELR manually enabled) and 4.1 (default-on): kill-ISR sequences never showed the observer in `Elr:`/`LastKnownElr:`, and it was never elected even with `unclean.leader.election.enable=true`. See [`evidence/elr_verification_evidence.md`](../evidence/elr_verification_evidence.md).
- ELR is complementary to observers: crashed non-observer ISR members enter ELR and recover with a *clean* election (zero data loss), stacking with the observer's never-unclean-elected guarantee.
- 4.0's `kafka-topics.sh --describe` natively prints `Elr:` / `LastKnownElr:` columns — no metadata-shell needed for observation.

### Operational notes carried over from 3.7.1 KRaft (re-verified on 4.0/4.1)

- Demoting a *leader* observer does not take effect hot (the leader never self-removes from ISR); move leadership first or restart that broker once. Follower demotion is hot (~12 s measured on 4.0).
- Both dynamic-load paths work: broker-side `kafka.observer.ObserverIds` and controller-side `org.apache.kafka.controller.ObserverReplicas` each log "Observer id set changed".

## Support matrix

| Kafka version | ZK mode | KRaft mode |
|---|---|---|
| 3.6.2 / 3.8.1 / 3.9.1 | ✅ canonical 3.7.1 patch applies + compiles (real-machine + weekly CI sentinel) | — (not a target; 3.7.1 is the verified KRaft baseline for 3.x) |
| 3.7.1 | ✅ v0.3 (verified, frozen) | ✅ v0.5 (verified, combined patch — full 8-item capability matrix) |
| 4.0.0 | n/a (ZK removed upstream) | ✅ v0.6 (verified — 6-item capability matrix on a real 6-node cluster; ELR off by default, manually-enabled ELR also verified) |
| 4.1.0 | n/a | ✅ v0.6 (verified — patch byte-identical to 4.0.0; ELR default-on verified; includes upstream KAFKA-19522 fix) |

Out of scope: clusters **mid-migration** (ZK→KRaft dual-write) — the two controller planes coexist and both would need consistent patching; finish the migration first.

## Known upstream oddity (resolved in v0.6)

3.7.1 `PartitionChangeBuilder.canElectLastKnownLeader` (L319-322) appears to log "not alive" yet still return true — a suspected missing negation upstream. **v0.6 resolution**: confirmed a real upstream bug, still present verbatim in 4.0.0, fixed upstream in 4.1.0 (KAFKA-19522, commit `e4e2dce2eb`). It has no observer-election pathway — observers never appear in LastKnownElr (real-machine verified) — but on 4.0 with ELR enabled it can mis-elect a fenced ordinary broker. Guidance: use 4.1.0 if you need ELR; keep ELR off (default) on 4.0.0.
