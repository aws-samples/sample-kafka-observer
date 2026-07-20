# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Versioning starts at v0.3.0 to reflect the three POC iterations that produced
the current design. Nothing before v1.0 carries API-stability guarantees.

## [Unreleased]

- v0.8 (outlook): topic-level observer config (`observer.replicas`) or
  metadata-log-propagated marker as the file-distribution successor; upstream
  KIP tracking (KIP-966 / KIP-929); long-running soak test of the
  auto-promoter daemon mode (see ROADMAP.md)

## [0.7.0] - 2026-07-20

Operability layer: JMX metrics, structured audit log, and an opt-in
auto-promotion watchdog — all verified end-to-end on a real patched KRaft
cluster (Tokyo; JMX readings + fault injection). Functional observer hooks
are byte-identical to v0.6 (zero behavior change — v0.7 only adds the
ability to observe and to automate). Raw evidence in
`evidence/metrics_patch_evidence.md` (build) and
`evidence/v07_operability_evidence.md` (runtime).

### Added

- **Canonical patch** `patches/kafka-3.7.1-kraft-v07/observer.patch` —
  combined ZK+KRaft patch (v0.6 hooks, 12 `OBSERVER PATCH` markers verified
  byte-identical) plus the v0.7 observability layer (6 files, +384/−8):
  - **7 JMX gauges**, all reusing native `KafkaMetricsGroup` registration
    (lock-free reads of existing volatile state, no extra IO):
    - `kafka.observer:type=ObserverMetrics,name=ObserverCount` — this node's
      view of the observer set size (detects file drift across nodes;
      lazily registered — absent on a broker leading no partitions)
    - `kafka.server:type=ReplicaManager,name=ObserversInIsrCount` — steady
      state 0; measured non-zero only in the demotion transition window
      (~5 s at `replica.lag.time.max.ms=10s`) or on a real gate bypass /
      file inconsistency. Recommended alert: `> 0` sustained beyond
      2× `replica.lag.time.max.ms`
    - `kafka.server:type=ReplicaManager,name=ObserverCaughtUpCount` /
      `ObserverLagMessages` — observer catch-up status using the native
      `isCaughtUp` function (same semantics as the ISR check)
    - per-partition `kafka.cluster:type=Partition,name={ObserversInIsrCount,
      ObserverCaughtUpCount,ObserverLagMessages},topic=…,partition=…` —
      parity with the isObserver/isCaughtUp/lag fields of Confluent's
      `kafka-replica-status.sh`
- **Structured audit log** (WARN, default-visible): every observer-set change
  emits a broker/controller pair — `OBSERVER AUDIT (broker)` /
  `OBSERVER AUDIT (controller)` — with
  `before/after/added/removed/source/epochMs` fields (`removed` non-empty =
  promotion, `added` non-empty = demotion; `source` distinguishes file vs
  env fallback). Measured file-change → first audit line: 3–6 s.
- **Auto-promotion watchdog** `scripts/observer-auto-promoter.sh`
  (`under-min-isr` policy, **default OFF** with an explicit `-e` interlock)
  + systemd unit template `deploy/observer-auto-promoter.service` (never
  auto-installed) + design/risk/SOP doc `docs/auto-promotion.md`.
  Fault-injection verification on a live cluster: broker kill → `DETECT` →
  automatic promotion (scan→PROMOTE-OK **12 s**; ISR restored to ≥ minISR,
  zero restart, zero data movement) → broker recovery → double-confirmed
  automatic demotion (scan→DEMOTE-OK **31 s**, incl. 5 s double-check;
  ownership state file correctly cleared). Dry-run mode (`-n`) verified to
  log full decisions while mutating nothing. Safety: caught-up gate,
  anti-flap cooldown, max one action per scan, scoped ownership
  (only auto-demotes brokers it promoted), audit-or-die.
- **docs/monitoring-alerting.md** updated from design names to the shipped,
  measured metric semantics and alert thresholds.

### Known boundaries (measured, not hidden)

- The `kafka.observer` ObserverCount MBean is lazily registered; monitoring
  must tolerate its absence on brokers leading no partitions (or use the
  ReplicaManager gauges).
- Per-partition lag is an LEO message count, not a time lag; the
  `lastCaughtUpLagMs` equivalent is derivable from caught-up counts (no
  per-replica MBean — avoids MBean-cardinality explosion).
- The demotion transition shows `ObserversInIsrCount=1` for roughly the
  native shrink latency (longer at the default 30 s
  `replica.lag.time.max.ms`) — alert rules need a duration condition.
- Auto-promoter daemon mode (`-i` loop) shares the verified single-scan
  logic, but cooldown/anti-flap behavior under long sustained operation has
  not been soak-tested (v0.8 item).

## [0.6.0] - 2026-07-20

Kafka 4.0 / 4.1 support (pure KRaft — ZooKeeper is removed upstream in 4.0),
verified end-to-end on real EC2 clusters (Tokyo, 3 controller-only + 3 broker
nodes). Raw evidence in `evidence/kafka40_port_evidence.md` and
`evidence/elr_verification_evidence.md`.

### Added

- **Canonical patch** `patches/kafka-4.0.0-kraft/observer.patch` — port of the
  3.7.1 combined patch to Kafka 4.0.0: all 8 usable hunks applied with
  line-number drift only (zero hand edits); the 2 ZK-controller hunks
  (`PartitionStateMachine.scala`) are dropped because Kafka 4.0 deleted the
  ZooKeeper controller. Compiles on JDK 17 / Scala 2.13 / Gradle 8.10.2.
- **Canonical patch** `patches/kafka-4.1.0-kraft/observer.patch` — byte-identical
  patch content to the 4.0.0 one (hunk offsets only); compiles cleanly on 4.1.0.
- **CI matrix extended to 7 legs** (`.github/workflows/build-verify.yml`):
  3.6.2 / 3.7.1 / 3.8.1 / 3.9.1 ZK + 3.7.1 KRaft-combined + 4.0.0 / 4.1.0
  KRaft, using per-version `include:` triples (version + patch path + gradle
  tasks); KRaft legs also compile `:metadata:compileJava` and check the
  controller-side patch markers.
- **`tools/check-anchors.sh` extended**: new KRaft controller anchors K1–K3
  (`ReplicationControlManager` initial-ISR / AlterPartition gate /
  LeaderAcceptor); ZK anchors A4–A5 auto-skip on 4.x (file removed upstream);
  default matrix now covers 3.6.2–4.1.0.
- **docs/multi-version.md**: Kafka 4.x differences section (hunk-by-hunk port
  analysis, ELR compatibility, version guidance).

### Verified capabilities (v0.6.0, Kafka 4.0.0 real cluster)

- Initial-ISR filtering (controller log
  `Filtered observers [3] from initial ISR [1, 2, 3] -> [1, 2]`)
- Full sync, byte-identical observer data under acks=all traffic
- Promotion ~4 s / follower demotion ~12 s, zero restart
- Preferred election after promotion; promoted observer serves writes
- Unclean election refusal: kill all ISR members → `Leader: none` (the
  surviving observer is never elected, even with
  `unclean.leader.election.enable=true`); recovery with zero data loss

### ELR (KIP-966) compatibility — verified, no code needed

- Observers structurally never enter ELR or LastKnownElr: the ELR candidate
  set is `ELR ∪ ISR` and observers never enter the ISR. Verified on real
  clusters on both 4.0.0 (ELR manually enabled via `kafka-features.sh
  upgrade --feature eligible.leader.replicas.version=1`) and 4.1.0 (ELR
  default-on for new clusters at 4.1-IV1). The planned
  `maybePopulateTargetElr` hook from the v0.4 design is therefore
  unnecessary — the patch touches no ELR code.
- ELR is complementary: non-observer ISR members that crash enter ELR and
  recover with a clean election (zero data loss), stacking with the
  observer's never-unclean-elected guarantee.

### Fixed / clarified

- The suspected missing negation in
  `PartitionChangeBuilder.canElectLastKnownLeader` (recorded in v0.4 source
  verification) is a **confirmed upstream bug**, fixed upstream in 4.1.0
  (KAFKA-19522). It has no observer-election pathway (observers never appear
  in LastKnownElr); on 4.0.0 with ELR enabled it can mis-elect a fenced
  ordinary broker. Guidance: use 4.1.0 if you want ELR; keep ELR at its
  default (off) on 4.0.0, where behavior is equivalent to 3.7.1.

## [0.5.0] - 2026-07-20

KRaft mode support for Kafka 3.7.1 — combined ZK+KRaft canonical patch
(`patches/kafka-3.7.1-kraft/observer.patch`): broker-side hooks unchanged,
new controller-side support in the `metadata` module (`ObserverReplicas.java`
+ 3 `ReplicationControlManager` hooks). Full 8-item capability matrix passed
on real machines in both combined and controller-only topologies — see
`evidence/kraft_controller_patch_evidence.md` and ROADMAP.md. CI matrix
(3.6.2–3.9.1 ZK) with weekly version-drift sentinel; `tools/check-anchors.sh`
offline anchor verification.

## [0.3.0] - 2026-07-20

First public release: Observer/Learner replicas for Apache Kafka (ZooKeeper
mode), verified end-to-end on real EC2 clusters (Tokyo, 3 brokers across 3 AZs,
m7g.large). Raw evidence in `evidence/`.

### Added

- **Canonical patch** `patches/kafka-3.7.1-zk/observer.patch` — 151-line diff,
  ~60 lines of Scala across 5 hook points, all reusing Kafka's native ISR
  expansion/shrink machinery:
  1. `Partition.canAddReplicaToIsr` — promotion gate: observers never enter ISR
  2. `Partition.getOutOfSyncReplicas` — demotion hook: in-ISR observer treated
     as lagging, native `isr-expiration` shrinks it out
  3. `Partition.maybeIncrementLeaderHW` — high-watermark never waits for
     observers (structural guarantee, not just empirical)
  4. `ZkPartitionStateMachine` initial ISR — observers excluded at topic
     creation, leader chosen from non-observers (fail-open if all live
     replicas are observers)
  5. `PartitionLeaderElectionAlgorithms` unclean election — observers excluded
     even in last-resort election (prefer no leader over losing consistency)
- **Dynamic observer list** `kafka.observer.ObserverIds` — file-driven
  (`/opt/kafka/observer.ids`, 5 s cache, fail-safe reads, env-var fallback);
  promotion/demotion by editing the file, **zero restart, zero data movement**,
  both ≤ 10 s in real-cluster measurements
- **Operational scripts** `scripts/observer-promote.sh` / `observer-demote.sh`
- **Build tooling** `tools/apply-and-build.sh` (clone → `git apply --3way` →
  marker check → gradle build) and `tools/generate-patch.py`
- **Documentation**: architecture, multi-version hook matrix
  (`docs/multi-version.md`), deployment guide, failure runbooks
  (Scenario A: one primary AZ down; Scenario B: total primary loss)
- **Evidence** (5 real-machine reports): observer lifecycle, byte-level EOS
  preservation (per-batch CRC identical; txn markers copied verbatim),
  `read_committed` equivalence, MM2 duplicate control group (20,000 duplicates
  under the same failure that the observer survives with zero), KRaft probe
- **License hygiene**: Apache-2.0 + NOTICE; patches are the canonical
  distribution artifact (no pre-built jars); redistributed binaries must be
  marked as modified and not labeled "Apache Kafka"

### Verified capabilities (v0.3.0)

- Sync all data, never join ISR (`Isr: 2,3` while `Replicas: 2,3,1`)
- Never drag the high-watermark (acks=all 2.04–2.35 ms with observer in the
  slowest AZ)
- Never become leader, including unclean election (kill all ISR → `Leader: none`)
- Promote observer → elected leader, serves reads/writes (RPO = 0)
- Exactly-once semantics preserved through replication (impossible for any
  consume-then-produce replicator by construction)

### Known limitations

- ZK-mode controller only notifies ISR members on topic creation → an observer
  discovers a **new** topic's assignment only after a broker restart or
  controller failover. Existing topics are unaffected. (KRaft mode does not
  have this issue — brokers read the metadata log.)
- The observer list file must be identical on all brokers; the inconsistency
  window is bounded (rollout + 5 s) but distribution should be handled by a
  single script with checksum verification.
- Demoting a broker that is currently leader requires moving leadership first
  (the native shrink path never removes the leader itself — a safety property,
  not a bug).

[Unreleased]: https://github.com/aws-samples/sample-kafka-observer/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/aws-samples/sample-kafka-observer/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/aws-samples/sample-kafka-observer/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/aws-samples/sample-kafka-observer/compare/v0.3.0...v0.5.0
[0.3.0]: https://github.com/aws-samples/sample-kafka-observer/releases/tag/v0.3.0
