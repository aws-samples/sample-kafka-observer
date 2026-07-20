# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Versioning starts at v0.3.0 to reflect the three POC iterations that produced
the current design. Nothing before v1.0 carries API-stability guarantees.

## [Unreleased]

- CI: patch-apply + compile verification matrix (Kafka 3.6.2 / 3.7.1 / 3.8.1 / 3.9.1),
  weekly version-drift sentinel, shellcheck / terraform fmt / python lint
- `tools/check-anchors.sh` — offline anchor verification against GitHub raw sources
- v0.4 (planned): KRaft controller-side support in the `metadata` module
  (design complete and probe-verified, see ROADMAP.md)

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

[Unreleased]: https://github.com/aws-samples/sample-kafka-observer/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/aws-samples/sample-kafka-observer/releases/tag/v0.3.0
