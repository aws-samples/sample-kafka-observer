# sample-kafka-observer

**Observer/Learner replicas for Apache Kafka — an open reference implementation.**

Add a third replica state to open-source Apache Kafka: replicas that **fully sync data but never join the ISR** — so they never drag the high-watermark, never become leader, and can be **promoted to a fully electable replica in seconds, with zero restarts and zero data movement**.

This is the capability known commercially as Confluent Multi-Region Clusters (MRC) *Observers*, and internally at some large tech companies as *Learner* replicas. Apache Kafka upstream has no equivalent (KIP-929 is an empty placeholder). This project is a minimal, auditable, reproducible implementation — **~60 lines of Scala across 5 hook points**, all reusing Kafka's native ISR expansion/shrink machinery.

> Every number in this repository was measured on real EC2 instances (Tokyo, 3 brokers across 3 AZs, m7g.large). Evidence files with raw command output are in [`evidence/`](evidence/).

## What you get

| Capability | Mechanism | Verified |
|---|---|---|
| Sync all data, never join ISR | Gate in `canAddReplicaToIsr()` | ✅ `Isr: 2,3` while `Replicas: 2,3,1` |
| Never drag the high-watermark | Gate in `maybeIncrementLeaderHW` | ✅ acks=all 2.04–2.35 ms with observer in the slowest AZ |
| Never become leader (incl. unclean) | Excluded from initial ISR + unclean election | ✅ kill all ISR → `Leader: none` |
| **Promote** (observer → electable) | Remove id from `/opt/kafka/observer.ids` → next fetch passes the gate → native ISR expand | ✅ ≤10 s, zero restart |
| **Demote** (electable → observer) | Add id back → native `isr-expiration` task shrinks it out | ✅ ≤10 s, zero restart |
| Promoted replica leads & serves | Full election eligibility restored | ✅ kill all ISR → `Leader: 1`, write + read OK |
| Exactly-once preserved | `appendAsFollower` byte-copies leader batches — offsets, PID, epoch, sequence, txn markers | ✅ per-batch CRC identical; `read_committed` view identical; MM2 control group produced 20 000 duplicates under the same failure |

## Why this matters

Cross-cluster replication tools (MirrorMaker 2, Confluent Replicator, uReplicator, Brooklin) are all *consume → re-produce*: the target cluster reassigns offsets, so exactly-once is structurally impossible and client failover requires offset translation. A same-cluster observer replica is *replicate-the-log*: there is no second offset space, so EOS survives replication for free. See [docs/eos-semantics.md](docs/eos-semantics.md) and the [industry comparison](docs/industry-comparison.md).

## Quick start

```bash
# 1. Get clean Kafka source (3.7.1, ZooKeeper mode — see docs/multi-version.md for others)
git clone --depth 1 --branch 3.7.1 https://github.com/apache/kafka.git kafka-src

# 2. Apply the patch
cd kafka-src && python3 ../patches/kafka-3.7.1-zk-v0.3.py

# 3. Build (needs JDK 17 with javac; ~1–3 min on 4 vCPUs)
./gradlew :core:jar -x test

# 4. Deploy: replace kafka_2.13-3.7.1.jar and kafka-storage-3.7.1.jar on every broker,
#    create /opt/kafka/observer.ids containing the observer broker ids, rolling restart.

# 5. Verify
kafka-topics.sh --describe --topic your_topic   # observer id absent from Isr
```

Full deployment guide including rolling-replacement SOP: [docs/deployment.md](docs/deployment.md).

## Failure runbooks

- **Scenario A — one primary AZ down**: writes fail-stop (`NOT_ENOUGH_REPLICAS`) → delete observer id from the file → observer joins ISR in ≤10 s → writes resume. No data movement (it was byte-identical all along). RPO = 0. [runbook](docs/runbooks/scenario-a-az-loss.md)
- **Scenario B — all primary replicas down**: promote observer → it is elected leader and serves reads/writes (verified on real machines). [runbook](docs/runbooks/scenario-b-total-loss.md)
- Pre-checks, multi-observer layouts, monitoring: [docs/runbooks/](docs/runbooks/)

## Version support

| Kafka version | Mode | Status |
|---|---|---|
| 3.7.1 | ZooKeeper | ✅ verified on real clusters (v0.3, [evidence](evidence/observer_v3_lifecycle_evidence.md)) |
| 3.7.1 | **KRaft** | ✅ **verified — full 8-item capability matrix passed** (v0.5): initial-ISR filtering, unclean election refusal, promotion 4 s / demotion 9 s, promoted observer serves as leader, new-topic instant fetch. Controller-side Java patch (`ObserverReplicas.java` + 3 RCM hooks), verified in both combined and controller-only topologies. [evidence](evidence/kraft_controller_patch_evidence.md) |
| 3.6.2 / 3.8.1 / 3.9.1 | ZooKeeper | ✅ canonical patch applies + compiles cleanly (real-machine, [evidence](evidence/multiversion_apply_evidence.md)); weekly CI drift sentinel |
| 4.0.x | KRaft | 🔄 same hook points confirmed at source level; CI verification pending |
| 4.1+ | KRaft | 🔄 adds ELR (KIP-966) test coverage (observers structurally never enter ELR) |

Patches: [`patches/kafka-3.7.1-zk/`](patches/kafka-3.7.1-zk/) (ZK-only) and [`patches/kafka-3.7.1-kraft/`](patches/kafka-3.7.1-kraft/) (**combined ZK+KRaft** — one patched build serves both modes; deploy `core` + `storage` + `metadata` jars). Full rationale: [docs/multi-version.md](docs/multi-version.md).

> ⚠️ **KRaft-specific demotion rule** (real-machine finding): demoting an observer that is *currently a leader* does not take effect hot — the leader never self-removes from ISR and KRaft has no ZK-style re-election path for this. Move leadership first (`kafka-leader-election.sh`), or restart that broker once. Follower demotion is hot (≤10 s) as usual.

## Project status & versioning

Current release: **v0.3** (capabilities above, ZK mode). Roadmap to v0.4+ (KRaft, auto-promotion policy, metrics) in [ROADMAP.md](ROADMAP.md).

## License & trademark note

Patches are provided under Apache License 2.0. Binaries built with these patches are **modified versions of Apache Kafka** — if you redistribute them, mark them as modified (NOTICE) and do not label them "Apache Kafka". This project is not affiliated with the Apache Software Foundation or Confluent.
