# Industry comparison — replication approaches for Kafka high availability and DR

Two structurally different families exist. Everything else follows from which family a tool is in:

- **Replicate-the-log (same cluster)**: followers/observers byte-copy the leader's log via the native fetch protocol. One offset space; producer state and transaction markers travel inside the copied bytes.
- **Consume → re-produce (cross cluster)**: a client consumes from the source and produces to the target. Two offset spaces; the target broker assigns new offsets and sees a new producer session.

This project (observer replicas) is in the first family. All the tools it is usually compared against are in the second — except Confluent MRC Observers, which is the commercial implementation of the same idea.

## Six-way comparison

| | **MirrorMaker 1** | **MirrorMaker 2** (KIP-382) | **Confluent Replicator** | **uReplicator** (Uber) | **Brooklin** (LinkedIn) | **This project / Confluent MRC Observers** |
|---|---|---|---|---|---|---|
| Model | consume → re-produce | consume → re-produce (Connect-based) | consume → re-produce (Connect-based) | consume → re-produce (Helix-managed) | consume → re-produce (generic streaming bridge) | **replicate-the-log (native fetch)** |
| Scope | cross-cluster | cross-cluster | cross-cluster | cross-cluster | cross-cluster, multi-system | **same cluster**, cross-AZ / cross-site |
| Offset preservation | ❌ target reassigns | ❌ target reassigns (offset-translation topic as a workaround) | ❌ target reassigns (offset translation via timestamp interceptor) | ❌ target reassigns | ❌ target reassigns | ✅ **identical by construction** (one log) |
| Exactly-once through replication | ❌ | ❌ at-least-once (measured: 20,000 duplicates after one `kill -9`, see [evidence](../evidence/mm2_duplicate_evidence.md)) | ❌ at-least-once | ❌ at-least-once | ❌ at-least-once | ✅ **preserved** — per-batch CRC identical, txn markers byte-copied ([evidence](../evidence/eos_byte_level_evidence.md)) |
| Transaction markers propagated | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ (copied verbatim inside batches) |
| Client failover | full offset reset | offset translation (approximate) | offset translation (approximate) | offset management external | offset management external | ✅ **none needed** — same cluster, same offsets |
| Producer-visible latency impact | none (async) | none (async) | none (async) | none (async) | none (async) | **none** — HW never waits for the observer (measured 2.04–2.35 ms `acks=all` with the observer in the slowest AZ) |
| RPO on failover | > 0 (replication lag) | > 0 (replication lag) | > 0 (replication lag) | > 0 (replication lag) | > 0 (replication lag) | **0 for acknowledged writes** (observer trails only by never-acknowledged in-flight messages) |
| Extra infrastructure | MM1 processes | Connect cluster | Connect cluster (commercial license) | Helix controller + workers | Brooklin cluster | **none** — patched broker jars + one text file |
| Status | deprecated (removed in Kafka 4.0) | ✅ maintained upstream | ✅ commercial (Confluent) | archived (Uber moved on) | ✅ open source (LinkedIn) | this repo (open reference) / Confluent MRC (commercial, supported) |
| Best for | — (legacy) | cross-region DR, cluster migration, hub-and-spoke | cross-region DR with Confluent support | (historical) high-scale cross-DC | multi-system data bridges | **in-region multi-AZ HA with EOS**, strongly consistent standby |

## What this means in practice

**The two families solve different problems — they compose, they do not compete.**

- **Same-region, multi-AZ, consistency-critical** (payments, trading, ledgers): consume→re-produce tools structurally cannot preserve exactly-once (the re-produce step creates a new producer session; the target has no dedup basis — observed as `producerId = -1` on MM2's target). An observer replica is the only construction where EOS survives replication, because there is no second produce step at all.
- **Cross-region DR**: observers *can* stretch across regions (the log stays byte-identical), but a same-cluster stretch means one controller plane spanning regions — most operators prefer MM2-class tools there and accept at-least-once plus downstream idempotence. Confluent's answer at that tier is Cluster Linking (offset-preserving *cross-cluster* replication), which is out of scope for this project.
- **Rule of thumb**: inside a region, replicate the log (observers); across regions, mirror the stream (MM2 / MSK Replicator) and design consumers to be idempotent.

## This project vs Confluent MRC Observers

Same core semantics — a replica that fully syncs, never joins the ISR, never drags the high-watermark, and can be promoted without data movement.

| | Confluent MRC Observers | This project |
|---|---|---|
| Distribution | commercial product (Confluent Platform ≥ 5.4) | Apache-2.0 source patches (~60–115 lines) |
| Placement config | topic-level `confluent.placement.constraints` JSON | broker-local `observer.ids` file (topic-level config on the [roadmap](../ROADMAP.md)) |
| Promotion | `observerPromotionPolicy` (automatic options) | deliberately **manual** by default — deterministic operation for financial workloads; opt-in automation is a v0.7 roadmap item |
| Observability | `kafka-replica-status.sh`, dedicated metrics | v0.7 roadmap ([design](monitoring-alerting.md)); today: `kafka-topics.sh --describe` + log lines |
| Support | vendor-supported | community / self-supported reference |

If you want the capability with a support contract, buy Confluent MRC. If you need it on open-source Apache Kafka today (upstream KIP-929 is an empty placeholder), this repo is a minimal, auditable implementation of the same mechanism.

## Related upstream work

- **KIP-966 (Eligible Leader Replicas)** — upstream's own separation of "ISR membership" from "leader eligibility", shipped default-on in Kafka 4.1. Complementary, verified compatible: observers structurally never enter ELR ([evidence](../evidence/elr_verification_evidence.md)).
- **KIP-929 (Observer Replicas)** — a zero-length placeholder wiki page as of 2026-07; no upstream implementation exists.
- **KIP-392 (Fetch from followers)** — rack-aware consumer reads; orthogonal (about *reads*, not replica eligibility) and works unchanged with observers.
