# Roadmap

Versioning starts at v0.3 to reflect the three POC iterations that produced the current design. Nothing before v1.0 carries API-stability guarantees.

## v0.3 — current (ZooKeeper mode, verified)

Shipped capability (all verified on real EC2 clusters, Tokyo, 3 AZ):

- File-driven observer list (`/opt/kafka/observer.ids`, 5 s cache, fail-safe read)
- 5 hook points on Kafka 3.7.1:
  1. `Partition.canAddReplicaToIsr` — promotion gate (observer never enters ISR)
  2. `Partition.getOutOfSyncReplicas` — demotion hook (in-ISR observer treated as lagging → native shrink)
  3. `Partition.maybeIncrementLeaderHW` — HW never waits for observers (structural, not just empirical)
  4. `PartitionStateMachine` initial ISR — observers excluded at topic creation
  5. `PartitionStateMachine` unclean election — observers excluded even in last-resort election
- Promotion ≤10 s / demotion ≤10 s, zero restart, zero data movement
- EOS preservation verified byte-level (CRC per batch, txn markers, `read_committed`)

Known limitations (documented, not hidden):

- ZK-mode controller only notifies ISR members on topic creation → **even a running observer never learns a new topic's assignment** (no partition directory, no fetch; promotion would fail) until its next restart or a controller failover. Existing topics unaffected. Operational rule: restart the observer once after creating topics that span it. (KRaft mode verified free of this issue — brokers read the metadata log.)
- Observer list file must be identical on all brokers; inconsistency window is bounded (rollout + 5 s) but should be pushed by a single script with checksum verification.
- Demoting a broker that is currently leader requires moving the leader first (the native shrink path never removes the leader itself — this is a safety property, not a bug).

## v0.4 — KRaft support (design complete, probe-verified)

Status upgrade after real-machine probe + source verification (2026-07-20, see `evidence/kraft_probe_evidence.md` and `docs/multi-version.md`):

- **Broker-side hooks (1–3): verified working on a real KRaft cluster** — the patched 3.7.1 jar ran a pure-KRaft 3-node cluster; promotion gate, demotion hook, and dynamic `observer.ids` file all behaved identically to ZK mode. `canAddReplicaToIsr` returning false prevents the AlterPartition from ever being sent, so the gate semantics carry over unchanged. [fact]
- **Controller-side: confirmed NOT to fire under KRaft** — probe measured a new topic's initial ISR *including* the observer. Rework lands in the `metadata` module (Java, ~70 lines, source-verified against 3.7.1/4.0.0):
  - `ObserverReplicas.java` helper (file + mtime cache)
  - `ReplicationControlManager.buildPartitionRegistration` — initial-ISR filter (3 call sites covered)
  - `LeaderAcceptor.test` — one line covers all 7 election entry points incl. unclean
  - `ReplicationControlManager.ineligibleReplicasForIsr` — AlterPartition defense-in-depth (`INELIGIBLE_REPLICA`)
- ELR (KIP-966) exclusion in `PartitionChangeBuilder.maybePopulateTargetElr` — required only where ELR enabled (default off in 3.7/4.0; expected on for new 4.1+ clusters) → can slip to v0.5.
- KRaft deployment SOP difference: patched jar + `observer.ids` must reach **controller quorum nodes** too; update controllers before brokers when promoting (mismatch fails safe: `INELIGIBLE_REPLICA` until consistent).
- Out of scope: clusters mid ZK→KRaft migration (dual controller planes).
- Config distribution: file stays for v0.4; topic-level config (`observer.replicas`, prototyped in v0.2) or metadata-log-propagated marker is the v0.5+ direction.
- Effort estimate: patch 1–2 days, full 5-capability re-verification on a 3-broker + 3-controller topology 2–3 days.

## v0.5 — operability (planned)

- `observer-promote.sh` / `observer-demote.sh` CLI with built-in pre-checks (caught-up lag threshold; leader check; post-demotion `ISR ≥ min.insync.replicas`)
- Metrics: `ObserversInIsrCount`, per-replica `isObserver / isCaughtUp / lastCaughtUpLagMs` (parity with Confluent's `kafka-replica-status.sh` output)
- Optional auto-promotion policy (`under-min-isr`, default **off** — deterministic manual operation is the recommended posture for financial workloads)
- Promotion/demotion audit log

## Later

- Terraform one-command verification environment (module exists; extend to full test matrix)
- CI: patch-apply + compile verification for every supported version on every commit
- Upstream engagement: track KIP-966 (ELR) as the official "ISR membership ≠ election eligibility" beachhead; contribute learnings if a real observer KIP ever opens (KIP-929 is an empty placeholder as of 2026-07).
