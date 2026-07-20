# Roadmap

Versioning starts at v0.3 to reflect the three POC iterations that produced the current design. Nothing before v1.0 carries API-stability guarantees.

## v0.6 — Kafka 4.0 / 4.1 support ✅ SHIPPED (2026-07-20)

All verified on real EC2 clusters (Tokyo, 3 controller-only + 3 broker nodes) — see `evidence/kafka40_port_evidence.md` and `evidence/elr_verification_evidence.md`:

- **4.0.0 port**: the 8 usable hunks of the 3.7.1 combined patch applied with line-number drift only, zero hand edits; the 2 ZK-controller hunks are dropped (Kafka 4.0 removed ZooKeeper entirely). Canonical patch: `patches/kafka-4.0.0-kraft/observer.patch`.
- **4.0.0 capability matrix passed** (6/6): initial-ISR filtering, full sync (byte-identical), promotion ~4 s, follower demotion ~12 s, preferred election after promotion, unclean election refusal (`Leader: none` with only the observer alive; recovery with zero data loss).
- **4.1.0 port**: patch byte-identical to the 4.0.0 one (hunk offsets only), compiles cleanly. Canonical patch: `patches/kafka-4.1.0-kraft/observer.patch`.
- **ELR (KIP-966) compatibility proven on real clusters** — the v0.4 design item "filter observers in `maybePopulateTargetElr`" turned out to be **unnecessary**: the ELR candidate set is `ELR ∪ ISR`, and observers never enter the ISR, so they structurally never enter ELR or LastKnownElr. Verified on 4.0 (ELR manually enabled via `kafka-features.sh upgrade`) and 4.1 (ELR default-on for new clusters at 4.1-IV1): kill-ISR sequences never put the observer in `Elr:`/`LastKnownElr:`, and it is never elected even with `unclean.leader.election.enable=true`. The patch touches no ELR code.
- **Upstream bug closure**: the suspected missing negation in `PartitionChangeBuilder.canElectLastKnownLeader` (recorded during v0.4 source verification) is a confirmed upstream bug, fixed in 4.1.0 as KAFKA-19522. It has no observer-election pathway (observers never appear in LastKnownElr); on 4.0 it can mis-elect a fenced ordinary broker when ELR is on — recommendation: use 4.1 for ELR, keep ELR off (default) on 4.0.
- **CI**: build-verify matrix extended to 7 legs (3.6.2/3.7.1/3.8.1/3.9.1 ZK + 3.7.1 KRaft-combined + 4.0.0/4.1.0 KRaft) with per-version patch paths; `tools/check-anchors.sh` extended with KRaft controller anchors (K1–K3) and 4.x-aware skipping of ZK anchors.
- Version guidance: for ELR use 4.1.0 (default-on + KAFKA-19522 fix); 4.0.0 with ELR off behaves identically to 3.7.1.

Carried over to v0.7 (not blocking): none of the 4.0/4.1 items — the runtime ELR tests originally slated as "pending" were completed in this cycle.

## v0.8 / v1.0 — outlook

- **Topic-level observer config** (`observer.replicas`, prototyped in v0.2) or a metadata-log-propagated marker as the file-distribution successor — removes the multi-node file-consistency burden entirely
- **Upstream KIP tracking**: KIP-966 (ELR) as the official "ISR membership ≠ election eligibility" beachhead; contribute learnings if a real observer KIP ever opens (KIP-929 remains a zero-length placeholder as of 2026-07)
- **Long-running soak test**: multi-day sustained-traffic run with periodic fault injection — auto-promoter daemon mode (`-i 10`) cooldown/anti-flap behavior under repeated cycles is the main unproven area (v0.7 verified single-scan-driven cycles only)
- v1.0 gate: API/file-format stability guarantees, at least one external production adoption report

## v0.7 — operability ✅ SHIPPED (2026-07-20)

All verified end-to-end on a real patched KRaft cluster (Tokyo; JMX readings via JmxTool + fault injection) — see `evidence/metrics_patch_evidence.md` (build) and `evidence/v07_operability_evidence.md` (runtime):

- **7 JMX gauges** layered on the v0.6 combined patch — canonical patch `patches/kafka-3.7.1-kraft-v07/observer.patch` (6 files, +384/−8; the 12 functional `OBSERVER PATCH` markers verified byte-identical to v0.6 — zero behavior change):
  - broker: `kafka.observer:…ObserverCount`, `ReplicaManager` `ObserversInIsrCount` / `ObserverCaughtUpCount` / `ObserverLagMessages`
  - per-partition: the same three under `kafka.cluster:type=Partition` — parity with the isObserver/isCaughtUp/lag fields of Confluent's `kafka-replica-status.sh`
  - `ObserversInIsrCount` semantics confirmed in both directions on a live cluster: a promoted broker in ISR but off the list reads 0; the demotion transition shows a ~5 s `Value=1` window before native shrink returns it to 0 → alert on `> 0` sustained beyond 2× `replica.lag.time.max.ms`
  - Known boundary (as measured): the `kafka.observer` ObserverCount MBean is lazily registered — absent on a broker leading no partitions; per-partition lag is LEO message count, not time (`lastCaughtUpLagMs` equivalent is derivable from caught-up counts; no per-replica MBean to avoid cardinality explosion)
- **Structured audit log**: WARN `OBSERVER AUDIT (broker)` / `(controller)` pair on every observer-set change, fields `before/after/added/removed/source/epochMs` (`removed`≠∅ = promotion, `added`≠∅ = demotion); measured file-change→first-audit-line latency 3–6 s (within the 5 s cache)
- **Auto-promotion watchdog** (`scripts/observer-auto-promoter.sh`, `under-min-isr` policy, default **OFF** with `-e` interlock): full fault-injection cycle verified — broker kill → `DETECT` → automatic promotion (scan→PROMOTE-OK **12 s**, ISR restored to ≥ minISR with zero restart/data movement) → broker recovery → double-confirmed automatic demotion (scan→DEMOTE-OK **31 s**, state file correctly cleared). Dry-run mode (`-n`) verified to mutate nothing. Design + risk boundary + enable SOP: `docs/auto-promotion.md`
- Promote/demote CLI pre-checks (caught-up lag threshold; leader check; post-demotion `ISR ≥ min.insync.replicas`) exercised as part of the demotion path
- aws-samples publication readiness: repo hygiene pass, license/NOTICE review, README polish (`PUBLICATION_CHECKLIST.md`)

Not done in v0.7 (moved up): topic-level observer config → v0.8; daemon-mode long-soak observation → v0.8.

## v0.3 — ZooKeeper mode (shipped)

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

## v0.5 — KRaft support ✅ SHIPPED (2026-07-20)

Full 8-item capability matrix passed on real machines (Tokyo, both combined and controller-only topologies) — see `evidence/kraft_controller_patch_evidence.md`:

1. ✅ Initial-ISR filtering (the decisive item that failed in the probe): controller log `Filtered observers [3] from initial ISR [1,2,3] -> [1,2]`
2. ✅ Full sync, byte-identical data on observer
3. ✅ Never in ISR under sustained traffic
4. ✅ Promotion ≤30 s target → measured **4 s**
5. ✅ Demotion ≤45 s target → measured **9 s** (follower case)
6. ✅ Promoted observer elected leader + serves writes (200 msgs verified)
7. ✅ New-topic instant fetch (ZK-mode limitation confirmed absent in KRaft)
8. ✅ AlterPartition defense-in-depth (`INELIGIBLE_REPLICA "observer"`)
9. ✅ Unclean election refusal: with only the observer surviving, `Leader: none` — never the observer

Deliverables: `patches/kafka-3.7.1-kraft/observer.patch` (combined ZK+KRaft — one build serves both modes; **deploy core + storage + metadata jars**, and `observer.ids` to controller nodes too).

**New operational finding**: demoting a *leader* observer does not take effect hot under KRaft (leader never self-removes from ISR; no ZK-style re-election path). SOP: move leadership first, or restart that broker. Codified in runbooks.

## v0.4 — original design notes (superseded by shipped v0.5 above)

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

## Later

- Terraform one-command verification environment (module exists; extend to full test matrix)
