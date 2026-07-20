# Design story — how three POC iterations produced this patch

This document is the narrative behind the code: where the problem came from, what each iteration got wrong, how the next one found out, and what that teaches about doing systems research against a large codebase. Everything here happened on real EC2 clusters in Tokyo (3 brokers across 3 AZs, m7g.large, Kafka 3.7.1); the raw outputs live in [`evidence/`](../evidence/) and the exact patch scripts of every iteration are preserved in [`patches/archive/`](../patches/archive/).

## The starting point: a requirement vanilla Kafka cannot express

The requirement pattern comes from exchange-like customers (generalized here — no specific customer data): a trading or ledger system runs its Kafka primaries on a fast AZ pair for latency, and needs a **strongly consistent, byte-identical copy in a third AZ or a remote site** for disaster recovery. Two hard constraints:

1. The backup must be *strongly consistent* — RPO = 0 for acknowledged writes, exactly-once semantics intact. Eventual-consistency replicas (MirrorMaker 2 and friends) fail this: they re-produce messages into a new offset space, and a single `kill -9` in the offset-flush window measurably re-delivered 20,000 duplicates ([evidence](../evidence/mm2_duplicate_evidence.md)).
2. The backup must **not slow down the main path**. Putting the remote replica in the ISR fails this: `acks=all` waits for every ISR member and the high-watermark is set by the slowest one, so the whole cluster inherits cross-AZ latency.

Vanilla Kafka has exactly two replica states — "in ISR" and "out of ISR (lagging)" — and neither satisfies both constraints. Confluent sells the missing third state as MRC *Observers*; some large tech companies built it internally as *Learner* replicas; upstream's KIP-929 "Observer Replicas" is a wiki page with a zero-length body. So the question became a research question: **how small can an open implementation of this third state be, and can every property be proven on real machines?**

## Iteration v0.1 — the 8-line hypothesis

**Hypothesis**: ISR membership is decided at a single choke point. If a replica can fetch normally but is refused at that choke point, everything else — HW, acks, elections — should follow from Kafka's own rules, because they are all defined *in terms of the ISR*.

Reading the 3.7.1 source located the choke point: `Partition.canAddReplicaToIsr()`, the only gate on the `maybeExpandIsr → AlterPartition` path. The entire first patch was ~8 lines ([archive](../patches/archive/archive-v0.1-canAddReplicaToIsr.py)): read an env var `KAFKA_OBSERVER_BROKER_IDS`, and if the fetching replica's id is in it, `return false`.

**Real-machine result**: it worked. The observer (broker 1, in the "slow" AZ) fetched all 30,000 test messages, stayed out of the ISR even after fully catching up, and — the decisive measurement — `acks=all` end-to-end latency stayed at the fast-pair level (2.35 ms avg / 4 ms P99), unaffected by the slow AZ. The config-only approach tested earlier had the cross-AZ replica *in* the ISR and dragged the HW; the patch achieved what no configuration can.

**Lesson 1**: *find the choke point before writing code.* The reason 8 lines suffice is that Kafka already derives everything from ISR membership; the design work was locating the one function where membership is granted, not inventing a parallel mechanism.

## v0.1 → v0.2: the initial-ISR vulnerability

Adversarial testing of v0.1 found the first hole: **create a *new* topic spanning the observer, and the observer is in the ISR from the start.** The controller's topic-creation path (`PartitionStateMachine.initializeLeaderAndIsrForPartitions`) stuffs *all live replicas* into the initial ISR directly — it never calls `maybeExpandIsr`, so the v0.1 gate never runs. Worse, the same reasoning exposed a second hole: with `unclean.leader.election.enable=true` and all ISR members dead, the unclean-election path (`offlinePartitionLeaderElection`) would happily elect the observer — a replica that, by design, can never re-enter the ISR, which would deadlock the partition.

**v0.2** ([archive](../patches/archive/archive-v0.2-hardening.py)) added two controller-side hooks: filter observers from the initial ISR (leader chosen from non-observers; fail-open if *all* live replicas are observers — never block topic creation), and exclude observers from unclean election (prefer `Leader: none` over electing an un-promotable leader).

**Real-machine result**: a freshly created topic showed `Isr: 2,3` from birth; killing both ISR members under unclean election yielded `Leader: none` — the observer was never chosen.

**Lesson 2**: *a gate on the steady-state path says nothing about the bootstrap path.* Initialization code frequently bypasses the machinery that maintains an invariant later. Enumerate every writer of the state you are gating (here: who ever *constructs* an ISR?), not just the writer you found first.

## v0.2 → v0.3: env vars are the wrong control plane

v0.2 was semantically complete but operationally wrong: the observer list lived in an environment variable, so changing it — the *promotion* operation, the entire point of having an observer during a disaster — required a broker restart. A DR mechanism that needs a restart in the middle of a disaster is a design smell.

**v0.3** ([archive](../patches/archive/v0.3-generator-snapshot.py)) moved the identity to a file (`/opt/kafka/observer.ids`) read through a new self-contained object `kafka.observer.ObserverIds`: 5-second TTL cache (the gate sits on the fetch hot path — no disk I/O per call), fail-safe reads (corrupt/missing file → keep last value + WARN, never crash a broker), env-var fallback for v0.1/v0.2 compatibility. It also added the piece that makes the file *bidirectional*: a demotion hook in `getOutOfSyncReplicas` that reports an in-ISR observer as lagging, so the native `isr-expiration` task shrinks it out — plus a gate in `maybeIncrementLeaderHW` closing a theoretical window where an observer inside the 30 s lag window could stall the HW (making "never drags HW" structural rather than empirical).

**Real-machine result**: promotion (delete id from file) ≤10 s, demotion (add it back) ≤10 s, both with zero restarts and zero data movement — the log was byte-identical all along ([evidence](../evidence/observer_v3_lifecycle_evidence.md)). EOS was verified at the byte level: per-batch CRCs identical across leader and observer over 5,001 batches, transaction markers copied verbatim, `read_committed` views identical ([evidence](../evidence/eos_byte_level_evidence.md), [evidence](../evidence/txn_read_committed_evidence.md)).

**Lesson 3**: *the control plane is part of the design.* The mechanism (v0.2) and the operable mechanism (v0.3) are different systems. "How does an operator flip this at 3 a.m. during an AZ outage?" is a first-class design input, and it dictated the cache TTL, the fail-open direction, and the reuse of native expand/shrink flows instead of any custom promotion RPC.

## v0.3 → v0.5: the KRaft probe, or why you test the port before designing it

The obvious question: does the ZK-mode patch work under KRaft? The tempting answer was to reason from source. Instead, a **probe** ran first: the existing patched jar on a pure-KRaft 3-node cluster, exercising each hook ([evidence](../evidence/kraft_probe_evidence.md)).

The probe split the patch cleanly in two:

- **Broker-side hooks (all 3): work unchanged.** `Partition.scala` is shared by both modes; `canAddReplicaToIsr` returning false prevents the AlterPartition request from ever being sent.
- **Controller-side hooks (both): silently dead.** A new topic's initial ISR *included* the observer — measured, not inferred. The ZK controller (`PartitionStateMachine`) simply never executes in a KRaft process; the quorum controller lives in the `metadata` module, in Java.

**v0.5** rebuilt the controller side where it actually runs: `ObserverReplicas.java` (same file, same cache semantics, Java) plus three hooks in `ReplicationControlManager` — initial-ISR filter in `buildPartitionRegistration`, a one-line gate in `LeaderAcceptor.test` that covers **all seven election entry points including unclean** (the ZK version needed two hooks for less coverage), and an AlterPartition rejection (`INELIGIBLE_REPLICA "observer"`) as defense-in-depth. The full 8-item capability matrix passed on real machines; promotion measured 4 s, demotion 9 s ([evidence](../evidence/kraft_controller_patch_evidence.md)). The probe also surfaced two operational asymmetries that documentation now carries: KRaft has *no* new-topic blind spot (brokers read the metadata log — a win over ZK mode), but demoting an observer that is currently a *leader* never takes effect hot (no ZK-style re-election path — move leadership first).

**Lesson 4**: *probe before you port.* One afternoon of running the old code in the new mode replaced a design document's worth of speculation with two measured facts — which hooks were free, and which were dead — and the dead ones failed in a way source reading alone would likely have mis-scoped (the KRaft controller isn't a different implementation of the same class; it is a different module in a different language).

## v0.5 → v0.6: the ELR worry, resolved structurally

Kafka 4.x brought two concerns. First, the mechanical port: would the patch survive the removal of ZooKeeper? Answer: cleanly — the 8 usable hunks applied to 4.0.0 with line-number drift only, zero hand edits, and the 4.1.0 patch is byte-identical ([evidence](../evidence/kafka40_port_evidence.md)).

Second, the real worry: **ELR (KIP-966)**, default-on in new 4.1 clusters, adds a *new election candidate set* — exactly the kind of mechanism that could resurrect the v0.1-era vulnerability in a new place (an observer becoming electable through a path nobody gated). The v0.4 design had pencilled in a defensive hook in `maybePopulateTargetElr`.

Source analysis suggested the hook was unnecessary — the ELR candidate set is built from `ELR ∪ ISR`, and observers never enter the ISR, so they can never enter ELR — but after the initial-ISR lesson, "suggested by source reading" was not the bar. v0.6 verified it on real clusters, both on 4.0.0 (ELR manually enabled) and 4.1.0 (default-on): kill-ISR sequences never showed the observer in `Elr:` or `LastKnownElr:`, and it was never elected even with unclean election enabled ([evidence](../evidence/elr_verification_evidence.md)). The guarantee is **structural** — it holds because of what ELR is built from, not because a fourth gate catches it — and the patch touches no ELR code. As a side effect, the verification confirmed a genuine upstream bug in `canElectLastKnownLeader` (recorded as a suspicion during v0.4 source reading, fixed upstream in 4.1.0 as KAFKA-19522) — it has no observer pathway but can mis-elect a fenced ordinary broker on 4.0 with ELR on.

**Lesson 5**: *the strongest safety argument is structural, and you still test it.* "Observers never enter ELR because ELR is derived from ISR" is better than any added filter — no code to maintain, no new anchor to drift. But it earned that status only after real-cluster kill sequences failed to falsify it. And instrumenting a system deeply enough to verify your own change tends to find upstream's bugs too.

## What the arc adds up to

| Iteration | Change | Triggered by |
|---|---|---|
| v0.1 | 8-line gate in `canAddReplicaToIsr` | The hypothesis that ISR membership is a single choke point |
| v0.2 | + initial-ISR filter, + unclean-election exclusion | Adversarial test: new topics bypassed the gate |
| v0.3 | env var → file, + demotion hook, + HW gate | Operational review: promotion must not need a restart |
| v0.5 | + KRaft controller side (Java, `metadata` module) | Probe: measured that both ZK controller hooks are dead under KRaft |
| v0.6 | 4.0/4.1 port; ELR verified safe **with zero new code** | ELR default-on threat model; structural argument, then real-cluster falsification attempts |

The method, compressed: *locate the choke point → gate it minimally → attack your own patch → make it operable → probe every new execution environment instead of reasoning about it → prefer structural guarantees, but only after trying to break them.* Each iteration's patch script is preserved in [`patches/archive/`](../patches/archive/) so the failures are auditable, not just the successes.
