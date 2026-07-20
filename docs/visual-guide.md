# Visual guide — every diagram in one place

All diagrams are self-contained animated SVGs (SMIL). GitHub renders the animations natively — no plugins, no video files. Each entry below tells you **how to read the animation** and links to the document that explains it in depth.

> Tip: the animations loop. If you open one mid-cycle, wait for the big phase banner at the top to return to PHASE/STEP 0.

---

## Architecture

### Global architecture

<p align="center">
  <img src="diagrams/architecture.svg" alt="Global architecture — 3 AZs, leader + ISR follower + observer; HW advances on ISR only; promotion via observer.ids" width="100%">
</p>

**How to read it:** one Kafka cluster spanning 3 AZs. The leader and ISR follower (left) form the acks quorum; the observer (right, dashed purple) runs the same fetch protocol and holds a byte-identical log, but the ISR-boundary gates keep it out of the quorum. The high-watermark advances on ISR replicas only, so the observer can never slow a write. Deep dive: [architecture.md](architecture.md).

### ZooKeeper vs KRaft

<p align="center">
  <img src="diagrams/zk-vs-kraft.svg" alt="ZooKeeper vs KRaft — shared broker-side hooks in Partition.scala, mode-specific controller hooks" width="100%">
</p>

**How to read it:** the broker-side hooks (center) are byte-identical across both modes and all supported versions; only the controller-side hooks differ — Scala state machine in ZK mode, `ObserverReplicas.java` + RCM hooks in KRaft. Full hook matrix and 4.x port analysis: [multi-version.md](multi-version.md).

### Promotion sequence (mechanism view)

<p align="center">
  <img src="diagrams/promotion-flow.svg" alt="Promotion sequence — file edit, 5 s cache refresh, fetch triggers maybeExpandIsr, canAddReplicaToIsr opens, AlterPartition, ISR expands, election eligibility restored" width="100%">
</p>

**How to read it:** left to right, the causal chain from one file edit to full election eligibility. The only non-native step is the file edit; everything after — cache refresh, fetch, `canAddReplicaToIsr()`, AlterPartition, ISR expand — is stock Kafka machinery. Measured end-to-end: ≤10 s. Details: [architecture.md](architecture.md).

### Demotion sequence (mechanism view)

<p align="center">
  <img src="diagrams/demotion-flow.svg" alt="Demotion sequence — id written back, isr-expiration task, getOutOfSyncReplicas hook, native shrink, out of ISR" width="100%">
</p>

**How to read it:** the reverse path — write the id back, the periodic `isr-expiration` task hits the `getOutOfSyncReplicas` hook, native ISR shrink pushes the broker out (measured ≤10–20 s). Replication continues; only electability is removed. Details: [architecture.md](architecture.md).

### Why exactly-once survives (vs MirrorMaker 2)

<p align="center">
  <img src="diagrams/eos-comparison.svg" alt="Replication comparison — observer byte-copy keeps one offset space and identical CRCs; MM2 consume-reproduce breaks the offset space and duplicated 20000 messages" width="100%">
</p>

**How to read it:** top path — the observer byte-copies leader batches, so offsets, PIDs, epochs, sequences and txn markers are preserved (per-batch CRCs identical). Bottom path — MM2 consumes and re-produces, creating a new offset space; the control experiment produced 20 000 duplicates under the same failure. Full analysis: [eos-semantics.md](eos-semantics.md).

---

## Lifecycle stories

### Promotion — observer becomes electable

<p align="center">
  <img src="diagrams/story-promotion.svg" alt="Animated promotion story — steady state, operator removes id from observer.ids, cache refresh opens canAddReplicaToIsr gate, native ISR expansion pulls the observer in (~4-10 s measured), electable with zero restart and zero data movement" width="100%">
</p>

**How to read it:** 16 s loop, 5 steps. Watch the ISR boundary box — it physically expands to swallow broker 3 in STEP 3. Every step after the file edit is native Kafka; measured promotion 4–10 s, zero restart, zero data movement. Timing analysis: [timing-and-automation.md](timing-and-automation.md).

### Demotion — electable steps back to observer

<p align="center">
  <img src="diagrams/story-demotion.svg" alt="Animated demotion story — operator adds the id back to observer.ids, isr-expiration tick runs the getOutOfSyncReplicas hook, native ISR shrink pushes the broker out (~9-12 s measured), replication continues as an observer" width="100%">
</p>

**How to read it:** the mirror image of promotion — the ISR boundary contracts. Measured 9–12 s. Note the warning phase: if the broker is currently the **leader**, move leadership first (the native shrink never removes a leader). Details: [timing-and-automation.md](timing-and-automation.md).

### Observer crash — the failure that does not matter

<p align="center">
  <img src="diagrams/story-observer-crash.svg" alt="Animated observer-crash story — observer dies, ISR/leader/writes/latency (2.0 ms) all unchanged, observer restarts and catches up on its own, log byte-identical after reconnect; zero impact on writes" width="100%">
</p>

**How to read it:** the observer dies and the top row of indicators (ISR, leader, writes, latency) never changes — that is the whole point of sitting outside ISR. It restarts, catches up on its own, and the log is byte-identical after reconnect. Details: [timing-and-automation.md](timing-and-automation.md).

---

## Failure stories

### Scenario A — one primary AZ lost

<p align="center">
  <img src="diagrams/story-az-loss.svg" alt="Looping story animation — steady state, AZ loss and fail-stop, operator file edit, observer promoted into ISR in about 9 seconds, failed AZ returns and the observer demotes back; RPO 0 and the leader never changed" width="100%">
</p>

**How to read it:** the fail-stop is the feature — writes stop with `NOT_ENOUGH_REPLICAS` instead of silently losing data. One file edit later the observer is in ISR (~9 s) and writes resume with RPO = 0. When the AZ returns, the observer demotes back. Runbook: [runbooks/scenario-a-az-loss.md](runbooks/scenario-a-az-loss.md) · playbook: [scenario-playbook.md](scenario-playbook.md).

### Scenario B — all primary replicas lost

<p align="center">
  <img src="diagrams/story-total-loss.svg" alt="Looping story animation — both primaries killed, Leader: none because the un-promoted observer refuses to take over even unclean, operator promotes via file edit plus explicit unclean election, promoted observer elected leader in 9.4 seconds and verified with real writes" width="100%">
</p>

**How to read it:** the key frame is `Leader: none` — the un-promoted observer refuses to take over **even with unclean election enabled**. Only an explicit promote + election makes it lead (9.4 s measured, verified with real writes). Safety by default, capability on demand. Runbook: [runbooks/scenario-b-total-loss.md](runbooks/scenario-b-total-loss.md).

### observer.ids fail-safe — three injections, zero casualties

<p align="center">
  <img src="diagrams/story-file-failsafe.svg" alt="Looping story animation, three parallel injections against observer.ids — chmod 000 keeps the last cached value with a WARN, garbage content is dropped by the parser, inconsistent copies are fenced by the controller with INELIGIBLE_REPLICA and self-heal in 5.8 seconds once aligned; the file can never take a broker down" width="100%">
</p>

**How to read it:** three parallel attack columns against the control file — unreadable (`chmod 000`), garbage content, inconsistent copies. Each degrades safely: cached value + WARN, parser drop, controller fence (`INELIGIBLE_REPLICA`) that self-heals in 5.8 s. The file can never take a broker down. Evidence: [scenario-playbook.md](scenario-playbook.md).

---

## Automation stories

### Auto-promoter — the watchdog cycle

<p align="center">
  <img src="diagrams/story-auto-promoter.svg" alt="Animated auto-promoter story — daemon radar scans every 10 s, detects ISR below minISR when a follower dies, verifies observer lag is 0, atomically edits observer.ids with a full audit-log trail, native ISR expand restores writes in ≤14 s total, then demotes what it promoted once the follower recovers" width="100%">
</p>

**How to read it:** 22 s loop, upper half is the cluster, lower half the external daemon. Follow the radar sweep (green → red on detect), the lag check, the atomic file edit, and the audit log filling in on the right. Fault → detect → promote → writes resumed: ≤14 s total, no human. It later **demotes only what it promoted**. Design and safety rules: [auto-promotion.md](auto-promotion.md).

### Three operational modes — Manual / Auto / Hybrid

<p align="center">
  <img src="diagrams/story-three-modes.svg" alt="Animated comparison — the same follower failure replayed in three columns: Manual (alert, human, runbook script, RTO = human + 9 s), Auto (daemon detects and edits the file, RTO ≤ 14 s), Hybrid (daemon detects fast in dry-run, human confirms, daemon executes); same observer.ids file mechanism underneath" width="100%">
</p>

**How to read it:** the same fault replays simultaneously in three columns. Manual trades RTO for determinism (recommended for financial); Auto delivers RTO ≤ 14 s; Hybrid detects fast and decides deliberately. The punchline is the bottom banner: whichever mode you pick, the mechanism underneath is the same one-line file. Mode comparison: [timing-and-automation.md](timing-and-automation.md).

### Dry-run — watch it think before you let it act

<p align="center">
  <img src="diagrams/story-dryrun.svg" alt="Animated dry-run story — daemon detects a real fault but only writes a would-promote log line, the cluster mutation path is visibly blocked, a human reviews the decision trace, flips -n to -e, and the same detection then performs a real promotion" width="100%">
</p>

**How to read it:** in dry-run (`-n`) the daemon runs the full decision path on real faults but the arrow to the cluster is **blocked** — only the log line lands. A human reviews the trace ("would it have promoted at the right moment?"), flips one flag, and the identical detector gains real hands. This is week 1 of the recommended rollout SOP. Details: [auto-promotion.md](auto-promotion.md) · rollout timeline: [timing-and-automation.md](timing-and-automation.md).
