# Failure-scenario playbook — every experiment, what happened, where the raw output is

This is the index of all failure and semantics experiments executed against real clusters (EC2 Tokyo, 3 AZs — plus the local Docker environment for ZK topic-creation semantics). Each row states the exact perturbation, the observed outcome, and the evidence file with raw command output. Operator procedures live in the runbooks; this page is the *what we actually did and saw* ledger.

## Availability & election experiments

| # | Scenario | Setup | Perturbation | Observed outcome | Evidence / runbook |
|---|---|---|---|---|---|
| 1 | **One primary AZ lost** (Scenario A) | ZK 3.7.1, primaries 2@1c+3@1d, observer 1@1a, minISR=2, acks=all | Stop the ISR follower's broker | ISR shrinks below minISR → producers get `NOT_ENOUGH_REPLICAS` (fail-stop, no wrong acks); delete observer id from file → observer joins ISR ≤10 s → writes resume; RPO = 0, zero data movement | [runbook A](runbooks/scenario-a-az-loss.md) · [lifecycle evidence](../evidence/observer_v3_lifecycle_evidence.md) |
| 2 | **All primary replicas lost, observer un-promoted** (Scenario B, refusal half) | Same; `unclean.leader.election.enable=true` | `kill` broker 2 **and** 3 (the whole ISR) | `Leader: none` — the surviving observer is **never** elected, even unclean. By design: an un-promotable leader would deadlock the partition | [runbook B](runbooks/scenario-b-total-loss.md) |
| 3 | **All primary replicas lost, observer promoted** (Scenario B, takeover half) | Same | Kill 2+3, then remove observer id from the file | Observer enters ISR, is elected `Leader: 1, Isr: 1`; test message produced and consumed back successfully | [runbook B](runbooks/scenario-b-total-loss.md) |
| 4 | **Unclean-election refusal, KRaft 4.0** | 6-node (3 controller + 3 broker), 4.0.0 patched | Kill all ISR members, observer alive, unclean=true | `Leader: none`; recovery after broker restart with zero data loss | [4.0 port evidence](../evidence/kafka40_port_evidence.md) |
| 5 | **ELR interaction, 4.0 (manually enabled) & 4.1 (default-on)** | 6-node KRaft, observer=3, minISR=2 | Kill ISR members one by one until ISR empty | Crashed *ordinary* members enter `Elr:` and later recover with a **clean** election (zero data loss); the observer **never** appears in `Elr:`/`LastKnownElr:` and is never elected — structural (`ELR ⊆ ELR ∪ ISR`, observer ∉ ISR) | [ELR evidence](../evidence/elr_verification_evidence.md) |

## Lifecycle experiments (promotion / demotion)

| # | Scenario | Mode | Observed outcome | Evidence |
|---|---|---|---|---|
| 6 | Promote by deleting id from `observer.ids` | ZK 3.7.1 | In ISR ≤10 s, zero restart, zero data movement | [lifecycle](../evidence/observer_v3_lifecycle_evidence.md) |
| 7 | Demote by adding id back | ZK 3.7.1 | Native `isr-expiration` shrinks it out ≤10 s | [lifecycle](../evidence/observer_v3_lifecycle_evidence.md) |
| 8 | Promote / demote under KRaft | KRaft 3.7.1 (v0.5) | Promotion **4 s**, follower demotion **9 s**; promoted observer elected and served 200 verified writes | [controller patch evidence](../evidence/kraft_controller_patch_evidence.md) |
| 9 | Promote / demote on Kafka 4.0 | KRaft 4.0.0 (v0.6) | Promotion ~4 s, follower demotion ~12 s; preferred election after promotion works | [4.0 port evidence](../evidence/kafka40_port_evidence.md) |
| 10 | **Demote a *leader* observer** (negative result) | KRaft | Does **not** take effect hot — the leader never self-removes from ISR and KRaft has no ZK-style re-election path. SOP: move leadership first, or restart that broker once | [multi-version](multi-version.md) · [runbook A pre-checks](runbooks/scenario-a-az-loss.md) |
| 11 | AlterPartition defense-in-depth | KRaft | An observer whose broker-side gate is open but controller-side file lags is rejected `INELIGIBLE_REPLICA "observer"` — fails safe | [controller patch evidence](../evidence/kraft_controller_patch_evidence.md) |

## Consistency & EOS experiments

| # | Scenario | Observed outcome | Evidence |
|---|---|---|---|
| 12 | Byte-level log comparison, leader vs observer | Per-batch CRC identical across all 5,001 batches; `producerId`/epoch/sequence/offsets byte-identical — `appendAsFollower` copies, never re-produces | [EOS byte-level](../evidence/eos_byte_level_evidence.md) |
| 13 | Transactions + `read_committed` on the observer | 15 committed messages visible, 3 aborted invisible — identical view on leader and observer (txn markers travel inside the copied bytes) | [txn evidence](../evidence/txn_read_committed_evidence.md) |
| 14 | **MM2 control group** — same failure, consume→re-produce architecture | `kill -9` MM2 inside the offset-flush window → restart → offsets reset to 0 → **20,000 duplicate messages** at the target (target PID=-1: no idempotence, no dedup basis). The structural contrast to rows 12–13 | [MM2 duplicates](../evidence/mm2_duplicate_evidence.md) |

## Environment & compatibility experiments

| # | Scenario | Observed outcome | Evidence |
|---|---|---|---|
| 15 | **KRaft probe** — run the ZK patch unmodified under KRaft | Broker-side hooks work; both ZK controller hooks measured **dead** (new topic's initial ISR included the observer). This experiment triggered the v0.5 controller-side rewrite | [KRaft probe](../evidence/kraft_probe_evidence.md) |
| 16 | ZK-mode new-topic blind spot | A running observer never receives a new topic's assignment (controller notifies ISR members only); learns it on restart / controller failover. KRaft verified free of this issue | [architecture § known behavior](architecture.md#known-behavior-notes) · [deployment](deployment.md) |
| 17 | Multi-version apply + compile | Canonical 3.7.1 patch applies and compiles on 3.6.2 / 3.8.1 / 3.9.1 (strict direct application, no 3-way rescue); weekly CI drift sentinel | [multiversion evidence](../evidence/multiversion_apply_evidence.md) |
| 18 | Kafka 4.0 / 4.1 port | 8/8 usable hunks apply with line-number drift only, zero hand edits; 4.1 patch byte-identical to 4.0 | [4.0 port evidence](../evidence/kafka40_port_evidence.md) |
| 19 | Latency under observer-in-slowest-AZ | `acks=all` 2.04–2.35 ms — equal to the fast-pair baseline; the HW never waited for the observer | [POC report (中文)](zh/POC验证报告.md) |

## Re-running these experiments

- **Local**: `cd docker && docker compose up -d && ./demo.sh` (lifecycle, rows 6–7) — [docker/README.md](../docker/README.md).
- **Automated**: the pytest suite covers rows 1–3 semantics non-destructively and, with `--destructive`, broker-kill scenarios — [test/README.md](../test/README.md) and [testing.md](testing.md).
- **Real hardware**: [terraform/](../terraform/README.md) recreates the exact Tokyo 3-AZ topology that produced every number above.

---

# The S1–S8 scenario matrix (KRaft, executed 2026-07-20)

A single sitting, one cluster, all eight failure classes in sequence — so the numbers are directly comparable. Every scenario below follows the same template: **Setup → Inject → Observe (measured output) → Conclusion → Recovery SOP**.

**Environment**: Kafka 3.7.1 + combined observer patch (`patches/kafka-3.7.1-kraft/`), KRaft with a **dedicated 3-node controller quorum** (ids 101–103) plus **4 brokers** (ids 1–4), on the Tokyo loadgen EC2 host (single machine, multi-process — timings are therefore best-case network-wise, but all *semantics* are placement-independent). Observer = broker 3; each node has its own `observer.ids` file so per-node inconsistency can be injected (S6/S7). `replica.lag.time.max.ms=10000`, `min.insync.replicas=2` unless stated. Topics:

- `sm` — RF 3, `Replicas: 3,1,2` → **2 primaries + observer** (the stretched "2+1" layout)
- `smx` — RF 4, `Replicas: 3,1,2,4` → **3 primaries + observer**

Raw output for every number: [`evidence/scenario_matrix_evidence.md`](../evidence/scenario_matrix_evidence.md).

## Scenario index

| # | Scenario | Injection | Key measured result | Writes during fault | Recovery |
|---|---|---|---|---|---|
| [S1](#s1-leader-broker-offline-non-observer) | Leader broker dies (non-observer) | `kill -9` leader | New leader from ISR in **10.4 s**; observer never a candidate | ✅ continue (3-primary layout)¹ | restart → ISR rejoin **3.8 s** |
| [S2](#s2-follower-broker-offline) | Follower broker dies | `kill -9` follower | ISR shrinks in **10.3 s**, leader unchanged | ✅ 300/300 acked | restart → ISR rejoin **3.9 s** |
| [S3](#s3-observer-offline) | Observer dies | `kill -9` observer | ISR untouched; acks=all p50 19 ms → **2 ms** (zero impact) | ✅ zero impact | restart → md5-identical catch-up |
| [S4](#s4-both-primaries-down--observer-promotion) | **All primaries down** | `kill -9` both primaries | `Leader: none` until promoted; promote + unclean elect → leader in **9.4 s** | ❌ fail-stop, then per branch | both minISR branches verified; full un-promote SOP |
| [S5](#s5-promotion-while-the-observer-is-lagging) | Promote a lagging observer | freeze observer, +30 k records, restart+promote | ISR admission **waits for catch-up** (5.4 s / ~15 MB); HW never regresses | ✅ unaffected | hot demote **3.5 s** |
| [S6](#s6-inconsistent-observerids-across-nodes) | `observer.ids` split between brokers & controllers | clear broker files only | Controller rejects AlterPartition `INELIGIBLE_REPLICA [3 (observer)]`; ISR never moves | ✅ unaffected | align files → self-heal **5.8 s**, no restart |
| [S7](#s7-observerids-corrupted--unreadable) | File `chmod 000` / garbage / deleted | 3 sub-injections | Broker survives, WARNs, keeps last cached set; garbage tokens ignored | ✅ 100/100 acked each time | restore file; nothing to restart |
| [S8](#s8-controller-failover) | Active controller dies | `kill -9` active controller | New controller in **3.7 s**; new topic still filters observer from initial ISR (`[3,1,2] → [1,2]`) | ✅ 200/200 acked | restart → rejoins quorum, lag 0 |

¹ On the 2-primary layout (`sm`) the same kill drops ISR to 1 < minISR 2 and writes **fail-stop** (`acked: 0`) — the intended Scenario-A posture; see the S1 footnote.

Two structural takeaways this matrix proves end-to-end:

1. **Write-path bypass, data-path fidelity**: killing the observer changes nothing — not even tail latency (S3) — yet its log stays byte-identical to the leader's (segment md5 equal, S3/S4).
2. **Every mis-operation fails safe**: file inconsistency or corruption can only *keep* a replica out of the ISR, never wrongly admit or elect it (S6/S7); enforcement survives controller failover with no handover gap (S8).

## S1. Leader broker offline (non-observer)

**Setup**: topic `smx` (`Replicas: 3,1,2,4`, observer = 3, minISR = 2), leader = broker 1, `Isr: 1,2,4`. Baseline write 500/500 acked.

**Inject**: `kill -9` broker 1.

**Observe** (measured):

```
new leader: broker 2 after 10437 ms (observer is 3 — must never be it)
    Topic: smx  Partition: 0  Leader: 2  Replicas: 3,1,2,4  Isr: 2,4
```

Failover time ≈ `replica.lag.time.max.ms` (10 s) — the broker-liveness timeout, exactly as in vanilla Kafka. The new leader came from the surviving ISR, **not** the fully-caught-up observer. Degraded-state write: 300/300 acked (ISR 2 ≥ minISR 2).

**Conclusion**: leader failover is stock Kafka; the patch only removes the observer from candidacy. Nothing about failover timing changes.

**Recovery SOP**: restart the dead broker — no ISR action needed; it rejoined the ISR **3 827 ms** after the restart command. Optionally `kafka-leader-election.sh --election-type preferred` to move leadership back.

**Footnote — the 2+1 layout**: the same experiment run first on `sm` (`Replicas: 3,1,2`, only two ISR-eligible members) left `Isr: 2` after the kill and the post-failover write returned `acked: 0` (`NOT_ENOUGH_REPLICAS`). That is intended **fail-stop**: with 2 primaries + observer, any single primary loss halts acks=all writes until the primary returns or you run [runbook A](runbooks/scenario-a-az-loss.md). If you must ride out single-broker loss with no operator action, deploy 3 primaries + observer.

## S2. Follower broker offline

**Setup**: topic `smx`, leader = 2, `Isr: 2,4,1`. Target: follower 4 (in ISR, not leader, not observer).

**Inject**: `kill -9` broker 4.

**Observe** (measured):

```
ISR shrank (4 removed) after 10346 ms
    Topic: smx  Partition: 0  Leader: 2  Replicas: 3,1,2,4  Isr: 2,1
```

Leader unchanged; write during the shrunken window **300/300 acked** (ISR 2 ≥ minISR 2).

**Conclusion**: identical to vanilla Kafka; the observer plays no role in follower loss.

**Recovery SOP**: restart the broker; it auto-rejoined the ISR **3 874 ms** later via native `maybeExpandIsr` — zero operator ISR action.

## S3. Observer offline

**Setup**: topic `smx`, `Isr: 2,1,4`, observer 3 fully caught up. Baseline acks=all (1 000 × 200 B @ 500 rec/s): **p50 19 ms, p99 223 ms**.

**Inject**: `kill -9` broker 3 (the observer).

**Observe** (measured):

- ISR **unchanged** (`Isr: 2,1,4`) — the observer was never in it; there is nothing to shrink.
- acks=all latency *during* the outage: **p50 2 ms, p99 219 ms** — nothing got worse (the baseline delta is JVM warm-up noise).
- The leader advanced the HW freely to offset 3 100 with the observer dead.

**Conclusion**: observer loss is invisible to producers and to the ISR — the payoff of the two broker-side gates (`canAddReplicaToIsr` + `maybeIncrementLeaderHW`): the observer is simply not on the acknowledgement path.

**Recovery SOP**: restart the observer; it catches up through the normal replica fetcher. Post-catch-up proof of byte fidelity:

```
532065 bytes  leader   segment — md5 448a44d4c46c95c584315a4255c1b316
532065 bytes  observer segment — md5 448a44d4c46c95c584315a4255c1b316
```

Same size, same md5, same last offset (3 099). No ISR action needed or possible.

## S4. Both primaries down → observer promotion

The full [runbook-B](runbooks/scenario-b-total-loss.md) disaster flow, with **both** minISR end-games exercised.

**Setup**: topic `sm` (`Replicas: 3,1,2`, observer = 3, minISR = 2), leader = 2, `Isr: 2,1`. Pre-kill proof: leader and observer segment md5 **equal** (`0d5f9552…` on both).

**Inject**: `kill -9` brokers 1 **and** 2 simultaneously.

**Observe — exclusion holds under total ISR loss**:

```
Topic: sm  Partition: 0  Leader: none  Replicas: 3,1,2  Isr: 1
```

Broker 3 is alive, byte-identical, and **still not elected** — the election predicate refuses observers on every path, including "the observer is the only thing left". All 50 write attempts failed. No silent takeover, ever.

**Observe — promotion**: remove `3` from `observer.ids` on **all** nodes (controllers included — the controller copy is what gates election). The ISR still names a dead broker, so a clean election is impossible; an **unclean** election is required — safe *here* because the md5 check proved zero lag:

```
Successfully completed leader election (UNCLEAN) for partitions sm-0
leader after promotion+election (9353 ms since file edit):
    Topic: sm  Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3
```

**9.4 s** from file edit to serving leader (5 s file-cache TTL + election), zero restarts, zero data movement.

**Observe — Branch B (durability-first: stay read-only)** with `Isr: 3` (1 < minISR 2): reads work (consumed offsets 0–4 from the promoted leader ✅); acks=all writes refused with `NotEnoughReplicasException` ✅. Correct posture if you expect the primaries back and refuse to lower durability.

**Observe — Branch A (availability-first: compromise minISR)**: `--add-config min.insync.replicas=1` → **200/200 acked** on the single surviving replica. You now run with **zero redundancy** — every record accepted in this window lives on one machine. Make that decision consciously; restore minISR the moment a second replica is back.

**Conclusion**: the whole Scenario-B chain is operator-gated at every step — no auto-takeover, promotion is a file edit, election is explicit, the durability trade-off is an explicit config change — and both branches behave exactly as documented.

**Recovery SOP** (executed): ① restore `min.insync.replicas=2`; ② restart the primaries — they rejoined the ISR **4.7 s** later, truncating to the promoted leader's log (which lost nothing): `Isr: 3,1,2`; ③ re-demote 3 by adding it back to every `observer.ids`. **KRaft rule**: a replica that is *currently leader* does not hot-demote — move leadership first or bounce that broker once (we bounced broker 3). Final steady state `Leader: 1, Isr: 1,2`.

## S5. Promotion while the observer is lagging

**Setup**: topic `smx`, leader = 4, `Isr: 4,1,2`. Manufacture lag: `kill -9` the observer, then pump **30 000 × 500 B records** (≈15 MB; leader LEO → 34 100) while it is down.

**Inject**: promote 3 (clear all `observer.ids`) and restart it **in the same moment** — the worst realistic race: promoting on stale health data.

**Observe** (measured):

```
replica 3 entered ISR after 5383 ms from restart+promotion
broker 4 (leader):   baseOffset 34099 lastOffset 34099
broker 3 (promoted): baseOffset 34099 lastOffset 34099
```

The promoted replica did **not** appear in the ISR until it had replayed the entire 30 k-record backlog — native `maybeExpandIsr` only proposes a replica that `isCaughtUp`. **The HW never went backwards; in-flight acks=all writes were never exposed to the laggard.** Post-admission write: p50 6 ms.

**Conclusion**: lag + promotion is harmless **while a leader is alive** — ISR admission simply waits (5.4 s for 15 MB on localhost; scale by your lag and network). The dangerous combination is S4's, not this one: **lag + promotion + unclean election** makes the laggard's LEO the truth and permanently loses every record past it, and nothing in Kafka will stop you. Hence the hard runbook-B pre-check: **compare observer LEO to last-known-leader LEO before promoting for takeover** (`kafka-get-offsets.sh` per broker). Promoting a lagging observer for takeover is an explicit RPO > 0 decision.

**Recovery SOP**: demotion of the now-follower was hot: add 3 back to `observer.ids` → `isr-expiration` shrank it out in **3.5 s**, no restart.

## S6. Inconsistent `observer.ids` across nodes

The file is per-node local state — what happens in the window when brokers and controllers disagree?

**Setup**: all nodes agree `3` is observer; `Isr: 4,1,2` on `smx`.

**Inject**: clear `observer.ids` on **all broker nodes only** (simulates a botched promotion where the operator forgot the controller quorum). Brokers now think 3 is promotable; the 3 controllers still think it is an observer.

**Observe** (measured, 30 s window): the leader-side gate opens and proposes ISR expansion; the controller **refuses, repeatedly**:

```
INFO [QuorumController id=101] Rejecting AlterPartition request from node 4 for smx-0
  because it specified ineligible replicas [3 (observer)] in the new ISR [...]
```

and the leader broker, symmetrically:

```
INFO [Partition smx-0 broker=4] Failed to alter partition to PendingExpandIsr(newInSyncReplicaId=3, ...)
  since the controller rejected the request with INELIGIBLE_REPLICA.
  Partition state has been reset to the latest committed state ... isr=Set(4, 1, 2)
```

The committed ISR **never changed** throughout. The retry loop is INFO-level, cheap, stable — and those log lines are your alert signal. The reverse split (controller promoted, broker not) cannot wrongly admit anyone either: the broker gate simply never proposes the replica.

**Conclusion**: two-sided enforcement (broker gate + controller `INELIGIBLE_REPLICA` check) makes file inconsistency **fail-safe by construction** — the disagreement can only keep a replica *out* of the ISR, never let one in. A forgotten node delays a promotion; it cannot corrupt ISR semantics.

**Recovery SOP**: align the files (here: cleared the controller copies too) → self-healed with **zero restarts**, replica 3 admitted **5 783 ms** after the controller files changed. Operational rule: treat `observer.ids` as config deployed atomically to all nodes, **controllers first, then brokers** — that ordering keeps you inside the safe holding pattern.

## S7. `observer.ids` corrupted / unreadable

**Setup**: topic `smx`, `Isr: 4,1,2`, observer 3. Three sub-injections against the partition leader's (broker 4) and active controller's file copies.

**Inject 7a — `chmod 000`** (permission denied). Observe:

```
WARN Failed to read observer ids from .../obs-broker4.ids,
     keeping last value Set(3) (kafka.observer.ObserverIds$)
```

Broker alive (verified), ISR unchanged, last cached value kept — the documented never-throws contract of `ObserverIds.current()`. Writes during the fault: **100/100 acked**.

**Inject 7b — garbage content** (`banana,3`, `%%@@!!`, comments, stray whitespace). Observe: non-numeric tokens silently dropped, the parseable `3` survives → set stays `Set(3)`, **no flap** (the `Observer id set changed` log shows no garbage-induced transition). ISR unchanged; 100/100 acked.

**Inject 7c — file deleted** (falls back to `KAFKA_OBSERVER_BROKER_IDS` env; unset ⇒ empty set ⇒ broker-side would allow promotion). Observe: deleted on one broker only ⇒ the **S6 fail-safe** takes over — controllers still have `3`, ISR stays `4,1,2`. One lost file cannot promote anyone.

**Conclusion**: every file failure mode is absorbed — permission errors freeze the last known state, garbage degrades to whatever parses, deletion degrades to env-fallback and is still fenced by the controller. No injection crashed a process or moved the ISR. The one genuinely dangerous shape is **cluster-wide simultaneous deletion** (all nodes fall back to empty = mass promotion); protect the path with ordinary fs permissions and deploy tooling.

**Recovery SOP**: restore file content/permissions; the 5 s cache picks it up on the next tick — nothing to restart. Alert on the `Failed to read observer ids` WARN: it is the only sign of a wedged control file (state is otherwise frozen-safe).

## S8. Controller failover

KRaft observer enforcement lives in the controller (`ObserverReplicas` + 3 `ReplicationControlManager` hooks). Does it survive a controller handover — especially for *new* topics?

**Setup**: dedicated quorum 101/102/103, active = 101. Every controller node has its own `observer.ids` (the deployment rule exists precisely because any of them can become active).

**Inject**: `kill -9` controller 101 (the active one).

**Observe** (measured):

- New active controller (103) in **3 722 ms**; brokers unaffected.
- Existing partitions keep exclusion: `sm` → `Isr: 1,2`, `smx` → `Isr: 4,1,2` (3 absent from both).
- **New topic under the new controller** (`sm-postfailover`, assignment `3:1:2`) — initial ISR filtered on the spot:

```
Topic: sm-postfailover  Partition: 0  Leader: 1  Replicas: 3,1,2  Isr: 1,2
INFO Filtered observers [3] from initial ISR [3, 1, 2] -> [1, 2]
     (org.apache.kafka.controller.ObserverReplicas)     # logged by controller 103
```

- Preferred-leader election (preferred replica = 3, the observer) under the new controller: refused — `PreferredLeaderNotAvailableException`.
- Write path healthy after the failover: 200/200 acked.

**Conclusion**: observer semantics survive controller failover with **no gap and no handover state** — every controller evaluates the same local file through the same code. The operational requirement this proves: **ship `observer.ids` to every controller node, not only brokers.** A controller missing the file enforces nothing when it becomes active (env-fallback/empty — and per S6, brokers alone can block ISR admission but not an election).

**Recovery SOP**: restart the old controller; it rejoined as a follower voter (`CurrentVoters: [101,102,103]`, `MaxFollowerLag: 0`). No move-back needed — controller leadership is not placement-sensitive.

## Cross-scenario operator cheat sheet

| Symptom | Likely scenario | First command | Then |
|---|---|---|---|
| `Leader: none`, observer alive | S4 | compare observer LEO vs last-known leader LEO (`kafka-get-offsets.sh`) | lag 0 → promote + unclean elect; lag > 0 → explicit RPO decision |
| acks=all fail `NOT_ENOUGH_REPLICAS`, leader alive | S1-on-2+1 / S4-branch-B | `kafka-topics.sh --describe` (ISR count vs minISR) | wait for primary, or promote observer ([runbook A](runbooks/scenario-a-az-loss.md)) |
| `Rejecting AlterPartition ... [N (observer)]` repeating | S6 | diff `observer.ids` across all nodes, controllers first | align files; self-heals ≤10 s, no restart |
| `WARN Failed to read observer ids` | S7 | check file perms/content on that node | fix the file; state was frozen-safe meanwhile |
| Promotion "not taking effect" | S5 (lag) / S6 (missed node) / S4-rule (replica is leader) | check ISR, lag, file consistency | wait for catch-up / align files / move leadership first |
