# Version matrix — real-machine S1–S8 across Kafka 2.7 → 4.3

Every version below was **compiled, deployed, and taken through the complete S1–S8 failure
scenario suite on real EC2 instances** — not "the patch applies." All runs used a single-host
multi-process cluster on Graviton `m7g` in Tokyo (`ap-northeast-1`), 4 brokers + observer =
broker 3, `min.insync.replicas=2`, `replica.lag.time.max.ms=10000`. Raw per-run output lives in
[`evidence/version-matrix/`](../evidence/version-matrix/) and
[`evidence/old-versions-real-machine/`](../evidence/old-versions-real-machine/).

## Result: 20 builds, all S1–S8 green

### ZooKeeper mode (JDK 11)

| Kafka | Compile | S1 | S2 | S3 | S4 | S5 | S6 | S7 | S8 |
|---|---|---|---|---|---|---|---|---|---|
| 2.7.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 2.8.1 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 2.8.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.0.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.1.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.2.3 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.3.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.4.1 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.5.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.6.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.7.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.8.1 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.9.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### KRaft mode (JDK 11 for 3.x, JDK 17 for 4.x)

| Kafka | Compile | S1 | S2 | S3 | S4 | S5 | S6 | S7 | S8 |
|---|---|---|---|---|---|---|---|---|---|
| 3.7.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.8.1 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.9.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 4.0.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 4.1.2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 4.2.1 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 4.3.1 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

## What each scenario proves

| # | Scenario | Assertion verified |
|---|---|---|
| **S1** | Leader broker crash (`kill -9`) | New leader elected from an ISR primary — **never the observer**; `acks=all` writes continue in the degraded state (ISR ≥ minISR). |
| **S2** | ISR follower crash | ISR shrinks, leader unchanged, writes continue; the follower auto-rejoins ISR on restart. |
| **S3** | Observer crash | ISR **unchanged** (observer was never in it), writes see zero impact; after restart the observer's log segment is **byte-identical to the leader's** (per-segment md5 match — the EOS byte-copy proof). |
| **S4** | All ISR primaries crash → promotion | Un-promoted observer refuses election (`Leader: none`); clear its id from `observer.ids` → unclean election → observer becomes leader and serves reads/writes. |
| **S5** | Promote a lagging observer | Observer frozen, 30k records pumped to open a lag, then restarted+promoted — it is admitted to ISR **only after catching up to the leader's LEO**; the high-watermark never regresses. |
| **S6** | `observer.ids` node-inconsistency | Cleared on some nodes only → the surviving gate (leader-side in ZK, controller-side in KRaft) keeps the observer out of ISR (fail-safe direction); self-heals when files realign. |
| **S7** | `observer.ids` corruption / permission / deletion | `chmod 000`, garbage content, and file deletion — the broker **never crashes**, keeps its last cached value and logs a WARN (`keeping last value Set(3)`); writes continue throughout. |
| **S8** | Controller failover | ZK: kill the controller broker → new controller still excludes the observer. KRaft: kill a quorum member → remaining quorum takes over, observer still excluded; writes continue. |

## Measured timings (consistent across all 20 builds)

| Event | Time | Driven by |
|---|---|---|
| S1 leader failover | ~9–12 s | ≈ `replica.lag.time.max.ms` (10 s here) + election latency |
| S4 promotion (file edit → observer is leader) | ~10.5 s | `observer.ids` cache TTL (5 s) + unclean election |
| S5 lagging-observer catch-up before ISR admission | ~3–7 s | 30k-record replay volume |
| S8 controller failover | ZK ~7–9 s (new controller election); KRaft quorum sub-second metadata takeover | native Kafka |

## Tunable timing (all defaults, all configurable)

| Knob | Default | Effect | How to change |
|---|---|---|---|
| `replica.lag.time.max.ms` | 10000 ms (we used) / 30000 upstream default | Leader failover time; demotion trigger | broker config |
| `observer.ids` cache TTL | 5 s | Promotion/demotion effective latency after editing the file | `ObserverIds.scala` `CacheTtlNanos` (recompile) |
| isr-expiration period | `replica.lag.time.max.ms / 2` | How fast a demoted broker leaves ISR | derived from above |
| auto-promoter scan interval | 10 s | Auto-detect delay for unattended promotion | `observer-auto-promoter.sh -i` |

## Do I need a different patch per version?

**Yes — one patch family per source-structure generation, not one universal patch.** A patch
is a context-anchored diff, and Kafka's source was refactored several times over this range:

- **KIP-497 (2.7)** — introduced `canAddReplicaToIsr` (AlterIsr); this is the patch's core gate.
- **4.0** — deleted the ZooKeeper controller (`PartitionStateMachine.scala` is gone); 4.x is KRaft-only.
- **4.2** — moved partition/ISR state into Java (`getOutOfSyncReplicas` now sees a `java.util` set,
  needs `.asScala.map(_.toInt)`); the observer hook logic is unchanged, only the surrounding
  collection type differs.

The **five hook points are semantically identical across every supported version** — only line
numbers and surrounding context drift — so each version carries its own re-anchored patch under
[`patches/`](../patches/). **Within a minor line the patch is byte-identical** (e.g. `2.8.1` and
`2.8.2` produced the same patch). To use it: download the `patches/kafka-<your-version>-<mode>/`
directory matching your exact Kafka version and mode.

## Why Kafka 2.7 is the earliest supported version

The patch's central gate, the leader-side `canAddReplicaToIsr`, **first appears in Kafka 2.7**
as part of KIP-497 (AlterIsr), which moved ISR management from "leader writes ZooKeeper directly"
to "leader requests the controller via the AlterIsr API." Before 2.7 the method does not exist —
the patch has nowhere to attach its core gate. This is a **structural incompatibility**, confirmed
by real-machine function probing on shallow clones:

| Kafka | `canAddReplicaToIsr` present? | ISR management model | Supportable? |
|---|---|---|---|
| 2.4.1 / 2.5.1 / 2.6.3 | ❌ no | leader-writes-ZooKeeper (pre-KIP-497) | **No** — no hook site exists |
| 2.7.x and later | ✅ yes | AlterIsr API / KRaft | **Yes** — verified through 4.3 |

Supporting ≤ 2.6 would require a fundamentally different mechanism (intercepting the direct-ZK ISR
write path) and is out of scope for this project.

## Reproducing

```bash
# JDK 11 (Kafka ≤ 3.x) or JDK 17 (Kafka 4.x), plus git:
V=3.9.2 ; MODE=zk         # or e.g. V=4.3.1 MODE=kraft
git clone --depth 1 --branch $V https://github.com/apache/kafka.git build && cd build
git apply <repo>/patches/kafka-$V-$MODE/observer.patch
# Build-infra shims for older lines (unrelated to the patch — JCenter shut down in 2021):
sed -i '/^    jcenter()$/d' build.gradle 2>/dev/null || true
[ -f gradle/dependencies.gradle ] && sed -i -E 's/grgit: "4\.[0-9.]+"/grgit: "5.0.0"/' gradle/dependencies.gradle
mv .git .git-bak                       # skips the rat license-check task; not needed to compile
./gradlew :core:jar :tools:jar $([ "$MODE" = kraft ] && echo :metadata:jar) -x test
# Scenario harness: <repo>/tools/zk-scenario-test.sh <v>  |  tools/kraft-scenario-test.sh <v>
```

> Build-infra note: the three `sed`/`mv` shims above touch only the Gradle build wiring of older
> Kafka releases (dead JCenter repo, an old grgit plugin, the rat license task). **Kafka's own
> source and the observer patch are never modified by them.**
