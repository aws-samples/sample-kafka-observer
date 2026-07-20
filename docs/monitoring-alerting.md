# Monitoring & alerting

What to watch when running observer replicas: stock Kafka metrics (available with any patch version), plus the **v0.7 observer JMX metrics and structured audit log** (shipped in [`patches/kafka-3.7.1-kraft-v07/`](../patches/kafka-3.7.1-kraft-v07/); every number below was read off a live patched cluster — raw output in [evidence/v07_operability_evidence.md](../evidence/v07_operability_evidence.md)).

## The one invariant that matters

> **An observer must never be in the ISR** (except during an intentional promotion).

Everything below exists to (a) detect violations of that invariant, (b) confirm the observer is actually keeping up (so promotion stays cheap), and (c) make every promotion/demotion visible.

## Stock Kafka metrics (any patch version)

### Unchanged by the patch

| Metric (JMX object name) | Use with observers |
|---|---|
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | Baseline health. The observer does **not** count as under-replicated churn — it is out of ISR *by design*, but note Kafka counts `|ISR| < |Replicas|` partitions here, so partitions spanning an observer will show as URP ≥ 1 **permanently**. Baseline your dashboards accordingly (alert on *changes*, not absolute zero). |
| `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | The fail-stop signal. Observers never mask this — they don't count toward min.insync.replicas. `> 0` means writes are blocked → this is your promotion trigger (Scenario A). |
| `kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=ReplicaFetcherThread-*,topic=*,partition=*` (on the observer broker) | The observer's replication lag in messages. Near-zero lag = promotion will be fast. |
| `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce` (p99) | Confirms the "never drags HW" guarantee: adding/removing the observer must not move produce latency. |
| `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | `> 0` after losing all ISR members is expected (`Leader: none` — the observer correctly refuses unclean election). Pair with the Scenario B runbook. |

### Log lines emitted by the patch (grep-able audit trail)

| Log line | Logger / where | Meaning |
|---|---|---|
| `Observer id set changed ...` (v0.6 patches) / `OBSERVER AUDIT (broker\|controller): observer id set changed before=[...] after=[...] added=[...] removed=[...] source=file:<path> epochMs=<ts>` (v0.7 patch, WARN) | `kafka.observer.ObserverIds` (brokers) and `org.apache.kafka.controller.ObserverReplicas` (KRaft controllers) | The `observer.ids` file was re-read and the set changed — every promotion/demotion produces exactly one line per node per side (broker + controller pair in combined mode). `removed` non-empty = promotion, `added` non-empty = demotion; `source` distinguishes file vs env fallback. Measured file-change → first audit line: 3–6 s (5 s cache). Ship to your log pipeline: the full observer-set history is reconstructible from these lines alone. |
| `WARN` from `ObserverIds` on read failure | observer-list file unreadable/corrupt — last cached value kept | The fail-open path is active. A lost file means observers may gradually promote; alert on this WARN. |
| `Filtered observers [...] from initial ISR [...]` | KRaft controller (`ReplicationControlManager`) | Initial-ISR exclusion fired at topic creation — expected once per created partition set. |
| `INELIGIBLE_REPLICA` (reason `observer`) in AlterPartition handling | KRaft controller | Defense-in-depth gate fired: a broker tried to add an observer to ISR (usually an `observer.ids` inconsistency between brokers and controllers). Should be rare; investigate if sustained. |

### Cross-checks (no JMX required)

```bash
# Invariant check: observer id must not appear in Isr (nor Elr/LastKnownElr on 4.x)
kafka-topics.sh --bootstrap-server $BS --describe | grep -E 'Isr:.*\b<OBSERVER_ID>\b'   # expect empty

# File consistency: observer.ids must be identical on every broker AND (KRaft) controller
for h in $HOSTS; do ssh $h md5sum /opt/kafka/observer.ids; done | sort | uniq -c        # expect one hash
```

## Observer JMX metrics (v0.7 patch — shipped, real-machine verified)

Parity target: the information Confluent's `kafka-replica-status.sh` exposes for MRC observers. All 7 gauges reuse the native `KafkaMetricsGroup` registration pattern; evaluation is a lock-free read of existing volatile state (no extra IO — the observer-set check goes through the 5 s cache).

| Metric (JMX object name) | Type | Semantics (as measured) |
|---|---|---|
| `kafka.server:type=ReplicaManager,name=ObserversInIsrCount` | gauge (leader view, aggregated) | Number of replicas that are **on this node's observer list** and currently in the ISR of a partition this broker leads. **Steady-state value: 0** (measured under sustained traffic). Verified in both directions: a *promoted* broker in ISR but off the list reads 0; the *demotion* transition reads 1 for ~5 s (at `replica.lag.time.max.ms=10s`; proportionally longer at the 30 s default) until native shrink completes. Sustained non-zero = gate bypass or `observer.ids` inconsistency. |
| `kafka.observer:type=ObserverMetrics,name=ObserverCount` | gauge | Number of broker ids in this node's `observer.ids` view — compare across nodes to detect file drift. **Lazily registered**: the MBean does not exist on a broker that leads no partitions (ObserverIds not yet initialized) — tolerate its absence or use the ReplicaManager gauges. |
| `kafka.server:type=ReplicaManager,name=ObserverCaughtUpCount` | gauge | Sum over led partitions of observers that are caught up — uses the native `isCaughtUp` function (LEO equal, or lag time ≤ `replica.lag.time.max.ms`; identical semantics to the ISR check). Below expected count = a lagging observer → promotion would stall the HW. |
| `kafka.server:type=ReplicaManager,name=ObserverLagMessages` | gauge | Sum over led partitions of the max observer LEO lag in messages. Measured 0 at steady state with a caught-up observer. Note: message count, not time; the `lastCaughtUpLagMs` equivalent is derivable from the caught-up gauges (no per-replica MBean — avoids cardinality explosion). |
| `kafka.cluster:type=Partition,name=ObserversInIsrCount,topic=…,partition=…` | per-partition gauge | Same three semantics per partition (leader view; 0 when not leader). Steady-state measured `0 / 1 / 0` (InIsr / CaughtUp / Lag) for a partition with one caught-up observer. |
| `kafka.cluster:type=Partition,name=ObserverCaughtUpCount,topic=…,partition=…` | per-partition gauge | ↑ |
| `kafka.cluster:type=Partition,name=ObserverLagMessages,topic=…,partition=…` | per-partition gauge | ↑ |

## Recommended alert rules

| Severity | Condition | Why / action |
|---|---|---|
| **P1** | `ObserversInIsrCount > 0` for > 5 min (v0.7) — today: describe-based invariant check finds an observer id in any `Isr` outside a change window | The core invariant is violated: an observer in ISR can drag HW and (worse) become leader-eligible. Check `observer.ids` consistency on all nodes; demote or investigate immediately. |
| **P1** | `UnderMinIsrPartitionCount > 0` | Writes are fail-stopped. This is the Scenario A promotion trigger — follow [the runbook](runbooks/scenario-a-az-loss.md). |
| **P2** | `ObserverIds` WARN (file read failure) on any node | Fail-open path active — observers may silently promote when the cache expires differently across nodes. Restore the file. |
| **P2** | `observer.ids` checksum differs across nodes for > 2 min | Inconsistency window should be bounded by rollout + 5 s cache; sustained drift means the distribution script failed. KRaft symptom: repeated `INELIGIBLE_REPLICA` in controller logs. |
| **P2** | Observer replica lag > `replica.lag.time.max.ms` equivalent (or `lastCaughtUpLagMs` > 30 000 in v0.7) sustained | A lagging observer makes promotion slow/stalling (HW waits for it *after* promotion until caught up). Check the observer's network/disk. |
| **P3** | Produce p99 latency shifts when an observer is added | Should be structurally impossible (HW gate); a shift indicates misconfiguration (e.g. the id missing from `observer.ids` → the "observer" is just a normal slow follower). |
| **P3** | Any `Observer id set changed` outside a declared change window | Unaudited promotion/demotion — every set change should map to a ticketed operation. |

## Dashboard sketch

1. **Invariant row**: ObserversInIsrCount (v0.7) or scripted describe-check; observer.ids checksum agreement; `INELIGIBLE_REPLICA` rate.
2. **Readiness row**: observer fetch lag per partition; observer broker disk/network; last `Observer id set changed` timestamp per node.
3. **Cluster context row**: UnderMinIsrPartitionCount, OfflinePartitionsCount, produce p99, URP (baselined for permanent observer-induced offset).
