# Monitoring & alerting

What to watch when running observer replicas, with what exists **today** (v0.6 — stock Kafka metrics + log lines) and the **v0.7 planned JMX metrics** (design names — not yet shipped; see [ROADMAP.md](../ROADMAP.md)).

## The one invariant that matters

> **An observer must never be in the ISR** (except during an intentional promotion).

Everything below exists to (a) detect violations of that invariant, (b) confirm the observer is actually keeping up (so promotion stays cheap), and (c) make every promotion/demotion visible.

## Available today (v0.6)

### Stock Kafka metrics (unchanged by the patch)

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
| `Observer id set changed ...` | `kafka.observer.ObserverIds` (brokers) and `org.apache.kafka.controller.ObserverReplicas` (KRaft controllers) | The `observer.ids` file was re-read and the set changed — every promotion/demotion produces exactly one of these per node. Ship to your log pipeline; this is the audit log until the v0.7 structured version. |
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

## Planned for v0.7 (design names — not yet implemented)

Parity target: the information Confluent's `kafka-replica-status.sh` exposes for MRC observers.

| Planned metric | Type | Semantics |
|---|---|---|
| `kafka.server:type=ReplicaManager,name=ObserversInIsrCount` | gauge | Number of (partition, replica) entries where a configured observer is currently in the ISR. **Steady-state value: 0.** Non-zero = invariant violation or an in-progress intentional promotion. |
| `kafka.server:type=ReplicaManager,name=ObserverCount` | gauge | Number of broker ids currently configured as observers (file view of this node) — for detecting file drift between nodes. |
| Per-replica status: `isObserver` / `isCaughtUp` / `lastCaughtUpLagMs` | per-partition gauges (or an admin/CLI view) | Promotion pre-check data: is the replica an observer, is it within `replica.lag.time.max.ms` of the leader, and how far behind is its last caught-up timestamp. |
| Structured promotion/demotion audit log | log (single line, machine-parseable: timestamp, node, old set → new set, file mtime) | Successor of the free-form `Observer id set changed` line. |

Names are final-intent but unshipped; treat them as design references until the v0.7 release notes say otherwise.

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
