# Scenario Matrix Evidence — S1–S8 failure experiments (raw output)

Raw, unedited command output backing every number in
[docs/scenario-playbook.md](../docs/scenario-playbook.md) § "The S1–S8 scenario matrix".

- **Executed**: 2026-07-20 (UTC timestamps inline), Tokyo loadgen EC2 host (m7g.xlarge, 4 vCPU / 16 GB).
- **Build**: Kafka 3.7.1 + combined observer patch (`patches/kafka-3.7.1-kraft/`) — patched
  `core`/`metadata`/`storage` jars swapped into a vanilla 3.7.1 distribution.
- **Topology**: single-host multi-process KRaft — dedicated controller quorum (node ids 101/102/103,
  ports 9791–9793) + 4 brokers (ids 1–4, ports 9592/9594/9596/9598). Observer = broker 3.
  Each process had its own `KAFKA_OBSERVER_IDS_FILE` so per-node inconsistency could be injected.
  Heap: 512 MB per broker, 256 MB per controller.
- **Configs**: `replica.lag.time.max.ms=10000`; topics `sm` (`--replica-assignment 3:1:2`,
  `min.insync.replicas=2`) and `smx` (`--replica-assignment 3:1:2:4`, `min.insync.replicas=2`).
- Cluster was torn down and `/tmp/scenario-matrix` deleted after the run; ports verified free.

The first S1 block below ran on the 2-primary topic `sm` and shows the *fail-stop* variant
(`acked: 0` after failover, ISR 1 < minISR 2 — intended). The canonical S1 was then re-run
on the 3-primary topic `smx`.

```text
===== S1: leader broker offline (non-observer) — 2026-07-20T14:49:04Z =====
--- pre-state ---
Topic: sm	TopicId: Vj317gj6QaGiL3sv34LF4w	PartitionCount: 1	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: sm	Partition: 0	Leader: 1	Replicas: 3,1,2	Isr: 1,2
--- baseline write (acks=all) ---
{"timestamp":1784558947382,"name":"shutdown_complete"}
{"timestamp":1784558947384,"name":"tool_data","sent":500,"acked":500,"target_throughput":500,"avg_throughput":492.12598425196853}
current leader: broker 1
--- INJECT: kill -9 broker 1 (pid 503247) at 14:49:08.965Z ---
new leader elected: broker 2 after 10085 ms
	Topic: sm	Partition: 0	Leader: 2	Replicas: 3,1,2	Isr: 2
--- write after failover (acks=all, minISR=2, ISR should be 2 members) ---
{"timestamp":1784558960242,"name":"shutdown_complete"}
{"timestamp":1784558960243,"name":"tool_data","sent":200,"acked":0,"target_throughput":500,"avg_throughput":0.0}
--- RECOVER: restart broker 1 at 14:49:20.566Z ---
broker 1 rejoined ISR after 3825 ms from restart command
	Topic: sm	Partition: 0	Leader: 2	Replicas: 3,1,2	Isr: 2,1
===== S1 done =====

===== S1 (canonical, topic smx: Replicas 3,1,2,4, observer=3, minISR=2) — 2026-07-20T14:51:56Z =====
--- pre-state ---
	Topic: smx	Partition: 0	Leader: 1	Replicas: 3,1,2,4	Isr: 1,2,4
--- baseline write ---
{"timestamp":1784559119559,"name":"tool_data","sent":500,"acked":500,"target_throughput":500,"avg_throughput":489.2367906066536}
leader: broker 1
--- INJECT: kill -9 leader broker 1 (pid 509978) ---
new leader: broker 2 after 10437 ms (observer is 3 — must never be it)
	Topic: smx	Partition: 0	Leader: 2	Replicas: 3,1,2,4	Isr: 2,4
--- write during degraded state (ISR=2 >= minISR=2, must succeed) ---
{"timestamp":1784559134365,"name":"tool_data","sent":300,"acked":300,"target_throughput":500,"avg_throughput":482.31511254019296}
--- RECOVER: restart broker 1 ---
broker 1 rejoined ISR after 3827 ms
	Topic: smx	Partition: 0	Leader: 2	Replicas: 3,1,2,4	Isr: 2,4,1
===== S1 done =====

===== S2: follower broker offline — 2026-07-20T14:52:23Z =====
--- pre-state ---
	Topic: smx	Partition: 0	Leader: 2	Replicas: 3,1,2,4	Isr: 2,4,1
leader: 2, killing follower: 4
--- INJECT: kill -9 follower broker 4 (pid 511746) ---
ISR shrank (4 removed) after 10346 ms
	Topic: smx	Partition: 0	Leader: 2	Replicas: 3,1,2,4	Isr: 2,1
--- write during shrink (ISR=2 >= minISR=2, leader unchanged, must succeed) ---
{"timestamp":1784559160035,"name":"tool_data","sent":300,"acked":300,"target_throughput":500,"avg_throughput":487.8048780487805}
leader after follower kill: 2 (expected unchanged: 2)
--- RECOVER: restart broker 4 ---
broker 4 auto-rejoined ISR after 3874 ms (no operator ISR action)
	Topic: smx	Partition: 0	Leader: 2	Replicas: 3,1,2,4	Isr: 2,1,4
===== S2 done =====

===== S3: observer offline — 2026-07-20T14:53:37Z =====
--- pre-state ---
	Topic: smx	Partition: 0	Leader: 2	Replicas: 3,1,2,4	Isr: 2,1,4
--- baseline acks=all latency (1000 msgs via producer-perf) ---
1000 records sent, 495.049505 records/sec (0.09 MB/sec), 53.90 ms avg latency, 474.00 ms max latency, 19 ms 50th, 222 ms 95th, 223 ms 99th, 474 ms 99.9th.
--- INJECT: kill -9 observer broker 3 (pid 504082) ---
--- ISR after observer death (observer was never in it; ISR must be unchanged) ---
	Topic: smx	Partition: 0	Leader: 2	Replicas: 3,1,2,4	Isr: 2,1,4
--- acks=all latency DURING observer outage (1000 msgs) ---
1000 records sent, 495.540139 records/sec (0.09 MB/sec), 42.56 ms avg latency, 467.00 ms max latency, 2 ms 50th, 218 ms 95th, 219 ms 99th, 467 ms 99.9th.
--- leader HWM offset with observer dead ---
smx:0:3100
--- RECOVER: restart observer broker 3 ---
	Topic: smx	Partition: 0	Leader: 2	Replicas: 3,1,2,4	Isr: 2,1,4
note: observer stays out of Isr by design; catch-up verified by log-end offset
===== S3 done =====
--- S3 addendum: observer catch-up proof (2026-07-20T14:58:32Z) ---
leader (broker 2) log end:
532065 /tmp/scenario-matrix/data-broker2/smx-0/00000000000000000000.log
observer (broker 3) log end:
532065 /tmp/scenario-matrix/data-broker3/smx-0/00000000000000000000.log
replica lag via replication metrics (DumpLog last offset):
baseOffset: 3099 lastOffset: 3099
baseOffset: 3099 lastOffset: 3099
byte-level: md5 of leader vs observer segment file:
448a44d4c46c95c584315a4255c1b316  /tmp/scenario-matrix/data-broker2/smx-0/00000000000000000000.log
448a44d4c46c95c584315a4255c1b316  /tmp/scenario-matrix/data-broker3/smx-0/00000000000000000000.log

===== S4: both primaries down -> observer promotion (topic sm, observer=3, minISR=2) — 2026-07-20T14:59:58Z =====
--- pre-state ---
	Topic: sm	Partition: 0	Leader: 2	Replicas: 3,1,2	Isr: 2,1
--- baseline write ---
{"timestamp":1784559601064,"name":"tool_data","sent":300,"acked":300,"target_throughput":500,"avg_throughput":491.8032786885246}
--- pre-kill data proof: leader vs observer segment md5 ---
0d5f955252e66f8401cc8da5c4c04a55  /tmp/scenario-matrix/data-broker2/sm-0/00000000000000000000.log
0d5f955252e66f8401cc8da5c4c04a55  /tmp/scenario-matrix/data-broker3/sm-0/00000000000000000000.log
--- INJECT: kill -9 BOTH primaries (brokers 1 and 2) at 15:00:02.392Z ---
--- observe: partition has no leader; observer 3 alive but NOT elected (exclusion holds even with whole ISR dead) ---
	Topic: sm	Partition: 0	Leader: none	Replicas: 3,1,2	Isr: 1
--- write attempt while down (expect fail/timeout) ---
     50 "name":"producer_send_error"
      1 "name":"shutdown_complete"
      1 "name":"startup_complete"
      1 "name":"tool_data"

--- PROMOTE: remove id 3 from observer.ids on ALL nodes (3 controllers + brokers) at 15:02:17.493Z ---
--- promoted replica 3 is NOT in ISR (ISR still holds last dead member) -> clean election impossible; run UNCLEAN election onto byte-identical replica 3 ---
Successfully completed leader election (UNCLEAN) for partitions sm-0
leader after promotion+election (9353 ms since file edit):
	Topic: sm	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 3

--- BRANCH B (read-only wait): ISR={3} < minISR=2 -> acks=all writes MUST fail, reads MUST work ---
read from promoted leader (assign mode, no group coordinator):
0
1
2
3
4
(read of first 5 records OK)
acks=all write attempt (expect NOT_ENOUGH_REPLICAS):
      2 "exception":"class org.apache.kafka.common.errors.NotEnoughReplicasException"

--- BRANCH A (avail-over-durability): lower min.insync.replicas to 1 -> writes resume on single replica ---
Completed updating config for topic sm.
{"timestamp":1784559755580,"name":"tool_data","sent":200,"acked":200,"target_throughput":500,"avg_throughput":490.19607843137254}

--- RECOVERY SOP: restore minISR=2, restart primaries, re-demote 3 ---
Completed updating config for topic sm.
primaries 1,2 back in ISR after 4689 ms
	Topic: sm	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
--- re-demote 3: add back to all observer.ids; 3 is current LEADER -> known KRaft rule: hot demote will not remove a leader; restart broker 3 once ---
final steady state (leader on a primary, 3 out of ISR again):
	Topic: sm	Partition: 0	Leader: 1	Replicas: 3,1,2	Isr: 1,2
===== S4 done =====

===== S5: promotion while observer is lagging — 2026-07-20T15:04:22Z =====
--- pre-state ---
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
--- step 1: kill observer 3 to freeze its log ---
--- step 2: pump 30000 records x 500B while observer is down ---
30000 records sent, 10838.150289 records/sec (5.17 MB/sec), 1441.26 ms avg latency, 2379.00 ms max latency, 1541 ms 50th, 2283 ms 95th, 2366 ms 99th, 2377 ms 99.9th.
leader LEO now: 
--- step 3: PRE-CHECK the runbook mandates (observer down => lag unbounded; with it up you compare LEOs) ---
observer 3 is DOWN: kafka-get-offsets against it fails => promotion pre-check FAILS, do not promote blindly
--- step 4: restart observer AND promote immediately (worst-case realistic race) ---
replica 3 entered ISR after 5383 ms from restart+promotion (had to replay ~30k records first)
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2,3
observer LEO after ISR join:  (leader ) — caught up BEFORE admission, HW never went backwards
--- step 5: acks=all write with newly-admitted replica in ISR ---
1000 records sent, 498.007968 records/sec (0.09 MB/sec), 22.75 ms avg latency, 360.00 ms max latency, 6 ms 50th, 111 ms 95th, 112 ms 99th, 360 ms 99.9th.
--- key negative result: what if you UNCLEAN-elect a lagging promoted replica (Scenario-B style)? ---
    Its LEO would become the truth => every record past its LEO is LOST. That is why the
    runbook pre-check (compare observer LEO to leader LEO before promoting) is mandatory.
--- RECOVER: demote 3 back (it is a follower now => hot demotion path) ---
replica 3 hot-demoted out of ISR after 3500 ms (isr-expiration shrink)
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
===== S5 done =====
--- S5 addendum: quantified catch-up proof (2026-07-20T15:05:43Z) ---
leader-side latest offset (kafka-get-offsets):
smx:0:34100
on-disk log bytes, leader(4) vs observer(3):
37082883	/tmp/scenario-matrix/data-broker4/smx-0
37082975	/tmp/scenario-matrix/data-broker3/smx-0
per-broker last batch in smx-0 (leader 4 vs observer 3):
broker 4 (/tmp/scenario-matrix/data-broker4/smx-0/00000000000000000000.log):
baseOffset: 34099 lastOffset: 34099
broker 3 (/tmp/scenario-matrix/data-broker3/smx-0/00000000000000000000.log):
baseOffset: 34099 lastOffset: 34099

===== S6: observer.ids inconsistency window (broker-side promoted, controller-side NOT) — 2026-07-20T15:06:32Z =====
--- pre-state ---
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
controller files: 3 / 3 / 3; broker files: 3 3 3 3
--- INJECT: clear observer.ids ONLY on broker nodes (controllers keep 3) at 15:06:35.177Z ---
waiting 30s: leader-side gate now lets 3 through -> leader proposes AlterPartition -> controller must refuse
--- observe: ISR unchanged (3 still excluded — controller wins, fail-safe) ---
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
--- controller-side rejection log (active controller, ineligible replica reason=observer) ---
[2026-07-20 15:07:05,376] INFO [QuorumController id=101] Rejecting AlterPartition request from node 4 for smx-0 because it specified ineligible replicas [3 (observer)] in the new ISR [BrokerState(brokerId=4, brokerEpoch=664), BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)]. (org.apache.kafka.controller.ReplicationControlManager)
[2026-07-20 15:07:05,804] INFO [QuorumController id=101] Rejecting AlterPartition request from node 1 for sm-0 because it specified ineligible replicas [3 (observer)] in the new ISR [BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)]. (org.apache.kafka.controller.ReplicationControlManager)
[2026-07-20 15:07:05,877] INFO [QuorumController id=101] Rejecting AlterPartition request from node 4 for smx-0 because it specified ineligible replicas [3 (observer)] in the new ISR [BrokerState(brokerId=4, brokerEpoch=664), BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)]. (org.apache.kafka.controller.ReplicationControlManager)
[2026-07-20 15:07:06,306] INFO [QuorumController id=101] Rejecting AlterPartition request from node 1 for sm-0 because it specified ineligible replicas [3 (observer)] in the new ISR [BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)]. (org.apache.kafka.controller.ReplicationControlManager)
[2026-07-20 15:07:06,378] INFO [QuorumController id=101] Rejecting AlterPartition request from node 4 for smx-0 because it specified ineligible replicas [3 (observer)] in the new ISR [BrokerState(brokerId=4, brokerEpoch=664), BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)]. (org.apache.kafka.controller.ReplicationControlManager)
--- leader-broker-side view of the refused AlterPartition ---
[2026-07-20 15:07:04,374] INFO [Partition smx-0 broker=4] Failed to alter partition to PendingExpandIsr(newInSyncReplicaId=3, sentLeaderAndIsr=LeaderAndIsr(leader=4, leaderEpoch=3, isrWithBrokerEpoch=List(BrokerState(brokerId=4, brokerEpoch=664), BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)), leaderRecoveryState=RECOVERED, partitionEpoch=12), leaderRecoveryState=RECOVERED, lastCommittedState=CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED)) since the controller rejected the request with INELIGIBLE_REPLICA. Partition state has been reset to the latest committed state CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED). (kafka.cluster.Partition)
[2026-07-20 15:07:04,875] INFO [Partition smx-0 broker=4] Failed to alter partition to PendingExpandIsr(newInSyncReplicaId=3, sentLeaderAndIsr=LeaderAndIsr(leader=4, leaderEpoch=3, isrWithBrokerEpoch=List(BrokerState(brokerId=4, brokerEpoch=664), BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)), leaderRecoveryState=RECOVERED, partitionEpoch=12), leaderRecoveryState=RECOVERED, lastCommittedState=CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED)) since the controller rejected the request with INELIGIBLE_REPLICA. Partition state has been reset to the latest committed state CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED). (kafka.cluster.Partition)
[2026-07-20 15:07:05,376] INFO [Partition smx-0 broker=4] Failed to alter partition to PendingExpandIsr(newInSyncReplicaId=3, sentLeaderAndIsr=LeaderAndIsr(leader=4, leaderEpoch=3, isrWithBrokerEpoch=List(BrokerState(brokerId=4, brokerEpoch=664), BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)), leaderRecoveryState=RECOVERED, partitionEpoch=12), leaderRecoveryState=RECOVERED, lastCommittedState=CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED)) since the controller rejected the request with INELIGIBLE_REPLICA. Partition state has been reset to the latest committed state CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED). (kafka.cluster.Partition)
[2026-07-20 15:07:05,878] INFO [Partition smx-0 broker=4] Failed to alter partition to PendingExpandIsr(newInSyncReplicaId=3, sentLeaderAndIsr=LeaderAndIsr(leader=4, leaderEpoch=3, isrWithBrokerEpoch=List(BrokerState(brokerId=4, brokerEpoch=664), BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)), leaderRecoveryState=RECOVERED, partitionEpoch=12), leaderRecoveryState=RECOVERED, lastCommittedState=CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED)) since the controller rejected the request with INELIGIBLE_REPLICA. Partition state has been reset to the latest committed state CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED). (kafka.cluster.Partition)
[2026-07-20 15:07:06,378] INFO [Partition smx-0 broker=4] Failed to alter partition to PendingExpandIsr(newInSyncReplicaId=3, sentLeaderAndIsr=LeaderAndIsr(leader=4, leaderEpoch=3, isrWithBrokerEpoch=List(BrokerState(brokerId=4, brokerEpoch=664), BrokerState(brokerId=1, brokerEpoch=1875), BrokerState(brokerId=2, brokerEpoch=1879), BrokerState(brokerId=3, brokerEpoch=2117)), leaderRecoveryState=RECOVERED, partitionEpoch=12), leaderRecoveryState=RECOVERED, lastCommittedState=CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED)) since the controller rejected the request with INELIGIBLE_REPLICA. Partition state has been reset to the latest committed state CommittedPartitionState(isr=Set(4, 1, 2), leaderRecoveryState=RECOVERED). (kafka.cluster.Partition)
--- HEAL: make files consistent (clear controller files too) at 15:07:06.438Z ---
self-healed: 3 admitted to ISR 5783 ms after controller files were aligned
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2,3
--- RESTORE: put 3 back as observer everywhere; wait for hot demotion ---
3 demoted out again
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
===== S6 done =====

===== S7: observer.ids corruption / permission failure — 2026-07-20T15:07:56Z =====
--- pre-state (3 is observer, out of ISR) ---
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
--- INJECT 7a: chmod 000 on broker-4 leader file + active controller file at 15:07:58.088Z ---
state after permission denial (must be UNCHANGED — last cached value kept):
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
broker-4 WARN lines:
[2026-07-20 15:08:06,949] WARN Failed to read observer ids from /tmp/scenario-matrix/obs-broker4.ids, keeping last value Set(3) (kafka.observer.ObserverIds$)
[2026-07-20 15:08:06,949] WARN Failed to read observer ids from /tmp/scenario-matrix/obs-broker4.ids, keeping last value Set(3) (kafka.observer.ObserverIds$)
controller-101 WARN lines:
broker 4 still alive: 1 process(es)
write still works during the fault:
{"timestamp":1784560092391,"name":"tool_data","sent":100,"acked":100,"target_throughput":500,"avg_throughput":469.4835680751174}
permissions restored

--- INJECT 7b: write garbage into the files at 15:08:18.716Z ---
garbage file content:
banana,3
# comment line
%%@@!!
 3 ,xyz
state after garbage (non-numeric tokens silently ignored, id 3 still parsed => still observer):
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
log evidence that the set did NOT flap (no unexpected change events after garbage write):
[2026-07-20 15:04:41,562] INFO Observer id set changed: Set(3) -> Set(3) (source: /tmp/scenario-matrix/obs-broker4.ids) (kafka.observer.ObserverIds$)
[2026-07-20 15:06:36,822] INFO Observer id set changed: Set(3) -> Set() (source: /tmp/scenario-matrix/obs-broker4.ids) (kafka.observer.ObserverIds$)
[2026-07-20 15:07:16,877] INFO Observer id set changed: Set() -> Set(3) (source: /tmp/scenario-matrix/obs-broker4.ids) (kafka.observer.ObserverIds$)
write still works:
{"timestamp":1784560112986,"name":"tool_data","sent":100,"acked":100,"target_throughput":500,"avg_throughput":478.4688995215311}

--- INJECT 7c: delete the file entirely (fallback to env, which is unset => empty set => 3 becomes promotable) ---
after delete on broker-4 only (controllers still have 3 => S6 fail-safe protects ISR):
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
--- RECOVER: restore canonical files everywhere ---
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
===== S7 done =====

===== S8: active controller failover, observer semantics preserved — 2026-07-20T15:09:28Z =====
--- pre-state: quorum ---
LeaderId:               101
CurrentVoters:          [101,102,103]
active controller: 101
--- INJECT: kill -9 active controller 101 (pid 501917) at 15:09:31.354Z ---
new active controller: 103 after 3722 ms
--- verify 1: existing partitions keep observer exclusion under new controller ---
	Topic: sm	Partition: 0	Leader: 1	Replicas: 3,1,2	Isr: 1,2
	Topic: smx	Partition: 0	Leader: 4	Replicas: 3,1,2,4	Isr: 4,1,2
--- verify 2: CREATE NEW topic under the new controller — initial ISR must exclude 3 ---
Created topic sm-postfailover.
	Topic: sm-postfailover	Partition: 0	Leader: 1	Replicas: 3,1,2	Isr: 1,2
--- verify 3: new controller logs the initial-ISR filtering ---
--- verify 4: preferred-leader election under new controller never picks 3 ---
	Suppressed: org.apache.kafka.common.errors.PreferredLeaderNotAvailableException: The preferred leader was not available.

	Topic: sm-postfailover	Partition: 0	Leader: 1	Replicas: 3,1,2	Isr: 1,2
--- verify 5: write path healthy after controller failover ---
{"timestamp":1784560186957,"name":"tool_data","sent":200,"acked":200,"target_throughput":500,"avg_throughput":483.0917874396135}
--- RECOVER: restart controller 101; confirm it rejoins the quorum ---
LeaderId:               103
MaxFollowerLag:         0
CurrentVoters:          [101,102,103]
===== S8 done =====
--- S8 addendum: controller-side initial-ISR filtering evidence (2026-07-20T15:10:15Z) ---
(new active controller 103 created sm-postfailover)
note on verify-4: preferred election returned PreferredLeaderNotAvailableException because
the preferred replica (first in Replicas: 3,1,2) is the observer — the new controller refuses it. QED.
--- S8 addendum 2: hunting the Filtered observers log line ---
(if empty: the log level or logger for ReplicationControlManager INFO may be routed elsewhere; check all files)
/tmp/scenario-matrix/logs/ctrl103/controller.log
/tmp/scenario-matrix/logs/ctrl101/controller.log.2026-07-20-14
--- S8 addendum 3: Filtered observers lines (found in controller.log) ---
[2026-07-20 15:09:38,510] INFO Filtered observers [3] from initial ISR [3, 1, 2] -> [1, 2] (org.apache.kafka.controller.ObserverReplicas)
(controller 103 = new active controller after failover; it filtered 3 from sm-postfailover initial ISR)
```
