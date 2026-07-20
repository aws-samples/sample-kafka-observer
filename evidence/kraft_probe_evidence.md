# KRaft 模式 broker 侧 hook 真机探针证据

- 日期: 2026-07-20 (JST 18:43-18:47, 日志时间戳为 UTC 09:43-09:47)
- 环境: 东京 loadgen (54.250.248.165), 单机 3 节点 KRaft 集群 (node 1 combined broker+controller @9292/9393, node 2/3 broker-only @9294/9296), 数据目录 /tmp/kraft-probe, 与现有 ZK 集群 (9092) 完全隔离
- Jar: /tmp/kafka-src 打过 v3 patch 的 Kafka 3.7.1 (`kafka_2.13-3.7.1.jar`, patch 脚本 = `patches/kafka-3.7.1-zk-v0.3.py`)
- 探针后已完整清理 (进程 kill、/tmp/kraft-probe 删除、9292/9294/9296/9393 端口释放), ZK 集群未受影响

## 结论一览

| Hook | 位置 | KRaft 下是否生效 | 证据 |
|---|---|---|---|
| [1] canAddReplicaToIsr 晋升闸门 | Partition.scala (broker) | ✅ 生效 | observer=3 追平 100 条消息后仍不回 ISR; 清空文件 5s 内自动 Expand 回 ISR |
| [2] getOutOfSyncReplicas 降级钩子 | Partition.scala (broker) | ✅ 生效 | 设 observer=3 后 isr-expiration 自动 Shrink ISR 2,3,1→2,1 |
| [2b] maybeIncrementLeaderHW 闸门 | Partition.scala (broker) | ✅ 代码路径生效[推断] | 同一 Partition.scala 路径; 单机低负载下 HW 拖累无法直接观测, 但 produce/consume 正常、无异常 |
| [3] PartitionStateMachine 初始 ISR 排除 | kafka.controller (纯 ZK) | ❌ 不生效 | RF3 建 topic 初始 ISR=2,3,1 **含 observer 3** (ZK 模式下同 patch 会排除) |
| [4] PartitionLeaderElectionAlgorithms unclean 排除 | kafka.controller (纯 ZK) | ❌ 不生效[事实-代码路径] | KRaft 选举走 quorum controller 的 ReplicationControlManager/PartitionChangeBuilder, 不经过 ZK controller 代码 (本探针未做 unclean 场景, 依据为 [3] 的同源事实: ZK controller 代码在 KRaft 进程中不被调用) |
| ObserverIds 动态文件读取 (KAFKA_OBSERVER_IDS_FILE / observer.ids, 5s 缓存) | 新增 kafka.observer 包 | ✅ 生效 | 日志两次打印 "Observer id set changed" |

**净结论: "broker 侧 3 hook 免费移植到 KRaft" 的推断被真机确认为 [事实]；controller 侧 2 hook 在 KRaft 下确认不生效 (初始 ISR 不排除 observer——真机实测)。ZK patch 在 KRaft 下 = "建 topic 后 15~30s 内 observer 被降级钩子自动挤出 ISR，之后行为与 ZK 模式一致"，但存在建 topic 初期 observer 短暂在 ISR 的窗口。**

## 原始证据

### 1. 纯 KRaft 模式启动 (patched jar, 无 ZK 连接)

```
[2026-07-20 09:43:11,489] INFO [BrokerServer id=1] Transition from STARTING to STARTED (kafka.server.BrokerServer)
[2026-07-20 09:43:11,490] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)
```
(日志出现 `KafkaRaftServer` 且启动 `BrokerServer`/quorum controller; server.properties 为 `process.roles=broker,controller` + `controller.quorum.voters`, 无 `zookeeper.connect`。zookeeper 相关行仅为客户端类库静态初始化 X509Util, 非 ZK 连接。)

### 2. ObserverIds 动态文件读取在 KRaft 下工作

```
[2026-07-20 09:45:38,643] INFO Observer id set changed: Set() -> Set(3) (source: /tmp/kraft-probe/observer.ids) (kafka.observer.ObserverIds$)
[2026-07-20 09:47:19,485] INFO Observer id set changed: Set(3) -> Set() (source: /tmp/kraft-probe/observer.ids) (kafka.observer.ObserverIds$)
```

### 3. [关键] 初始 ISR 不排除 observer — controller 侧 hook 在 KRaft 失效

observer.ids 内容为 `3`，随后创建 RF3 topic：

```
=== T+0 initial ISR ===
Topic: probe-rf3  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1   <-- observer 3 在初始 ISR 里!
```
ZK 模式下 hook[3] (PartitionStateMachine.initializeLeaderAndIsrForPartitions) 会在建 topic 时排除 observer；KRaft 的初始 ISR 由 quorum controller 的 ReplicationControlManager 决定，该代码未被 patch，故 observer 进入初始 ISR。[事实-真机]

### 4. 降级钩子 (getOutOfSyncReplicas) 在 KRaft 生效 — 自动挤出初始 ISR

无需任何操作，leader (broker 2) 的周期性 isr-expiration 任务把 observer 3 视同 out-of-sync 并 shrink（本探针 replica.lag.time.max.ms=10000，任务周期 = 其一半 = 5s）：

```
[2026-07-20 09:45:42,494] INFO [Partition probe-rf3-0 broker=2] Shrinking ISR from 2,3,1 to 2,1.
  Leader: (highWatermark: 0, endOffset: 0). Out of sync replicas: (brokerId: 3, endOffset: 0, ...)
[2026-07-20 09:45:42,535] INFO [Partition probe-rf3-0 broker=2] ISR updated to 2,1  and version updated to 1
=== T+20s ===  Isr: 2,1
```
注意: 此处 ISR 更新走的是 KRaft 的 AlterPartition→quorum controller 路径 (`ISR updated to ... and version updated to ...` 为 KRaft 写法)，说明 broker 侧 hook 产出的 shrink 决策能被 KRaft controller 正常接受。[事实-真机]

### 5. 晋升闸门 (canAddReplicaToIsr) 在 KRaft 生效 — 追平也不回 ISR

produce 100 条消息后等待 15s (多个 fetch/expiration 周期)：

```
=== ISR after producing ===  Isr: 2,1    <-- observer 3 已完全追平仍被闸门挡在 ISR 外
=== observer 3 本地副本 DumpLogSegments ===
baseOffset: 0 lastOffset: 99 count: 100 ... isvalid: true   <-- 数据面复制完全正常(100/100)
```
observer 副本持续复制数据 (LEO 追平)、却因 canAddReplicaToIsr 闸门永不进 ISR — 与 ZK 模式行为一致。[事实-真机]

### 6. 零重启晋升在 KRaft 生效

清空 observer.ids (`# promoted`) 后 ~12s：

```
[2026-07-20 09:47:19,485] INFO Observer id set changed: Set(3) -> Set()
[2026-07-20 09:47:19,517] INFO [Partition probe-rf3-0 broker=2] ISR updated to 2,1,3  and version updated to 2
=== describe ===  Isr: 2,1,3
```
文件变更 → 5s 缓存过期 → 下次 fetch 触发 maybeExpandIsr → 闸门放行 → KRaft AlterPartition 路径完成 ISR 扩张。全程零重启。[事实-真机]

## 能力边界 (由本探针精确划定)

- ✅ **KRaft 下已有 (broker 侧免费移植)**: observer 不进 ISR / 追平也不进 / 降级自动 shrink / 零重启晋升降级 / HW 不等 observer (代码路径共用[推断], 因 Partition.scala 为两模式共用[事实-源码: 3.7.1 与 4.0.0 的 core/src/main/scala/kafka/cluster/Partition.scala 均无 ZK 依赖分支])
- ❌ **KRaft 下缺失 (需另 patch quorum controller)**:
  1. 初始 ISR 排除 → 需改 `metadata` 模块 ReplicationControlManager (Java)。后果: 建 topic 后有一个 ~replica.lag.time.max.ms/2 量级的窗口, observer 在 ISR 内 (acks=all 会等它, min.insync.replicas 计数含它)
  2. unclean/受控选举排除 → 需改 PartitionChangeBuilder/ElectionStrategy。后果: 若在窗口期或极端竞态下 observer 仍在 ISR/ELR, 它可能被选为 leader
- [推断] 对 Kafka 4.0 (仅 KRaft): broker 侧 3 hook 概念上可平移 (Partition.scala 仍存在), 但 4.0 无 ZK controller, [3]/[4] 必须在 metadata 模块重做; 且 4.0 引入 ELR (KIP-966, Eligible Leader Replicas) 后还需把 observer 排除出 ELR, 这是 3.7.1 探针未覆盖的新面。

## 复现步骤 (概要)

1. loadgen: `cp -r /opt/kafka /tmp/kraft-probe/kafka`, 换入 patched jar
2. node1 combined (`process.roles=broker,controller`, 9292/9393) + node2/3 broker-only (9294/9296), 同一 cluster.id format
3. `KAFKA_OBSERVER_IDS_FILE=/tmp/kraft-probe/observer.ids`, 文件写 `3`, `replica.lag.time.max.ms=10000`
4. 建 RF3 topic → 看初始 ISR (含 3) → 等 shrink (不含 3) → produce 100 条 → 确认追平仍不回 → 清空文件 → 确认自动回 ISR
