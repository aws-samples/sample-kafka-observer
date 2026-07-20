# kafka-3.7.1-kraft-v07/observer.patch — Combined + Metrics (v0.7)

⚠️ 与 `kafka-3.7.1-kraft/observer.patch` 同为 **combined patch** (打在 vanilla Kafka 3.7.1 源码树上, ZK + KRaft 两模式通吃), 在 v0.6 全部 observer 功能之上叠加 **v0.7 可观测层**: JMX metrics + 结构化审计日志。功能性 hook (ISR gate / 降级钩子 / 选举排除 / controller 三处) 与 v0.6 **逐字节一致**, 零行为变更 — v0.7 只增加"看"的能力, 不改"做"的逻辑。

## 与 v0.6 的 diff (新增内容一览)

| 文件 | v0.6 | v0.7 新增 |
|---|---|---|
| `core/.../cluster/Partition.scala` | ISR gate + 降级钩子 (3 hook) | +3 个 per-partition gauge 注册/注销 + 3 个实现方法 (`observersInIsrCount` / `observerCaughtUpCount` / `observerMaxLagMessages`) |
| `core/.../server/ReplicaManager.scala` | (未触及) | **新触及**: +3 个 broker 级聚合 gauge (含 `GaugeMetricNames` 注册, `removeMetrics` 自动清理) |
| `core/.../observer/ObserverIds.scala` | 文件读取 + 5s 缓存 | +`ObserverCount` gauge; 变更日志 info → **WARN 结构化审计行** (before/after/added/removed/source/epochMs) |
| `metadata/.../controller/ObserverReplicas.java` | 同语义 Java 版 | +controller 侧 WARN 结构化审计行 (同字段, 与 broker 侧成对) |
| `core/.../controller/PartitionStateMachine.scala` | ZK 选举排除 | (不变) |
| `metadata/.../controller/ReplicationControlManager.java` | KRaft 三处 gate | (不变) |

## JMX 指标 (7 个)

### broker 级

| MBean | 含义 | 告警建议 |
|---|---|---|
| `kafka.observer:type=ObserverMetrics,name=ObserverCount` | 当前 observer.ids 集合大小 | 与预期 observer 数比对, 不符 = 配置漂移 |
| `kafka.server:type=ReplicaManager,name=ObserversInIsrCount` | 本 broker 为 leader 的分区中, ISR 内 observer 副本总数 | **恒 0; >0 立即告警** — gate 被绕过或各节点 observer.ids 不一致, 最高价值异常检测指标 |
| `kafka.server:type=ReplicaManager,name=ObserverCaughtUpCount` | 已追上 leader 的 observer 副本计数之和 | 低于预期 = observer 掉队 |
| `kafka.server:type=ReplicaManager,name=ObserverLagMessages` | 各分区 observer 最大 LEO 落后条数的总和 | 持续增长 = observer 复制不动 |

### per-partition (leader 视角, 非 leader 时归 0; 对标 Confluent `kafka-replica-status.sh` 的 isObserver/isCaughtUp/lag 字段)

| MBean | 含义 |
|---|---|
| `kafka.cluster:type=Partition,name=ObserversInIsrCount,topic=X,partition=N` | 该分区 ISR 内 observer 数 (恒 0 预期) |
| `kafka.cluster:type=Partition,name=ObserverCaughtUpCount,topic=X,partition=N` | 追上的 observer 数 (isCaughtUp 语义与 ISR 判定一致: LEO 相等或 lag 时间 ≤ `replica.lag.time.max.ms`) |
| `kafka.cluster:type=Partition,name=ObserverLagMessages,topic=X,partition=N` | observer 最大 LEO 落后条数 |

实现要点 (最小侵入): 全部复用 Kafka 原生 `KafkaMetricsGroup.newGauge` 模式 (与 `UnderReplicated`/`InSyncReplicasCount` 同一注册点); gauge 只读现有 volatile 状态 (`partitionState` / `remoteReplicas` / `stateSnapshot`), 无锁无额外磁盘 IO; observer 集合判定走 5s 缓存。

## 审计日志 (WARN 级, 默认 log4j 配置可见)

observer 集合每次变更打一行结构化审计, broker 侧与 controller 侧成对出现:

```
OBSERVER AUDIT (broker): observer id set changed before=[4] after=[] added=[] removed=[4] source=file:/opt/kafka/observer.ids epochMs=1752...
OBSERVER AUDIT (controller): observer id set changed before=[4] after=[] added=[] removed=[4] source=file:/opt/kafka/observer.ids epochMs=1752...
```

- `removed=` 非空 = 晋升操作; `added=` 非空 = 降级操作
- `source=` 区分 `file:<path>` 与 `env:KAFKA_OBSERVER_BROKER_IDS` (文件缺失回退时可发现部署漂移)
- 集合稳定排序输出, 便于 grep/机器解析

## 应用与编译

```bash
cd kafka-3.7.1-src
git apply observer.patch
./gradlew :metadata:jar :core:jar :storage:jar -x test
```

部署与 v0.6 相同 (替换 core/metadata/storage 3 个 jar, controller 节点也要部署), 见 `../kafka-3.7.1-kraft/README.md`。

## 验证证据

`evidence/metrics_patch_evidence.md` (2026-07-20 东京真机编译)。
