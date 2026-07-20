# v0.7 Metrics Patch — 编译验证证据

日期: 2026-07-20 (JST) · 环境: Tokyo build host (EC2, m7g.xlarge), `/tmp/kafka-src` = Kafka 3.7.1 源码树 (v0.6 combined patch 已打) · 方法: 真机直接改源码 + gradle 编译

## 1. 结论

v0.7 metrics patch 在 v0.6 combined patch 之上叠加 **7 个 JMX gauge + 2 侧结构化审计日志**, 于东京真机编译通过:

```
BUILD SUCCESSFUL in 53s
55 actionable tasks: 3 executed, 52 up-to-date
```

命令: `./gradlew :core:jar :metadata:jar :storage:jar -x test` (log: loadgen `/tmp/v07_build.log`, 0 errors)

产物 (patched jars):

```
-rw-rw-r-- 5062790 Jul 20 15:06 core/build/libs/kafka_2.13-3.7.1.jar
-rw-rw-r--  852363 Jul 20 15:01 metadata/build/libs/kafka-metadata-3.7.1.jar
-rw-rw-r--  361610 Jul 20 02:00 storage/build/libs/kafka-storage-3.7.1.jar   (storage 未改, up-to-date)
```

canonical patch: `patches/kafka-3.7.1-kraft-v07/observer.patch` (6 文件, +384/-8)。
`git apply --check` 对 vanilla HEAD 提取树验证: **PATCH APPLIES CLEANLY TO VANILLA**。

## 2. 设计: 先研究原生机制, 再最小侵入复用

真机源码调研结论 (全部来自 /tmp/kafka-src 实际代码, 非记忆):

| 原生机制 | 位置 | v0.7 复用方式 |
|---|---|---|
| per-partition gauge: `metricsGroup.newGauge("UnderReplicated"/..., tags)`, tags = topic/partition; 注销于 `Partition.removeMetrics` | `Partition.scala` L356-363, L153-160 | 3 个 observer gauge 在同一注册点注册、同一 removeMetrics 注销 |
| broker 级 gauge: `metricsGroup.newGauge(XxxMetricName, () => leaderPartitionsIterator....)`, 指标名列入 `GaugeMetricNames` 供 `removeMetrics()` 统一清理; **指标名常量必须加进 L27 的显式 import** (第一次编译失败即因漏此, 3 errors → 补 import 后通过) | `ReplicaManager.scala` L27, L193-216, L332-341, L2636 | 3 个聚合 gauge 同模式注册 + 纳入 GaugeMetricNames + 补 import |
| caught-up 语义: `ReplicaState.isCaughtUp(leaderEndOffset, currentTimeMs, replicaMaxLagMs)` = LEO 相等 或 lag 时间 ≤ replica.lag.time.max.ms — 与 ISR 判定同一函数 | `Replica.scala` L67-73 | ObserverCaughtUpCount 直接调用, 语义与 ISR 完全一致 |
| 自定义 JMX domain: `KafkaMetricsGroup.explicitMetricName(group, type, name, tags)` (public static) | `KafkaMetricsGroup.java` L54 | `kafka.observer:type=ObserverMetrics,name=ObserverCount` 用此 API 注册进 `KafkaYammerMetrics.defaultRegistry()` |

## 3. 指标清单 (7 个)

| MBean | 层级 | 语义 |
|---|---|---|
| `kafka.observer:type=ObserverMetrics,name=ObserverCount` | broker | observer.ids 集合大小 (走 5s 缓存, 零额外 IO) |
| `kafka.server:type=ReplicaManager,name=ObserversInIsrCount` | broker (leader 视角聚合) | **理论恒 0; >0 = gate 被绕过/文件不一致, 最高价值告警指标** |
| `kafka.server:type=ReplicaManager,name=ObserverCaughtUpCount` | broker | 追上 leader 的 observer 副本计数和 |
| `kafka.server:type=ReplicaManager,name=ObserverLagMessages` | broker | 各分区 observer 最大 LEO 落后条数之和 |
| `kafka.cluster:type=Partition,name=ObserversInIsrCount,topic=,partition=` | partition | 该分区 ISR 内 observer 数 |
| `kafka.cluster:type=Partition,name=ObserverCaughtUpCount,topic=,partition=` | partition | 追上的 observer 数 (isCaughtUp 原生语义) |
| `kafka.cluster:type=Partition,name=ObserverLagMessages,topic=,partition=` | partition | observer 最大 LEO 落后条数 |

性能: gauge 求值只读现有 volatile 状态 (`partitionState`/`remoteReplicas`/`stateSnapshot`), 无锁; observer 判定走 ObserverIds 5s 缓存; broker 级聚合仅在 JMX 拉取时遍历 leader 分区 (与原生 `UnderMinIsrPartitionCount` 同代价)。

## 4. 审计日志 (broker + controller 成对)

变更行为从 info 升级为 **WARN 结构化审计行** (默认 log4j 可见), 字段: before/after/added/removed/source/epochMs, 集合稳定排序:

```
OBSERVER AUDIT (broker): observer id set changed before=[4] after=[] added=[] removed=[4] source=file:/opt/kafka/observer.ids epochMs=...
OBSERVER AUDIT (controller): observer id set changed before=[4] after=[] added=[] removed=[4] source=file:/opt/kafka/observer.ids epochMs=...
```

`removed` 非空 = 晋升, `added` 非空 = 降级; `source` 区分 file/env 回退 (可发现部署漂移)。金融审计诉求: 谁看日志都能重建 observer 集合的完整变更史。

## 5. 完整性检查

- 原有 12 个 `OBSERVER PATCH` 标记行: v0.6 patch 与 v0.7 patch 逐字比对 **MARKERS IDENTICAL** (功能 hook 零改动)
- 新增代码全部带 `OBSERVER METRICS` 标记: patch 内 19 处标记行, 3 处 `OBSERVER AUDIT` 日志前缀
- 触及文件: v0.6 的 5 文件 + 新触及 `ReplicaManager.scala` (仅 metrics, 不碰复制逻辑)
- 源码树保留在 loadgen `/tmp/kafka-src` (staged, `git diff HEAD` 可复现 patch), 供 verify 阶段做 JMX 真机验证

## 6. 已知边界 (实事求是)

- **编译验证 ✅ / 运行时 JMX 读数验证 ⏳** — gauge 数值正确性 (ObserverCount/CaughtUp/Lag 随负载与晋升降级变化) 需在 KRaft 3 节点集群上用 `kafka-run-class kafka.tools.JmxTool` 或 jmxterm 实测, 属 verify 阶段
- per-partition `ObserverLagMessages` 为 LEO 条数差, 不是时间 lag; 时间维度可看 caught-up (基于 `lastCaughtUpTimeMs`)。Confluent 的 `lastCaughtUpLagMs` 等价物可由 `ObserverCaughtUpCount` 是否等于 observer 总数推断, 未单独出指标 (避免每 replica 一个 MBean 的基数爆炸)
- `kafka.observer` domain 的 ObserverCount gauge 注册发生在 ObserverIds object 首次初始化时 (首个 fetch/ISR 判定触发), broker 完全空闲时 MBean 可能尚未出现 — 属惰性初始化的自然行为
