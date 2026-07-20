# KRaft Controller-Side Observer Patch — 真机验证证据 (v0.5)

- 日期: 2026-07-20 (JST, 东京 loadgen)
- 环境: Tokyo build host (EC2, m7g.xlarge), JDK 17.0.19, 单机多进程 KRaft 集群, 与现有 ZK 集群 (9092, 三台 broker EC2) 完全隔离
- 源码树: `/tmp/kafka-src` (Kafka 3.7.1 + 已有 ZK v3 patch), KRaft controller patch 直接叠加 → **同一套 jar 两模式通吃 (combined patch)**
- 验证后已完整清理: 全部 KRaft 测试进程 kill、`/tmp/kraft-v05` 删除、9292/9294/9296/9393 端口释放, ZK 集群未受任何影响 (实测 `ss` 无 kraft 端口、loadgen 无残留 kafka 进程)

## 一、Patch 内容 (controller 侧, metadata 模块, 4 处)

| # | 文件 / 位置 | 改动 |
|---|---|---|
| 1 | `metadata/.../controller/ObserverReplicas.java` (新, 157 行) | 纯 Java 静态工具类: 读 `observer.ids` 文件 (env `KAFKA_OBSERVER_IDS_FILE` 覆盖路径, 默认 `/opt/kafka/observer.ids`), 5s 时间缓存, 文件缺失回退 env `KAFKA_OBSERVER_BROKER_IDS`, 读失败保留上次值只 WARN 绝不抛; 语义与 core 侧 `ObserverIds.scala` 完全一致 |
| 2 | `ReplicationControlManager.buildPartitionRegistration` | 初始 ISR 过滤 observer (`filterInitialIsr`); 过滤后为空则保留原 ISR + WARN (新分区必须有 leader, fail-safe) |
| 3 | `ReplicationControlManager$LeaderAcceptor.test` (static nested class) | 开头加 `if (ObserverReplicas.isObserver(brokerId)) return false` — 一处覆盖全部 7 个选举入口, **含 unclean 选举** |
| 4 | `ReplicationControlManager.ineligibleReplicasForIsr` | observer → `new IneligibleReplica(brokerId, "observer")` — AlterPartition 二次防御 (broker 侧 gate 之外的 controller 侧兜底) |

三处 RCM 修改用精确锚点脚本 `apply_kraft_patch.py` 应用, 每个锚点要求恰好匹配 1 次, 全部命中:

```
OK: buildPartitionRegistration initial-ISR filter
OK: LeaderAcceptor.test observer gate
OK: ineligibleReplicasForIsr observer defense
DONE: all 3 RCM modifications applied
```

## 二、编译

```
./gradlew :metadata:jar :core:jar :storage:jar -x test
BUILD SUCCESSFUL in 1m 3s
```

- `metadata/build/libs/kafka-metadata-3.7.1.jar` 内确认含 `org/apache/kafka/controller/ObserverReplicas.class` (4918 bytes)
- ⚠️ **部署注意: metadata 是独立 jar**, KRaft 部署需替换 3 个 jar: `kafka_2.13-3.7.1.jar` (core) + `kafka-metadata-3.7.1.jar` + `kafka-storage-3.7.1.jar`, 且 **controller 节点也必须部署 patched jar + observer.ids 文件**

## 三、真机验证 (全部实测通过)

### 拓扑 A: 3 节点 combined 模式 (broker+controller, 9292/9294/9296 + 9393/9395/9397), observer.ids=`3`

**V1. 初始 ISR 过滤 (决定性验证 — 探针时此项失效, patch 后生效)**

建 topic `v05-test` (3 分区, RF3), 三个分区初始 ISR 均不含 3, leader 均非 3 — 即使 partition 0 的 assignment 首位是 3:

```
Partition: 0  Leader: 1  Replicas: 3,1,2  Isr: 1,2
Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,2
Partition: 2  Leader: 2  Replicas: 2,3,1  Isr: 2,1
```

controller 日志直接证据 (PartitionRecord 落盘时 isr 就已过滤):

```
INFO Observer id set changed: [] -> [3] (source: .../observer.ids) (org.apache.kafka.controller.ObserverReplicas)
INFO Filtered observers [3] from initial ISR [1, 2, 3] -> [1, 2] (org.apache.kafka.controller.ObserverReplicas)
INFO Replayed PartitionRecord ... PartitionRegistration(replicas=[1, 2, 3], ... isr=[1, 2], ... leader=1 ...)
```

**V2. observer 持续追数据但不进 ISR**: 生产 1000 条消息 + 等 12s (`replica.lag.time.max.ms=10000`), ISR 仍为 `1,2`; 三个节点数据目录大小一致 (node3 与 node1/2 各分区字节数相同) → observer 正常复制, 只是被挡在 ISR 外 (broker 侧 AlterPartition gate + controller 侧 `ineligibleReplicasForIsr` 双重防御生效)。

**V3. kill broker 1 → 选举避开 observer**: leader 全部落到 2, 绝不选 3:

```
Partition: 0  Leader: 2  Isr: 2      (原 leader 1)
Partition: 1  Leader: 2  Isr: 2
Partition: 2  Leader: 2  Isr: 2
```

**V4. broker 1 重启 → 正常回 ISR, observer 3 仍被挡**: ISR 恢复 `2,1` (不含 3)。

### 拓扑 B: 1 controller-only (node 100 @9393) + 3 broker-only (9292/9294/9296) — 模拟生产分离部署, controller 进程独立设置 `KAFKA_OBSERVER_IDS_FILE`

**V5. controller-only 节点上 patch 生效**: 建 `v05b-test` (RF3) → `Isr: 1,2` (observer 3 被 controller-only 进程过滤, 证明 observer.ids 部署到 controller 节点这一要求真实成立)。

**V6. unclean 选举也不选 observer (LeaderAcceptor 全入口覆盖的决定性证据)**:
`unclean.leader.election.enable=true` 后 kill broker 1、再 kill broker 2 (此时唯一存活 broker = observer 3, 且它有完整数据副本):

```
Partition: 0  Leader: none  Replicas: 1,2,3  Isr: 2
```

**leader=none 而不是 leader=3** — 即使开启 unclean 且 observer 是唯一存活且数据完整的副本, controller 也拒绝选它 (可用性换一致性, 符合 observer 语义: 永不接写)。broker 3 此时确认存活可服务 API 请求。

**V7. 灾后恢复**: 重启 broker 1/2 → `Leader: 2, Isr: 2,1`, 集群自愈。

**V8. 动态晋升/降级 (5s 缓存热生效, 无需重启任何进程)**:
- 晋升: 清空 observer.ids → ~20s 内 `Isr: 2,1,3` (3 进 ISR); controller 日志 `Observer id set changed: [3] -> []`
- 降级: 写回 `3` → ~25s 内 `Isr: 2,1` (3 被踢出 ISR, broker 侧降级钩子 + isr-expiration 生效)

## 四、Canonical Patch

`patches/kafka-3.7.1-kraft/observer.patch` — **combined patch (ZK + KRaft 两模式通吃)**, 打在 vanilla Kafka 3.7.1 源码树上一次成型:

```
core/src/main/scala/kafka/cluster/Partition.scala              |  17 ++-  (ZK/broker 侧 v3)
core/src/main/scala/kafka/controller/PartitionStateMachine.scala |   9 +-  (ZK controller 侧 v3)
core/src/main/scala/kafka/observer/ObserverIds.scala           |  76 +++  (ZK/broker 侧 v3, 新文件)
metadata/.../controller/ObserverReplicas.java                  | 157 +++  (KRaft controller 侧, 新文件)
metadata/.../controller/ReplicationControlManager.java         |  18 ++-  (KRaft controller 侧, 3 处)
5 files changed, 271 insertions(+), 6 deletions(-)
```

命名说明: 目录叫 `kafka-3.7.1-kraft` 但 patch 是全量 combined — 同一套编译产物在 ZK 模式和 KRaft 模式都具备完整 observer 能力 (broker 侧 3 个 hook 在 KRaft 下免费生效, 见 `kraft_probe_evidence.md`; controller 侧 ZK 走 PartitionStateMachine.scala, KRaft 走 ReplicationControlManager.java, 互不干扰)。

## 五、结论

| 验证项 | 结果 |
|---|---|
| 初始 ISR 过滤 (探针时失效项) | ✅ isr=[1,2] 落盘, 有 controller 日志直接证据 |
| observer 复制数据但不进 ISR | ✅ 数据目录字节级一致, ISR 持续排除 |
| 正常 failover 不选 observer | ✅ |
| **unclean 选举不选 observer** | ✅ leader=none 而非 leader=3 |
| controller-only 分离部署生效 | ✅ (observer.ids 须部署到 controller 节点) |
| 动态晋升/降级热生效 | ✅ 5s 缓存, ~20s 端到端 |
| 编译 + jar 含新类 | ✅ BUILD SUCCESSFUL, ObserverReplicas.class 在 metadata jar |
| 环境清理 | ✅ 进程/数据目录/端口全清, ZK 集群未动 |

---

# 六、完整能力矩阵验证 (v0.5 收尾轮, 2026-07-20 第二次独立复跑)

- 环境: 同上 (东京 loadgen 单机 3 节点 combined KRaft, 9292/9294/9296 + 9393/9395/9397)
- 部署: vanilla 3.7.1 dist + 替换 3 个 patched jar (core/metadata/storage), `KAFKA_OBSERVER_IDS_FILE=/tmp/kraft-v05/observer.ids`, `-Xmx512m/进程`, `replica.lag.time.max.ms=10000`
- observer = node 3; 与上一轮 (第三节) 完全独立: 重新 format 新 cluster id `DYAlfGwBTiqYZRh7vco9DQ`

## 能力矩阵 (8 项)

| # | 能力项 | 预期 | 实测 | 结果 |
|---|---|---|---|---|
| 1 | 初始 ISR 排除 | 建 topic RF3, Isr 不含 3 | `cap-test` 3 分区: `Isr: 1,2 / 1,2 / 2,1`, 即使 p0 assignment 首位是 3 (`Replicas: 3,1,2`) 也 `Leader: 1`; controller 日志 `Observer id set changed: Set() -> Set(3)` | ✅ |
| 2 | 全量同步 | 写 5000 条, observer log end offset=5000 | `kafka-get-offsets` 三节点 p1 均 `5000`; 数据目录字节级一致 (3 分区各 ~21MB, node3 == node1/2) | ✅ |
| 3 | 不进 ISR 持续性 | 写入过程中多次 describe, ISR 恒不含 3 | 写入中 2 次 + 写完等 12s (>lag.max 10s) 1 次, ISR 恒 `1,2`/`2,1` | ✅ |
| 4 | 晋升 (≤30s) | 清空 observer.ids → 3 进 ISR | **t+4s** 三个分区全部 `Isr` 含 3 (5s 缓存 + follower 本就 caught-up, AlterPartition 立即放行); 日志 `Set(3) -> Set()` | ✅ |
| 5 | 降级 (≤45s) | 写回 `3` → 3 出 ISR | **follower 场景 t+9s** 出 ISR (`2,1,3`→`2,1`) — 远快于 ZK 版 (broker 降级钩子 + controller `ineligibleReplicasForIsr` 双重生效); **leader 场景见下方发现** | ✅ (follower) |
| 6 | 晋升后可当 leader | 晋升后 3 能当选 leader 且服务写入 | 晋升后 `kafka-leader-election --election-type preferred` → `Leader: 3`; 经 leader=3 写 200 条成功, 全量消费 300 条对账正确; 再 kill 非 observer 的 node1 (quorum 2/3 存活): `Leader: 3, Isr: 2,3`, 继续写入成功; node1 重启回 `Isr: 2,3,1` | ✅ |
| 7 | KRaft 特有: 新建 topic 立即 fetch | ZK 模式的"运行中 observer 不 fetch 新 topic"坑在 KRaft 不存在 | 新建 `fresh-topic` 后 **t+1s** node3 分区目录已出现; 写 100 条后 node3 与 node1 log 目录字节一致 (20972696) — 无需任何 workaround | ✅ |
| 8 | AlterPartition 二次防御 | 代码 review (broker gate 使其难以黑盒触发) | patch hunk 确认: `ineligibleReplicasForIsr` 中 `if (ObserverReplicas.isObserver(brokerId)) ineligibleReplicas.add(new IneligibleReplica(brokerId, "observer"))` 在 "not registered"/"shutting down" 检查之前; 运行时旁证 = 矩阵 #3 (12s>lag.max 仍不进 ISR) | ✅ (设计验证) |

## ⚠️ 新发现: KRaft 下 "observer 正在当 leader 时降级" 需要额外一步

复跑时故意构造了最刁钻的降级场景: **node3 晋升 → 当选 leader → 再写回 observer.ids**。结果:

- node3 **持续 85s+ 保持 leader=3, isr=2,3,1 不变** — KRaft 的 ISR 收缩由 leader 发起 (AlterPartition), 而 leader 永远不会把自己从 ISR 里剔掉; controller 侧 `LeaderAcceptor`/`ineligibleReplicasForIsr` 只在"选举/ISR 变更提案"时触发, 不会主动废黜在任 leader (ZK 版 controller 有主动重选举路径, KRaft 没有等价物, 这是两种模式的真实行为差异)。
- 此时 `kafka-leader-election preferred` 返回 `Valid replica already elected` (也不会动它)。
- **正确操作: 滚动重启该节点** (SIGTERM node3) → leadership 立即移交 `Leader: 1, Isr: 2,1`; node3 重启回来后作为 observer 被持续挡在 ISR 外 (等 15s+35s 两次确认), 且复制不受影响 (再写 100 条, node3 offset 同步到 100)。

**Runbook 影响**: KRaft 下降级一个正在当 leader 的 broker, 流程必须是 `写 observer.ids → 滚动重启该 broker`; 若它只是 follower 则热生效 (~9s), 无需重启。(该场景在生产不常见 — 正常运维顺序是先降级再考虑晋升, observer 平时永远不是 leader。)

## 清理

- 全部 KRaft 测试进程 kill (`ps` 确认 0 个 kafka 进程残留)
- `/tmp/kraft-v05` 删除, 9292/9294/9296/9393/9395/9397 端口全部释放 (`ss` 确认)
- /tmp 剩 7.2G, ZK 集群 (三台 broker EC2) 全程未触碰
