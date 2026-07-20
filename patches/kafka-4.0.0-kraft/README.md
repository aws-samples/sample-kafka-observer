# Kafka 4.0.0 KRaft Observer Patch

从 3.7.1 combined patch (`patches/kafka-3.7.1-kraft/observer.patch`) 移植到 Apache Kafka 4.0.0 (tag `4.0.0`, commit `985bc99`)。纯 KRaft patch——4.0 已删除全部 ZK 代码。

## 应用方法

```bash
git clone --depth 1 --branch 4.0.0 https://github.com/apache/kafka.git
cd kafka
git apply observer.patch
./gradlew :metadata:jar :core:jar :storage:jar -x test
```

编译环境: JDK 17 (Corretto 17.0.19), Gradle wrapper 8.10.2, Scala 2.13 (4.0 唯一支持的 Scala 版本)。
[事实] 2026-07-20 在东京 loadgen（EC2, Tokyo）真机编译通过 (BUILD SUCCESSFUL in 2m 16s, `-Xmx2g` 够用)。

## 与 3.7.1 版 patch 的差异

| 文件 | 3.7.1 | 4.0.0 | 变化 |
|---|---|---|---|
| `core/.../cluster/Partition.scala` (3 hunks: canAddReplicaToIsr / shouldWaitForReplicaToJoinIsr / getOutOfSyncReplicas) | @1044 / @1176 / @1312 | @1036 / @1170 / @1310 | 仅行号漂移 (-8/-13/-11)，锚点代码逐字一致，3-hunk 全部干净 apply |
| `core/.../controller/PartitionStateMachine.scala` (2 hunks: ZK 初始 ISR + ZK unclean 选举) | 有 | **删除** | 4.0 移除全部 ZK controller (KAFKA-17613 等)，该文件不存在，2 个 hunk 整体弃用 |
| `core/.../observer/ObserverIds.scala` (新文件) | 76 行 | 76 行 | 原样复制。依赖 `kafka.utils.Logging` 在 4.0 仍存在 |
| `metadata/.../controller/ObserverReplicas.java` (新文件) | 157 行 | 157 行 | 原样复制。metadata 模块结构未变 |
| `metadata/.../controller/ReplicationControlManager.java` (3 hunks: buildPartitionRegistration / ineligibleReplicas / LeaderAcceptor.test) | @824 / @1260 / @2272 | @855 / @1337 / @2445 | 仅行号漂移 (+31/+74/+166)，上下文代码一致，3-hunk 全部干净 apply |

净结果: **10 hunks → 8 hunks**（去掉 2 个 ZK-only hunk），无一行需要手工改写；`git apply` 全部自动偏移成功。

## canElectLastKnownLeader 复核 (2026-07-20)

[事实] 4.0.0 的 `PartitionChangeBuilder.canElectLastKnownLeader()` (L315-319) 与 3.7.1 相同——`if (isAcceptableLeader.test(lastKnownElr[0]))` **仍缺取反**，即"last known leader 存活"时打出 "not alive" 的 trace 日志后照样 `return true`；反之 leader 不存活也 `return true`。上游在 4.1.0 修复 (KAFKA-19522, commit `e4e2dce2eb`, 2025-07-20, 改为 `if (!isAcceptableLeader.test(...)) return false`)。

[推断] 对 observer patch 的影响: 该路径仅在 ELR 开启 + `useLastKnownLeaderInBalancedRecovery` + ISR/ELR 双空时走到。observer 永不进 ISR，也就永不进 ELR/lastKnownElr（`maybePopulateTargetElr` 候选集 = ELR ∪ ISR，见 4.0.0 L555-556），故此 bug 不会让 observer 当 leader；但在 4.0 上它可能把 fenced 的普通 broker 选为 leader。移植 4.1 时随上游自然消失。

## 补丁产物 (2026-07-20 loadgen 编译)

- `/tmp/kafka-40/core/build/libs/kafka_2.13-4.0.0.jar` (含 `kafka/observer/ObserverIds.class`)
- `/tmp/kafka-40/metadata/build/libs/kafka-metadata-4.0.0.jar` (含 `org/apache/kafka/controller/ObserverReplicas.class`)
- `/tmp/kafka-40/storage/build/libs/kafka-storage-4.0.0.jar`

详细证据: `evidence/kafka40_port_evidence.md`
