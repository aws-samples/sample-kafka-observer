# Kafka 4.0.0 Observer Patch 移植证据

日期: 2026-07-20 | 环境: 东京 loadgen（EC2 m7g.xlarge, Tokyo；Corretto JDK 17.0.19, tmpfs /tmp 7.2G 可用）
源码: `git clone --depth 1 --branch 4.0.0 https://github.com/apache/kafka.git /tmp/kafka-40` → commit `985bc99 "Bump version to 4.0.0"`
输入: 3.7.1 combined patch (`patches/kafka-3.7.1-kraft/observer.patch`, 10 hunks / 5 文件) scp 至 `/tmp/observer.patch`

## 1. Apply 结果（逐 hunk）

[事实] `git apply --3way` 整体失败（shallow clone 缺 blob + PartitionStateMachine.scala 不存在），改为逐文件 `git apply --verbose --include=<file>`：

| # | 文件 / hunk | 结果 |
|---|---|---|
| 1 | Partition.scala hunk1 (canAddReplicaToIsr, 3.7.1@1044) | **succeeded at 1036 (offset -8)** |
| 2 | Partition.scala hunk2 (shouldWaitForReplicaToJoinIsr, @1176) | **succeeded at 1170 (offset -13)** |
| 3 | Partition.scala hunk3 (getOutOfSyncReplicas, @1312) | **succeeded at 1310 (offset -11)** |
| 4 | PartitionStateMachine.scala hunk1 (ZK 初始 ISR) | **跳过** — 文件在 4.0 不存在（ZK controller 已删除） |
| 5 | PartitionStateMachine.scala hunk2 (ZK unclean 选举) | **跳过** — 同上 |
| 6 | ObserverIds.scala (新文件, 76 行) | **applied cleanly**（原样复制） |
| 7 | ObserverReplicas.java (新文件, 157 行) | **applied cleanly**（原样复制） |
| 8 | ReplicationControlManager.java hunk1 (buildPartitionRegistration, 3.7.1@824) | **succeeded at 855 (offset +31)** |
| 9 | RCM hunk2 (ineligibleReplicasForIsr 分支, @1260) | **succeeded at 1337 (offset +74)** |
| 10 | RCM hunk3 (LeaderAcceptor.test, @2272) | **succeeded at 2445 (offset +166)** |

结论: **8/8 可用 hunk 全部干净 apply，仅行号漂移，零手工改写**；2 个 ZK-only hunk 按计划弃用。预期"broker 侧 3 锚点逐字一致"得到实证；metadata 侧 LeaderAcceptor / buildPartitionRegistration / ineligibleReplicas 结构在 4.0 未变（grep 复核了三处 apply 后的上下文，均与 3.7.1 语义相同位置）。

## 2. 依赖复核

- [事实] `core/src/main/scala/kafka/utils/Logging.scala` 在 4.0.0 存在 → ObserverIds.scala 依赖满足。
- [事实] ObserverIds.scala 中 `Try(s.toInt).toOption`（为 Scala 2.12 兼容写法）在 2.13-only 的 4.0 仍合法编译。
- [事实] metadata 模块包路径 `org.apache.kafka.controller` 未变，ObserverReplicas.java 无需修改。

## 3. 编译结果

[事实] 命令: `./gradlew --no-daemon -Dorg.gradle.jvmargs="-Xmx2g -Xss4m -XX:+UseParallelGC" :metadata:jar :core:jar :storage:jar -x test`
（gradle wrapper 8.10.2；默认 jvmargs 是 -Xmx4g，loadgen 只有 ~3.4G available，压到 2g 通过）

```
BUILD SUCCESSFUL in 2m 16s
53 actionable tasks: 53 executed
```

产物（loadgen，保留供下阶段验证）:

| jar | 观测 |
|---|---|
| `/tmp/kafka-40/core/build/libs/kafka_2.13-4.0.0.jar` | 含 `kafka/observer/ObserverIds.class` + `ObserverIds$.class` [unzip -l 实证] |
| `/tmp/kafka-40/metadata/build/libs/kafka-metadata-4.0.0.jar` | 含 `org/apache/kafka/controller/ObserverReplicas.class` [unzip -l 实证] |
| `/tmp/kafka-40/storage/build/libs/kafka-storage-4.0.0.jar` | 生成正常 |

## 4. Canonical patch 生成与验证

- [事实] `git add -N` 两个新文件后 `git diff > /tmp/kraft4-observer.patch`（333 行, 4 个 diff, 8 hunks）。
- [事实] 在 pristine 4.0.0 worktree (`git worktree add /tmp/kafka-40-verify HEAD`) 上 `git apply --check` → **CLEAN-APPLY-OK**。
- 已 scp 回本地: `patches/kafka-4.0.0-kraft/observer.patch` (+ README.md)。

## 5. canElectLastKnownLeader 复核

[事实] 4.0.0 `PartitionChangeBuilder.java` L315-319:

```java
if (isAcceptableLeader.test(partition.lastKnownElr[0])) {
    log.trace("... but last known leader is not alive. ...");
}
return true;
```

与 3.7.1 (L319-323) 逐字同构——**疑似缺取反的 bug 在 4.0.0 仍在**：无论 lastKnownElr[0] 是否 acceptable 都 `return true`（且日志语义颠倒：leader 存活时反而打 "not alive"）。

[事实] 上游已在 4.1.0 修复: commit `e4e2dce2eb` (KAFKA-19522 "avoid electing fenced lastKnownLeader", #20200, 2025-07-20)，4.1.0 代码为 `if (!isAcceptableLeader.test(partition.lastKnownElr[0])) { ...; return false; }`。这确认了 3.7.1 时的判断——确实是上游 bug，非有意行为。

[推断] 对 observer 的影响评估：
- 触发前提: ELR enabled + `useLastKnownLeaderInBalancedRecovery=true` + targetISR 与 targetELR 双空 + `lastKnownElr.length==1`。
- observer 永不进 ISR（broker 侧 canAddReplicaToIsr + controller 侧 ineligibleReplicas 双重闸门）⇒ 永不进 ELR（4.0.0 `maybePopulateTargetElr` L555-556: 候选集 = targetElr ∪ partition.isr）⇒ 永不成为 lastKnownElr 成员 ⇒ **此 bug 不构成 observer 被选为 leader 的通路**（待 4.1 阶段用 ELR 集群实测佐证）。
- 但在 4.0 集群里它可能把 fenced/不存活的普通 broker 选为 leader，属上游风险，与本 patch 无关；4.1 移植时随上游修复自然消失。
- 另注: 4.0.0 中 ELR 仍非默认（`eligible.leader.replicas.version=1` 默认启用是 4.1 新集群行为），默认配置下该路径不触发。

## 6. 遗留

- `/tmp/kafka-40`（含已 patch 源码与编译产物）保留在 loadgen，供下阶段（4.0 三节点 KRaft + observer 生命周期验证）使用。`/tmp/kafka-src` (3.7.1) 未动。
- 4.1.0 移植预期：broker 侧与 metadata 侧锚点大概率再次干净 apply，但需重点验证 ELR 默认开启下 observer 不进 ELR（本文件第 5 节推断需实证）。

---

## 7. Kafka 4.0 真机能力矩阵（2026-07-20, 东京 loadgen）

拓扑: **controller-only ×3 (node 100/101/102, 端口 9491-9493, -Xmx256m) + broker ×3 (node 1/2/3, 端口 9392/9394/9396, -Xmx512m)**，共 6 进程。分离拓扑使 "kill 2 个 broker" 不伤 controller quorum，unclean 拒选实验可行。
部署方式: 官方 binary dist `kafka_2.13-4.0.0.tgz`（libs 内 jar 命名与 3.7.1 同构: `kafka_2.13-4.0.0.jar` / `kafka-metadata-4.0.0.jar` / `kafka-storage-4.0.0.jar`），用 §3 编译产物替换 3 个 jar，`unzip -l` 确认 ObserverIds/ObserverReplicas class 在位。
observer=3，`KAFKA_OBSERVER_IDS_FILE=/tmp/kraft-v06/observer.ids`。metadata.version format 为 **4.0-IV3**。topic `obs-test` 3 分区 RF3（assignment 1:2:3 / 2:3:1 / 3:1:2），`unclean.leader.election.enable=true`。

| # | 能力 | 结果 | 证据 |
|---|---|---|---|
| 1 | 初始 ISR 排除 | **PASS** | 3 个分区 Isr 均为 {1,2}，无 3；controller.log: `Filtered observers [3] from initial ISR [1, 2, 3] -> [1, 2]` ×3（org.apache.kafka.controller.ObserverReplicas） |
| 2 | 全量同步 | **PASS** | acks=all 写 3000 条；三 broker `kafka-get-offsets` 完全一致 (1520/888/592)，observer 数据目录字节级同大小 |
| 3 | 晋升 | **PASS, ~4.1s** | 清空 observer.ids → partition0 Isr {1,2}→{1,2,3} 实测 4.10s（5s 缓存 TTL 内），全部 3 分区随后进 ISR |
| 4 | 降级 | **PASS, ~12.2s** | 写回 "3" → follower 分区 partition0 Isr {1,2,3}→{1,2} 实测 12.24s（5s 缓存 + isr-expiration 周期）；观察: **leader 场景不自动降级**（partition2 上 node3 是 leader 时留在 ISR，getOutOfSyncReplicas 只在 leader 上跑、leader 不会 shrink 自己）——与 3.7.1 行为一致，运维流程需先转移 leader（重启该 broker 即可，重启后 Isr {1,2} 稳定不回归） |
| 5 | 晋升后 preferred election | **PASS** | 晋升后 `kafka-leader-election.sh --election-type preferred` → partition2 Leader: 3，console-producer acks=all 写入成功 |
| 6 | unclean 拒选 | **PASS** | unclean.leader.election.enable=true，kill broker1+2 → 三分区 **Leader: none**（存活的 observer 拒不当选）；显式 `--election-type unclean` 抛 `EligibleLeadersNotAvailableException`；broker1/2 回归后自动恢复 Leader，零数据丢失 |

补充观察:
- [事实] observer 若已是 leader，preferred election 返回 "Valid replica already elected"（LeaderAcceptor 只拦"新当选"，不废黜现任 leader）；与 3.7.1 语义一致。降级 runbook 保持: 先 demote 文件 + 重启/转移 leader。
- [事实] broker 侧 `kafka.observer.ObserverIds$` 与 controller 侧 `org.apache.kafka.controller.ObserverReplicas` 的 "Observer id set changed" 日志均出现（分别在 broker*.log 与 dist logs/controller.log），双侧动态加载正常。
- [事实] 4.0 的 `kafka-topics.sh --describe` 原生输出 `Elr:` / `LastKnownElr:` 列，ELR 观测无需 metadata-shell。

## 8. ELR 与 4.0（详见 elr_verification_evidence.md）

- [事实] 4.0 **支持** ELR: `kafka-features.sh describe` → `eligible.leader.replicas.version SupportedMaxVersion: 1`，但新集群 (4.0-IV3) 默认 `FinalizedVersionLevel: 0`（**不默认开**）；`kafka-features.sh upgrade --feature eligible.leader.replicas.version=1` 手动开启成功。
- [事实] 4.1 新集群 (4.1-IV1) 默认 `FinalizedVersionLevel: 1`（**默认开**），印证 "ELR 默认启用是 4.1 新集群行为"。
- [事实] ELR 开启后 observer 永不进 ELR/LastKnownElr — 4.0 与 4.1 真机均实证，见 elr_verification_evidence.md。

## 9. 4.1.0 移植（超前完成）

- [事实] `git clone --depth 1 --branch 4.1.0` (commit `13f7025`)，4.0 canonical patch `git apply --check` **8/8 hunk 全过**（Partition.scala offsets +4/-1/-1；RCM offsets -1/-1/+30），零手工改写。
- [事实] 编译: 同 jvmargs，`BUILD SUCCESSFUL in 3m 9s`；产物 `kafka_2.13-4.1.0.jar`（含 ObserverIds×2 class）、`kafka-metadata-4.1.0.jar`（含 ObserverReplicas）。
- [事实] canonical patch `patches/kafka-4.1.0-kraft/observer.patch`（333 行, 4 diff）在 pristine worktree `git apply --check` 通过；与 4.0 patch 逐字一致（仅 hunk 行号漂移）。
- [事实] 4.1.0 `PartitionChangeBuilder.canElectLastKnownLeader` 已含上游修复（L314: `if (!isAcceptableLeader.test(...)) { ...; return false; }`）——3.7.1/4.0.0 的缺取反 bug 在 4.1 消失，与 §5 预判一致。

## 10. 清理记录（2026-07-20）

- 全部 12 个实验进程（4.0 六进程 + 4.1 六进程）已 kill，9392/9394/9396/9491-9493 端口释放确认。
- `/tmp/kraft-v06`、`/tmp/kraft-v06-41`、`/tmp/kafka40-dist`、`/tmp/kafka41-dist` 已删除。
- `/tmp/kafka-40`、`/tmp/kafka-41` 的 build/.gradle 已删（204M→98M / 103M），patched 源码与 `/tmp/kraft4-observer.patch`、`/tmp/kraft41-observer.patch` 保留。
- `/tmp/kafka-src` (3.7.1) 未动；实验前已存在的 mm2 相关 java 进程未动。
