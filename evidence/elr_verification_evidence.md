# ELR (Eligible Leader Replicas) × Observer 专项验证

日期: 2026-07-20 | 环境: 东京 loadgen（EC2 m7g.xlarge, Tokyo），Corretto JDK 17.0.19
拓扑: controller-only ×3 (100/101/102 @9491-9493, -Xmx256m) + broker ×3 (1/2/3 @9392/9394/9396, -Xmx512m)，observer=3（`KAFKA_OBSERVER_IDS_FILE` 指向数据目录下 observer.ids）
版本: Kafka 4.0.0（patched，metadata.version 4.0-IV3，手动开 ELR）与 Kafka 4.1.0（patched，4.1-IV1，ELR 默认开）

## 1. ELR 可用性结论

| 版本 | eligible.leader.replicas.version | 新集群默认 | 结论 |
|---|---|---|---|
| 4.0.0 (4.0-IV3) | SupportedMax=1, **Finalized=0** | 不默认开 | [事实] `kafka-features.sh upgrade --feature eligible.leader.replicas.version=1` 手动开启成功（`was upgraded to 1.`, Epoch 813） |
| 4.1.0 (4.1-IV1) | SupportedMax=1, **Finalized=1** | **默认开** | [事实] 新集群 format 后 describe 即为 1，零配置 |

即: "ELR 需 4.1+" 的说法不准确 —— 4.0 即可手动开启；4.1 是"默认开启"的分水岭。

## 2. 4.0.0 ELR 实验（手动开启后）

topic `elr-test`, RF3 assignment 1:2:3, `min.insync.replicas=2`, observer=3, acks=all 写 1000 条。

| 步骤 | describe 输出 (partition 0) | 判定 |
|---|---|---|
| 初始 | `Leader: 1 Isr: 1,2 Elr: LastKnownElr:` | 初始 ISR 排除 observer；ISR≥minISR 时 ELR 空（maybePopulateTargetElr 直接清空，符合源码 L542-546） |
| kill broker2 (ISR 成员) | `Leader: 1 Isr: 1 Elr: 2 LastKnownElr:` | ISR<minISR → 掉出者 2 进 ELR；**observer 3 不进 ELR** |
| 再 kill broker1 (末位 ISR/leader) | `Leader: none Isr: Elr: 1,2 LastKnownElr: 1` | ISR 全空 → 1 也进 ELR，lastKnownElr=[1]（末代 leader）；**observer 3 既不进 ELR 也不进 LastKnownElr**，且不当选（Leader: none，此时唯一存活 broker 就是 observer） |
| 重启 broker2 (ELR 成员) | `Leader: 2 Isr: 2 Elr: 1 LastKnownElr:` | ELR 成员回归即**干净选主**（非 unclean），2 当选，1 留在 ELR |

## 3. 4.1.0 ELR 实验（默认开启，全新集群）

topic `elr41`, RF3 assignment 1:2:3, minISR=2, observer=3, acks=all 写 3000 条（三节点 offset 3000/3000/3000 对齐——4.1 全量同步顺带实证）。

| 步骤 | describe 输出 | 判定 |
|---|---|---|
| 初始 | `Leader: 1 Isr: 1,2 Elr: LastKnownElr:` | 初始 ISR 排除 PASS；controller.log: `Filtered observers [3] from initial ISR [1, 2, 3] -> [1, 2]` |
| kill broker2 | `Leader: 1 Isr: 1 Elr: 2 LastKnownElr:` | 同 4.0，observer 不进 ELR |
| kill broker1 | `Leader: none Isr: Elr: 1,2 LastKnownElr: 1` | **observer 不进 ELR/LastKnownElr，不当选** |
| 显式 unclean election | `EligibleLeadersNotAvailableException` / `1 replica(s) could not be elected` | unclean.leader.election.enable=true 下 observer 仍被 LeaderAcceptor 拒绝 |
| 重启 broker2 | `Leader: 2 Isr: 2 Elr: 1 LastKnownElr:`，offset 3000 | ELR 干净恢复，**零数据丢失** |

## 4. observer 永不进 ELR —— 机制解释（代码路径）

[事实] 4.0.0/4.1.0 `PartitionChangeBuilder.maybePopulateTargetElr()`（4.0 L538-569）：ELR 候选集 = `targetElr ∪ partition.isr`（再减 targetIsr、减 unclean-shutdown 副本）；LastKnownElr 候选集 = 上述 candidateSet ∪ 旧 targetLastKnownElr。
[推断→已实证] observer 因 broker 侧 `canAddReplicaToIsr` + controller 侧 `ineligibleReplicas`("observer") 双闸门永不进 ISR ⇒ 永不进 candidateSet ⇒ **结构性地永不进 ELR 和 LastKnownElr**。§2/§3 真机四步全部吻合，无需对 ELR 路径打任何额外补丁。

## 5. canElectLastKnownLeader

- [事实] 4.0.0 L315-319 仍是缺取反版本（`if (isAcceptableLeader.test(...)) { log.trace(...不返回...) } return true;`）——无条件 `return true`。
- [事实] 4.1.0 L314-318 已修复（KAFKA-19522, commit e4e2dce2eb）: `if (!isAcceptableLeader.test(...)) { ...; return false; }`。
- 对 observer 的影响（4.0，ELR 手动开启时）:
  - [事实] 触发条件是 `targetElr 与 targetIsr 双空 + lastKnownElr.length==1`，选出的对象只能是 `partition.lastKnownElr[0]`。
  - [事实] observer 永不进 LastKnownElr（§2/§3 实测 LastKnownElr 始终=1，从不含 3）⇒ **该 bug 没有任何让 observer 当选的通路**——即便 bug 触发，选中的也是普通 broker。
  - [事实] 即使假想 lastKnownElr[0] 是 observer，patch 后的 `LeaderAcceptor.test()` 也在 `isAcceptableLeader` 之前先查 `ObserverReplicas.isObserver` 返回 false —— 但注意 4.0 的 bug 恰是"无视 isAcceptableLeader 结果返回 true"，即这一道闸在 canElectLastKnownLeader 里被 bug 绕过。真正兜底的是上一条：observer 结构性不可能出现在 lastKnownElr。
  - [推断] 对普通 broker，4.0+ELR 存在把 fenced 的末代 leader 选回来的上游风险（上游 4.1 已修）。**建议客户如需 ELR 直接用 4.1**，4.0 保持 ELR=0（默认）即完全规避。
- 本次实验未观测到 lastKnownLeader 通路触发（kill 的 broker 均为 fenced，且 ELR 非空时该路径不进入）。

## 6. 结论

1. **observer 永不进 ELR / LastKnownElr / 永不因 ELR 机制当选 leader** —— 4.0（手动开 ELR）与 4.1（默认 ELR）真机双实证，机制上由"永不进 ISR"结构性保证。
2. ELR 与 observer patch **零冲突**：patch 未触碰 PartitionChangeBuilder，ELR 全部行为来自原生代码。
3. ELR 反而是加分项：非 observer 的 ISR 成员挂掉后进 ELR，回归即干净选主零丢数（§2/§3 末行），与 observer 的"永不脏选主"叠加后，RF3(2+1observer) 拓扑在 minISR=2 下的容灾语义更完整。
4. 版本建议: 要 ELR 用 4.1.0（含 KAFKA-19522 修复且默认开）；4.0.0 不开 ELR 时行为与 3.7.1 全等。
