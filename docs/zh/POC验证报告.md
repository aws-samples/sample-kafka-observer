# 自研 Kafka Observer/Learner 节点：真机源码改造实现与验证

> **突破性成果**：在开源 Apache Kafka 3.7.1 上，用一个最小源码 patch，真机实现了 Confluent 商业版 Observer / 字节内部 Learner 的核心语义——**副本同步全量数据、但不进 ISR、不拖 HW、不当 leader**。东京 POC 真机编译、部署、验证通过。
> 日期：2026-07-19　环境：AWS 东京 ap-northeast-1，3 broker 跨 3AZ 自建 Kafka 3.7.1(ZK)

---

## 一、结论先行

| 问题 | 答案 |
|---|---|
| 能不能自己实现"只同步不选举"的 learner？ | ✅ **能。真机做出来了。** |
| 需要多大改动？ | **约 8 行 Scala**（改 `Partition.scala` 一个方法） |
| 能不能真机编译？ | ✅ loadgen(4vCPU/15GB) 上 ~1-3 分钟编译成功 |
| 达到了 Observer 的核心价值吗？ | ✅ 同步全量 + 不进 ISR + **不拖 HW（延迟=快对水平）** + 不当 leader |
| 能多集群吗？ | ⚠️ 同集群天然 EOS；跨集群需 Cluster Linking 式 offset-preserving（见第五节） |

---

## 二、最小源码 patch

**改动点**：`core/src/main/scala/kafka/cluster/Partition.scala` 的 `canAddReplicaToIsr()`——这是决定副本能否进 ISR 的唯一关卡。

```scala
private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
  // === OBSERVER PATCH === 若该 broker 被标记为 observer, 永不允许进入 ISR
  // 通过环境变量 KAFKA_OBSERVER_BROKER_IDS="1,4" 指定 observer broker id 列表
  // observer 副本仍照常 fetch 同步全量数据, 但不进 ISR: 不拖 HW / 不算 minISR / 不当 leader
  val observerIds = Option(System.getenv("KAFKA_OBSERVER_BROKER_IDS")).getOrElse("")
    .split(",").filter(_.nonEmpty).map(_.trim.toInt).toSet
  if (observerIds.contains(followerReplicaId)) {
    return false
  }
  // === END OBSERVER PATCH ===
  val current = partitionState
  !current.isInflight &&
    !current.isr.contains(followerReplicaId) &&
    isReplicaIsrEligible(followerReplicaId)
}
```

**原理**：observer 副本照常走 `ReplicaFetcherThread` 拉取 leader 全量数据（不受影响），但当它追平、尝试通过 `maybeExpandIsr → canAddReplicaToIsr` 加入 ISR 时被拦截 → 永远停留在"副本但非 ISR"状态。而 HW = ISR 内最小 LEO，observer 不在 ISR 故不拖 HW；leader 选举候选来自 ISR，observer 不在 ISR 故不当 leader。

> patch 脚本：`poc/scripts/observer-patch.py`　改动版 jar：`poc/patched-kafka.jar`

---

## 三、真机编译步骤（可复现）

```bash
# 1. 在编译机(loadgen m7g.xlarge 4vCPU/15GB)装依赖
sudo dnf install -y git java-17-amazon-corretto-devel   # 关键：devel 版含 javac，headless JRE 会编译失败

# 2. 克隆源码（浅克隆省时间）
cd /tmp && git clone --depth 1 --branch 3.7.1 https://github.com/apache/kafka.git kafka-src

# 3. 应用 patch
cd kafka-src && python3 observer-patch.py

# 4. 编译 core 模块（~1-3 分钟）
./gradlew :core:jar -x test --console=plain
# 产出：core/build/libs/kafka_2.13-3.7.1.jar
```

**踩坑记录**：
- 首次编译失败 = 装的是 headless JRE 无 `javac`，Scala 编译器需要它。装 `-devel` 版解决。
- 2vCPU 编译 Scala 会很慢，4vCPU/15GB 的 m7g.xlarge ~1-3 分钟可接受。

---

## 四、真机验证数据（决定性证据）

### 部署
- 改动版 jar 替换 3 broker 的 `/opt/kafka/libs/kafka_2.13-3.7.1.jar`
- systemd override 加 `Environment=KAFKA_OBSERVER_BROKER_IDS=1`（broker1@1a = observer）
- 建 topic `observer_test`，副本 `2:3:1`（leader=broker2@1c，follower=broker3@1d，observer=broker1@1a）

### 验证结果

| 验证项 | 结果 | 证据 |
|---|---|---|
| **observer 不进 ISR** | ✅ | broker1 停→恢复→fetch 追上后 `Replicas: 2,3,1` 但 `Isr: 2,3`（原版会自动回到 2,1,3） |
| **observer 同步全量数据** | ✅ | broker1 本地 log 最大 offset=**29999**（全量 30000 条都在），7.6MB |
| **不拖 HW（核心价值）** | ✅ | acks=all 延迟 = **2.35ms / P99 4ms** = 快对(1c+1d)水平，未被慢 AZ 1a 拖累 |
| **不当 leader** | ✅ | leader 始终在 broker2（非 observer），ISR 只有 {2,3} 供选举 |

**对比意义**：
- 配置法（前测 08）：跨 AZ 副本在 ISR，HW 被它拖累。
- **本方案（源码 patch）**：跨 AZ 副本同步数据但不在 ISR，HW 不被拖 → 实现了配置法做不到的"第三态"。这正是 Confluent 收费、字节自研内核的那个能力。

---

## 五、多集群 Learner 分析

**同集群 vs 跨集群的本质区别**：
- **同集群 observer（本方案）**：字节级复制同一 partition 同一 offset，**exactly-once 天然免费**（前测已验证重复 offset=0）。
- **跨集群（MM2）**：消费源→生产目标，目标 offset 重新分配，故障重投**会重复**（at-least-once）。

**多集群要保 EOS 的路径**：
- Confluent **Cluster Linking**：跨集群但 offset-preserving（mirror topic 只读、直接复制不重新生产、保持源 offset）——这是跨集群版的 observer。
- 开源复刻难度高于同集群 observer（要跨集群传播 offset 且禁止目标本地写入），但机制上同源。

**给交易所的建议**：
- **同 region 多 AZ**：用本方案（同集群 observer），EOS 免费、延迟不受慢 AZ 拖累。
- **跨 region DR**：用 MM2/MSK Replicator 接受最终一致 + 下游幂等去重，或投入做 Cluster Linking 式 offset-preserving 复制。

---

## 六、生产化差距（诚实标注）

本 POC 证明了**核心机制可行**，但生产级还需补：
1. **topic 初始 ISR 构造**：controller 建 topic 时直接把全部副本放初始 ISR（不走 maybeExpandIsr），需改 controller 侧初始 ISR 构造逻辑，才能让 observer 从一开始就不在 ISR。
2. **选举候选显式排除**：改 `PartitionChangeBuilder.electLeader` / controller 选举候选集，双保险确保 observer 永不当选（当前靠"不在 ISR"间接保证，unclean 场景需加固）。
3. **observer 晋升通路**：DR 切换时受控提升 observer（对标 Confluent observerPromotionPolicy）。
4. **配置化**：从环境变量升级为 topic 级 placement 约束（对标 Confluent replica-placement JSON）。
5. **跟上游版本**：fork 维护成本。

**责任边界**：本方案是内部 POC 验证，证明"能做"。是否投入生产、代码是否交付客户、谁维护，需评估（会议已定调：提供思路，不承担生产代码责任）。

---

## 七、生产加固（第二轮，真机完成 ✅）

第一版 patch 只拦截"重新加入 ISR"，topic 创建时 observer 仍会进初始 ISR。生产加固补了两处 `PartitionStateMachine.scala`：

**Patch A — 初始 ISR 构造排除 observer**（`initializeLeaderAndIsrForPartitions`，line ~291）：
```scala
val observerIds = Option(System.getenv("KAFKA_OBSERVER_BROKER_IDS")).getOrElse("")
  .split(",").filter(_.nonEmpty).map(_.trim.toInt).toSet
val nonObserver = liveReplicas.filterNot(observerIds.contains)
val isrReplicas = if (nonObserver.nonEmpty) nonObserver else liveReplicas
val leaderAndIsr = LeaderAndIsr(isrReplicas.head, isrReplicas.toList)  // leader 与初始 ISR 都排除 observer
```

**Patch B — unclean 选举排除 observer**（`offlinePartitionLeaderElection`，line ~535）：
```scala
val leaderOpt = assignment.find(id => liveReplicas.contains(id) && !observerIds.contains(id))
```

### 加固后真机验证（全部通过）

| 验证 | 加固前 | 加固后 | 证据 |
|---|---|---|---|
| **建 topic 初始 ISR** | observer 进 ISR(2,1,3) | ✅ **从建起就不在**(`Isr: 2,3`) | 全新 hardened_test 建好即 Isr={2,3} |
| leader 落点 | — | ✅ 从非 observer 选(Leader=2) | — |
| **unclean 极端选举** | 会选中 observer | ✅ **kill 全部 ISR 后 `Leader: none`，宁可无 leader 也不选 observer** | unclean=true + kill broker2,3 → Leader:none |
| observer 仍是副本同步 | — | ✅ Replicas 含 1、fetch 正常 | — |

> patch 脚本：`poc/scripts/observer-patch.py`(canAddReplicaToIsr) + patch2(PartitionStateMachine)；加固版 jar：`poc/patched-kafka-v2.jar`

## 八、Roadmap（更新）

| 阶段 | 内容 | 状态 |
|---|---|---|
| 0 配置逼近 | 强一致备份+EOS，但拖 HW | ✅ 已验证 |
| 1 源码原型 | 最小 patch，observer 不进 ISR 不拖 HW | ✅ 真机完成 |
| **2 生产加固** | **初始 ISR 排除 + unclean 选举排除** | ✅ **本轮真机完成** |
| 3 生产化 | topic 级 placement 配置化 + observer 受控晋升(DR) + 上游版本跟进 | 待投入（机制已全部打通） |

---

### 数据来源
真机：东京 observer_test topic 的 describe / DumpLogSegments / EndToEndLatency 输出。patch 与 jar 存于 `poc/`。
