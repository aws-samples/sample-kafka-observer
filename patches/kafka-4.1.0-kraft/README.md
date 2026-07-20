# Kafka 4.1.0 KRaft Observer Patch

从 4.0.0 patch (`patches/kafka-4.0.0-kraft/observer.patch`) 移植到 Apache Kafka 4.1.0 (tag `4.1.0`, commit `13f7025`)。patch 内容与 4.0.0 版**逐字一致**，仅 hunk 行号漂移。

## 应用方法

```bash
git clone --depth 1 --branch 4.1.0 https://github.com/apache/kafka.git
cd kafka
git apply observer.patch
./gradlew :metadata:jar :core:jar :storage:jar -x test
```

编译环境: JDK 17 (Corretto 17.0.19), Scala 2.13。
[事实] 2026-07-20 东京 loadgen 真机: 4.0 patch 对 4.1.0 `git apply` **8/8 hunk 干净通过**（Partition.scala offsets +4/-1/-1; ReplicationControlManager.java offsets -1/-1/+30），`BUILD SUCCESSFUL in 3m 9s`；canonical patch 在 pristine worktree `git apply --check` 通过。

## 4.1 相关差异（相对 4.0）

- [事实] **ELR 默认开启**: 新集群 (metadata.version 4.1-IV1) format 后 `eligible.leader.replicas.version` FinalizedVersionLevel=1，零配置。4.0 需手动 `kafka-features.sh upgrade`。
- [事实] **KAFKA-19522 已修复** (commit `e4e2dce2eb`): `PartitionChangeBuilder.canElectLastKnownLeader` 的缺取反 bug（3.7.1/4.0.0 均存在）在 4.1 消失——fenced 的末代 leader 不再被误选。
- [事实] observer 永不进 ELR/LastKnownElr（真机实证，见 `evidence/elr_verification_evidence.md`）：ELR 候选集 = ELR ∪ ISR，observer 永不进 ISR ⇒ 结构性排除，patch 无需触碰 ELR 代码。

## 真机验证 (2026-07-20, 东京 loadgen)

controller×3 + broker×3 分离拓扑，observer=3，Kafka 4.1.0 官方 dist + patched jars:
- 初始 ISR 排除 PASS（controller.log `Filtered observers [3] from initial ISR [1, 2, 3] -> [1, 2]`）
- acks=all 3000 条三节点 offset 全对齐 PASS
- kill 全部非 observer broker → `Leader: none`, `Elr: 1,2`, `LastKnownElr: 1`（均不含 observer），显式 unclean election 抛 `EligibleLeadersNotAvailableException` PASS
- ELR 成员回归 → 干净选主，offset 3000 零丢失 PASS

完整证据: `evidence/kafka40_port_evidence.md` §7-§9 与 `evidence/elr_verification_evidence.md`。
