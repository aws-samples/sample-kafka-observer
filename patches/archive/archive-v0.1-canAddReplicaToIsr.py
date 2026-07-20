#!/usr/bin/env python3
# 最小 observer patch: 在 canAddReplicaToIsr 里排除 KAFKA_OBSERVER_BROKER_IDS 指定的副本
# 效果: observer 副本照常 fetch 同步数据, 但永不进 ISR -> 不拖 HW / 不算 minISR / 不当 leader 候选
import re, sys

f = "core/src/main/scala/kafka/cluster/Partition.scala"
src = open(f).read()

# 在 canAddReplicaToIsr 方法体开头插入 observer 检查
old = """  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
    val current = partitionState
    !current.isInflight &&
      !current.isr.contains(followerReplicaId) &&
      isReplicaIsrEligible(followerReplicaId)
  }"""

new = """  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
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
  }"""

if old not in src:
    print("ERROR: 未找到 canAddReplicaToIsr 原文, patch 失败")
    sys.exit(1)

src = src.replace(old, new)
open(f, "w").write(src)
print("OK: observer patch 已应用到 canAddReplicaToIsr")
# 验证
if "OBSERVER PATCH" in open(f).read():
    print("VERIFY: patch 标记存在")
