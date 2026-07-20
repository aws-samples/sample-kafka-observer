#!/usr/bin/env python3
# 生产加固 patch: 让 observer 从建 topic 起就不在初始 ISR + unclean 选举也排除 observer
import sys
f = "core/src/main/scala/kafka/controller/PartitionStateMachine.scala"
src = open(f).read()

# ---- Patch A: 初始 ISR 构造排除 observer ----
oldA = """    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)"""
newA = """    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      // === OBSERVER PATCH: 初始 ISR 排除 observer, leader 从非 observer 中选 ===
      val observerIds = Option(System.getenv("KAFKA_OBSERVER_BROKER_IDS")).getOrElse("")
        .split(",").filter(_.nonEmpty).map(_.trim.toInt).toSet
      val nonObserver = liveReplicas.filterNot(observerIds.contains)
      val isrReplicas = if (nonObserver.nonEmpty) nonObserver else liveReplicas
      val leaderAndIsr = LeaderAndIsr(isrReplicas.head, isrReplicas.toList)"""

# ---- Patch B: unclean 选举分支排除 observer ----
oldB = """      if (uncleanLeaderElectionEnabled) {
        val leaderOpt = assignment.find(liveReplicas.contains)"""
newB = """      if (uncleanLeaderElectionEnabled) {
        // === OBSERVER PATCH: unclean 选举也不选 observer ===
        val observerIds = Option(System.getenv("KAFKA_OBSERVER_BROKER_IDS")).getOrElse("")
          .split(",").filter(_.nonEmpty).map(_.trim.toInt).toSet
        val leaderOpt = assignment.find(id => liveReplicas.contains(id) && !observerIds.contains(id))"""

for tag, old, new in [("A-初始ISR", oldA, newA), ("B-unclean选举", oldB, newB)]:
    if old not in src:
        print(f"ERROR: patch {tag} 未找到原文")
        sys.exit(1)
    src = src.replace(old, new)
    print(f"OK: patch {tag} 已应用")

open(f, "w").write(src)
print("VERIFY:", src.count("OBSERVER PATCH"), "处 observer patch 标记")
