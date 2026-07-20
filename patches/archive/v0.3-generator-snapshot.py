#!/usr/bin/env python3
# =============================================================================
# Observer Patch v3: 动态文件配置 + 受控晋升/降级, 零重启
# =============================================================================
# 适用: Apache Kafka 3.7.1 (ZK 模式), 在【干净源码树】上运行 (取代 patch1+patch2)
#   cd /tmp/kafka-src && python3 observer-patch3-dynamic.py
#   ./gradlew :core:jar -x test --console=plain
#
# 与 v1/v2 的区别:
#   v1/v2: observer 列表来自环境变量 KAFKA_OBSERVER_BROKER_IDS → 改动需重启 broker
#   v3:    observer 列表来自动态文件 /opt/kafka/observer.ids (5 秒缓存)
#          - 晋升: 从文件删掉 broker id → 下一次 follower fetch 触发 maybeExpandIsr,
#            canAddReplicaToIsr 放行 → 自动进 ISR → 自动成为选举候选。零重启。
#          - 降级: 把 id 加回文件 → 新增的降级钩子(getOutOfSyncReplicas)把 ISR 内的
#            observer 视同 out-of-sync → 周期性 isr-expiration 任务(默认每
#            replica.lag.time.max.ms/2 = 15s)自动 shrink 出 ISR。零重启。
#          - 兼容: 文件不存在时回退读环境变量(兼容 v1/v2 部署); 都没有 = 原生行为。
#
# 改动清单 (1 个新文件 + 5 处替换):
#   [NEW] core/src/main/scala/kafka/observer/ObserverIds.scala   动态读取工具 object
#   [1]  Partition.canAddReplicaToIsr        observer 永不进 ISR (同 v1, 改读文件)
#   [2]  Partition.getOutOfSyncReplicas      降级钩子: ISR 内 observer 视同掉队 (v3 新增)
#   [2b] Partition.maybeIncrementLeaderHW    HW 推进不等 observer (v3 新增, 补 v1 理论缺口:
#        shouldWaitForReplicaToJoinIsr 走的是 isReplicaIsrEligible 而非 canAddReplicaToIsr,
#        observer 在 lag 窗口内(默认30s)LEO 落后时理论上仍会拖 HW; 此处补上使"不拖HW"闭环)
#   [3]  PartitionStateMachine 初始 ISR       建 topic 时排除 observer (同 v2, 改读文件)
#   [4]  PartitionLeaderElectionAlgorithms   unclean 选举排除 observer (同 v2, 改读文件)
# =============================================================================
import os, sys

# ---------------------------------------------------------------------------
# [NEW] 工具 object: kafka.observer.ObserverIds
#   放独立新包 kafka.observer, 不碰任何现有文件的 import 块;
#   调用点全部用全限定名 kafka.observer.ObserverIds 引用, 使 patch 最小且稳健。
# ---------------------------------------------------------------------------
OBSERVER_IDS_SCALA = '''\
package kafka.observer

import java.nio.file.{Files, Path, Paths}
import kafka.utils.Logging
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

/**
 * === OBSERVER PATCH v3 ===
 * Observer broker id 的动态来源: 本地文件 (默认 /opt/kafka/observer.ids), 带 5 秒缓存。
 *
 * 语义:
 *   - 文件存在: 以文件内容为准 (每行一个或逗号分隔的 broker id; 支持 # 注释; 空文件 = 无 observer)
 *   - 文件不存在: 回退读环境变量 KAFKA_OBSERVER_BROKER_IDS (兼容 v1/v2 部署)
 *   - 文件读取失败 (权限/IO 错误): 保留上一次缓存值, 只打 warn 日志, 绝不影响 broker 运行
 *
 * 晋升 = 从文件删 id (下次 fetch 触发 maybeExpandIsr 放行, 自动进 ISR);
 * 降级 = 把 id 加回文件 (getOutOfSyncReplicas 降级钩子把它视同掉队, isr-expiration 自动 shrink)。
 * 缓存 5 秒: canAddReplicaToIsr 在 fetch 热路径上, 不能每次读盘; 5 秒对运维操作足够实时。
 */
object ObserverIds extends Logging {

  private val CacheTtlNanos: Long = 5L * 1000 * 1000 * 1000 // 5s

  @volatile private var cachedIds: Set[Int] = Set.empty
  @volatile private var lastRefreshNanos: Long = 0L
  @volatile private var initialized: Boolean = false

  private def filePath: Path =
    Paths.get(Option(System.getenv("KAFKA_OBSERVER_IDS_FILE")).getOrElse("/opt/kafka/observer.ids"))

  // 兼容 v1/v2: 文件不存在时回退环境变量
  // Try(...).toOption 而非 toIntOption: 兼容 Scala 2.12/2.13 双构建
  private def envFallback: Set[Int] =
    Option(System.getenv("KAFKA_OBSERVER_BROKER_IDS")).getOrElse("")
      .split(",").iterator.map(_.trim).filter(_.nonEmpty)
      .flatMap(s => Try(s.toInt).toOption).toSet

  private def readFromFile(): Set[Int] = {
    val p = filePath
    if (!Files.exists(p)) {
      envFallback
    } else {
      Files.readAllLines(p).asScala.iterator
        .filterNot(_.trim.startsWith("#"))          // 整行注释
        .flatMap(_.split("[,\\\\s]+"))                 // 逗号/空白分隔
        .map(_.trim)
        .filter(_.nonEmpty)
        .flatMap(s => Try(s.toInt).toOption)         // 非数字 token 静默忽略
        .toSet
    }
  }

  /** 当前 observer 集合 (5s 缓存)。任何异常都不外抛。 */
  def current(): Set[Int] = {
    val now = System.nanoTime()
    // now - lastRefreshNanos 用减法比较, 对 nanoTime 回绕安全; 首次调用必刷新
    if (!initialized || now - lastRefreshNanos >= CacheTtlNanos) {
      try {
        val fresh = readFromFile()
        if (fresh != cachedIds)
          info(s"Observer id set changed: $cachedIds -> $fresh (source: $filePath)")
        cachedIds = fresh
      } catch {
        case NonFatal(e) =>
          warn(s"Failed to read observer ids from $filePath, keeping last value $cachedIds", e)
      }
      lastRefreshNanos = now
      initialized = true
    }
    cachedIds
  }

  def isObserver(brokerId: Int): Boolean = current().contains(brokerId)
}
'''

# ---------------------------------------------------------------------------
# 4 处替换 (old 文本逐字来自 apache/kafka tag 3.7.1)
# ---------------------------------------------------------------------------

PARTITION = "core/src/main/scala/kafka/cluster/Partition.scala"
PSM = "core/src/main/scala/kafka/controller/PartitionStateMachine.scala"

# [1] Partition.canAddReplicaToIsr: observer 永不进 ISR (晋升闸门)
old1 = """  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
    val current = partitionState
    !current.isInflight &&
      !current.isr.contains(followerReplicaId) &&
      isReplicaIsrEligible(followerReplicaId)
  }"""
new1 = """  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
    // === OBSERVER PATCH v3 === observer 永不进 ISR。列表来自动态文件(5s 缓存, 零重启)。
    // 晋升: 从 /opt/kafka/observer.ids 删掉该 id → 下次 fetch 触发 maybeExpandIsr 时此处放行
    //       → 自动进 ISR → 自动恢复 leader 选举候选资格。
    if (kafka.observer.ObserverIds.isObserver(followerReplicaId)) {
      return false
    }
    // === END OBSERVER PATCH ===
    val current = partitionState
    !current.isInflight &&
      !current.isr.contains(followerReplicaId) &&
      isReplicaIsrEligible(followerReplicaId)
  }"""

# [2] Partition.getOutOfSyncReplicas: 降级钩子 (v3 新增)
#     leader 的周期任务 isr-expiration (ReplicaManager.scala:387, 每 replica.lag.time.max.ms/2)
#     调 maybeShrinkIsr → getOutOfSyncReplicas。把 ISR 内的 observer 并入结果 =
#     复用原生 shrink 全流程(日志/AlterPartition/HW 重算), 不新增任何状态机路径。
#     注: candidateReplicaIds 已排除 leader 自身(localBrokerId), 故绝不会把 leader shrink 掉。
old2 = """  def getOutOfSyncReplicas(maxLagMs: Long): Set[Int] = {
    val current = partitionState
    if (!current.isInflight) {
      val candidateReplicaIds = current.isr - localBrokerId
      val currentTimeMs = time.milliseconds()
      val leaderEndOffset = localLogOrException.logEndOffset
      candidateReplicaIds.filter(replicaId => isFollowerOutOfSync(replicaId, leaderEndOffset, currentTimeMs, maxLagMs))
    } else {
      Set.empty
    }
  }"""
new2 = """  def getOutOfSyncReplicas(maxLagMs: Long): Set[Int] = {
    val current = partitionState
    if (!current.isInflight) {
      val candidateReplicaIds = current.isr - localBrokerId
      val currentTimeMs = time.milliseconds()
      val leaderEndOffset = localLogOrException.logEndOffset
      val laggingReplicaIds = candidateReplicaIds.filter(replicaId => isFollowerOutOfSync(replicaId, leaderEndOffset, currentTimeMs, maxLagMs))
      // === OBSERVER PATCH v3: 降级钩子 ===
      // 已在 ISR 内但被标记为 observer 的副本, 视同 out-of-sync →
      // 周期性 isr-expiration 任务自动走原生 shrink 流程将其移出 ISR (零重启降级)。
      // 之后它想回 ISR 会被 canAddReplicaToIsr 拦住, 状态稳定。
      laggingReplicaIds ++ candidateReplicaIds.filter(kafka.observer.ObserverIds.isObserver)
      // === END OBSERVER PATCH ===
    } else {
      Set.empty
    }
  }"""

# [2b] Partition.maybeIncrementLeaderHW: HW 推进不等 observer (v3 新增)
#      原逻辑: 副本"已追上(lag窗口内)且 ISR-eligible"时, HW 推进要等它的 LEO —
#      即使它不在 maximalIsr。observer 恰好命中该条件(存活+窗口内), LEO 一旦落后即拖 HW。
#      v1/v2 真机未测出拖累是因为 observer 一直贴近追平; 此 patch 把理论缺口关死。
old2b = """      def shouldWaitForReplicaToJoinIsr: Boolean = {
        replicaState.isCaughtUp(leaderLogEndOffset.messageOffset, currentTimeMs, replicaLagTimeMaxMs) &&
        isReplicaIsrEligible(replica.brokerId)
      }"""
new2b = """      def shouldWaitForReplicaToJoinIsr: Boolean = {
        // === OBSERVER PATCH v3: HW 推进永不等待 observer (它也永远不会加入 ISR) ===
        !kafka.observer.ObserverIds.isObserver(replica.brokerId) &&
        replicaState.isCaughtUp(leaderLogEndOffset.messageOffset, currentTimeMs, replicaLagTimeMaxMs) &&
        isReplicaIsrEligible(replica.brokerId)
      }"""

# [3] PartitionStateMachine.initializeLeaderAndIsrForPartitions: 初始 ISR 排除 observer
old3 = """    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)"""
new3 = """    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      // === OBSERVER PATCH v3: 初始 ISR 排除 observer, leader 从非 observer 中选 ===
      // 兜底: 若存活副本全是 observer, 保留原生行为(宁可可用也不空 ISR)
      val nonObserver = liveReplicas.filterNot(kafka.observer.ObserverIds.isObserver)
      val isrReplicas = if (nonObserver.nonEmpty) nonObserver else liveReplicas
      val leaderAndIsr = LeaderAndIsr(isrReplicas.head, isrReplicas.toList)"""

# [4] PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection: unclean 选举排除 observer
old4 = """      if (uncleanLeaderElectionEnabled) {
        val leaderOpt = assignment.find(liveReplicas.contains)"""
new4 = """      if (uncleanLeaderElectionEnabled) {
        // === OBSERVER PATCH v3: unclean 选举也不选 observer (宁可无 leader 不丢一致性) ===
        val leaderOpt = assignment.find(id => liveReplicas.contains(id) && !kafka.observer.ObserverIds.isObserver(id))"""

# ---------------------------------------------------------------------------
def main() -> None:
    # 0. 前置检查: 必须是干净源码树
    for f in (PARTITION, PSM):
        if not os.path.exists(f):
            print(f"ERROR: 找不到 {f} — 请在 kafka 源码根目录运行")
            sys.exit(1)
        if "OBSERVER PATCH" in open(f).read():
            print(f"ERROR: {f} 已含 OBSERVER PATCH 标记 — v3 需在干净 3.7.1 源码树上应用(取代 v1/v2)")
            sys.exit(1)

    # 1. 写入新文件 ObserverIds.scala
    target = "core/src/main/scala/kafka/observer/ObserverIds.scala"
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with open(target, "w") as fh:
        fh.write(OBSERVER_IDS_SCALA)
    print(f"OK: [NEW] {target}")

    # 2. 四处替换
    patches = [
        (PARTITION, "1-canAddReplicaToIsr晋升闸门", old1, new1),
        (PARTITION, "2-getOutOfSyncReplicas降级钩子", old2, new2),
        (PARTITION, "2b-HW推进不等observer", old2b, new2b),
        (PSM, "3-初始ISR排除observer", old3, new3),
        (PSM, "4-unclean选举排除observer", old4, new4),
    ]
    for path, tag, old, new in patches:
        src = open(path).read()
        n = src.count(old)
        if n != 1:
            print(f"ERROR: patch {tag} 锚点出现 {n} 次(应为 1) — 源码版本不匹配?")
            sys.exit(1)
        open(path, "w").write(src.replace(old, new))
        print(f"OK: patch {tag} 已应用 -> {path}")

    total = sum(open(p).read().count("OBSERVER PATCH") for p in (PARTITION, PSM, target))
    # 预期 8: 新文件 scaladoc 1 + patch1 首尾 2 + patch2 首尾 2 + patch2b 1 + patch3 1 + patch4 1
    status = "OK" if total == 8 else "WARN(请人工核对)"
    print(f"VERIFY[{status}]: 共 {total} 处 OBSERVER PATCH 标记 (预期 8)")

if __name__ == "__main__":
    main()
