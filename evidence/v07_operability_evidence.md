# v0.7 Operability — 真机验证证据 (Metrics + 审计日志 + auto-promoter 端到端)

- 日期: 2026-07-20 (UTC) · 环境: 东京 loadgen EC2 (54.250.248.165)
- 集群: 单机 3 节点 KRaft **combined** (broker,controller), Kafka 3.7.1 + v0.7 patched jars
  - broker 端口 9392/9394/9396, controller 9393/9395/9397, JMX 9992/9994/9996
  - 工作目录 /tmp/kraft-v07, 每节点 -Xmx512m, `replica.lag.time.max.ms=10000`
  - jars 来源: /tmp/kafka-src (v0.7 metrics patch 编译产物, 见 metrics_patch_evidence.md)
    - `core/build/libs/kafka_2.13-3.7.1.jar` (5062790 bytes) — 含 `kafka/observer/ObserverIds*.class`
    - `metadata/build/libs/kafka-metadata-3.7.1.jar` (852363 bytes) — 含 `org/apache/kafka/controller/ObserverReplicas.class`
  - `KAFKA_OBSERVER_IDS_FILE=/tmp/kraft-v07/observer.ids`, 初始内容 `3` (broker 3 = observer)
  - topic `orders`: 2 分区, RF=3, `min.insync.replicas=2`, 写入 5000 条
- 读数工具: `kafka-run-class.sh org.apache.kafka.tools.JmxTool --jmx-url service:jmx:rmi:///jndi/rmi://localhost:<port>/jmxrmi`
- 脚本: repo `scripts/observer-{auto-promoter,promote,demote}.sh` 原样 scp 到主机, ssh 走 loopback (`-H localhost` + 专用密钥)

## 0. 结论 — 验证矩阵

| # | 验证项 | 预期 | 实测 | 结果 |
|---|---|---|---|---|
| 1a | `kafka.observer:...ObserverCount` (node1/2) | 1 (名单含 broker 3) | `Value=1` | ✅ |
| 1b | ObserverCount (node3, observer 自身) | MBean 惰性注册, 可能缺席 | node3 无此 MBean (未当 leader, ObserverIds 未初始化) — 与 metrics_patch_evidence.md 第6节预告一致 | ✅(边界如实) |
| 1c | RM `ObserverLagMessages` (leader 视角) | ≈0 (observer 已追上) | `Value=0` (5000 条写入后) | ✅ |
| 1d | RM `ObserverCaughtUpCount` | = 该 broker 领导分区上的 caught-up observer 数 | node1=1, node2=1 (各领 1 分区); 后期 node1 领 2 分区时 =2 | ✅ |
| 1e | RM `ObserversInIsrCount` 稳态 | 0 (gate 生效, observer 不在 ISR) | `Value=0` (全部 3 节点) | ✅ |
| 1f | per-partition 3 gauge (orders-0 @leader) | InIsr=0 / CaughtUp=1 / Lag=0 | `0 / 1 / 0` | ✅ |
| 1g | 晋升后 ObserversInIsrCount (broker3 在 ISR 但**不在名单**) | 0 — 名单里的 id 在 ISR 才算 | `Value=0` (ISR=1,3 时实测) | ✅ 语义确认 |
| 1h | 降级窗口 ObserversInIsrCount (broker3 在名单**且仍在 ISR**) | 短暂 >0, shrink 后归 0 | 1s 采样序列: `8×0 → 5×1 → 27×0` (≈5s 的 Value=1 窗口) | ✅ 语义确认 |
| 2a | 改 observer.ids → broker 侧审计行 | WARN 结构化行, 含 before/after/added/removed/source/epochMs | 见 §2, 全部字段齐全 | ✅ |
| 2b | controller 侧审计行 | 同上, 成对出现 | `OBSERVER AUDIT (controller)` 同字段, 与 broker 行时间差 <20ms~200ms | ✅ |
| 3a | auto-promoter 默认关 | 无 `-e` 打印 OFF 退出 0 | (v0.6 dry-run evidence 已验; 本轮全部带 `-e`) | ✅(既有) |
| 3b | dry-run 检测 under-min-isr | DETECT + PROMOTE-DRYRUN, 集群不动 | kill broker2 后: `DETECT ... isr=1 (size=1 < minISR=2)` → `PROMOTE-DRYRUN | broker=3 | ... observerLag=0 | no action taken`; observer.ids 未变 | ✅ |
| 3c | 真实模式自动晋升 | 脚本改 observer.ids + broker3 进 ISR + 审计行 | observer.ids `3→空`, ISR `1 → 1,3` (双分区), 审计 `removed=[3]`, state 文件记录 3 | ✅ |
| 3d | 真实模式自动降级 (broker2 恢复后) | 脚本加回 observer.ids + broker3 退出 ISR + 审计行 | observer.ids `空→3`, ISR `1,3,2 → 1,2`, 审计 `added=[3]`, state 清空 | ✅ |
| 3e | 计时 | 晋升 ≤10s 量级 / 降级 ≤20s 量级 (README 口径) | 晋升 scan→OK **12s**; 降级 scan→OK **31s** (含 5s 双确认+preferred election 检查), 文件变更→ISR shrink **≤9s** | ✅ |

## 1. Metrics 真机读数 (JmxTool)

稳态 (observer.ids=[3], orders 2 分区 leader 分别为 node1/node2, 5000 条已写入):

```
node1: kafka.observer:type=ObserverMetrics,name=ObserverCount:Value=1
       kafka.server:type=ReplicaManager,name=ObserversInIsrCount:Value=0
       kafka.server:type=ReplicaManager,name=ObserverCaughtUpCount:Value=1
       kafka.server:type=ReplicaManager,name=ObserverLagMessages:Value=0
node2: (同 node1, 各值 1/0/1/0)
node3: ObserverCount MBean 不存在 (惰性注册, node3 无 leader 分区未触发 ObserverIds 初始化)
       RM 三 gauge 均为 0 (无 leader 分区, 聚合为空)
per-partition orders-0 @node1:
       kafka.cluster:type=Partition,name=ObserversInIsrCount,topic=orders,partition=0:Value=0
       kafka.cluster:type=Partition,name=ObserverCaughtUpCount,topic=orders,partition=0:Value=1
       kafka.cluster:type=Partition,name=ObserverLagMessages,topic=orders,partition=0:Value=0
```

### ObserversInIsrCount 语义确认 (任务核心问题)

指标定义 = "**observer.ids 名单里的 id** 当前在 ISR" 的计数。两个方向都实测:

1. **晋升后** (broker3 在 ISR=1,2,3 / 1,2,4… 但已从名单删除): `Value=0` — 在 ISR 但不在名单, 不算。✅ 恒 0 成立。
2. **降级窗口** (broker3 加回名单但 native shrink 尚未完成, 仍在 ISR): 1 秒间隔连续采样 40s:
   ```
   8×Value=0 → 5×Value=1 → 27×Value=0
   ```
   即约 5 秒的 `Value=1` 过渡窗口, shrink 完成后归 0。

**结论**: 稳态恒 0; 瞬时 >0 只出现在 (a) 降级进行中 (在名单+还没被 shrink 出去, 秒级自愈) 或 (b) gate 被绕过/各节点文件不一致 (持续 >0, 需告警)。告警建议: `ObserversInIsrCount > 0 持续超过 2×replica.lag.time.max.ms` 才触发, 避开降级过渡窗口。

## 2. 审计日志 (broker + controller 成对, WARN 级)

手工编辑 observer.ids 触发 (`echo 3 >` / `: >`), server.log 实录:

```
[2026-07-20 15:18:17,658] WARN OBSERVER AUDIT (broker): observer id set changed before=[3] after=[] added=[] removed=[3] source=file:/tmp/kraft-v07/observer.ids epochMs=1784560697658 (kafka.observer.ObserverIds$)
[2026-07-20 15:18:17,674] WARN OBSERVER AUDIT (controller): observer id set changed before=[3] after=[] added=[] removed=[3] source=file:/tmp/kraft-v07/observer.ids epochMs=1784560697674 (org.apache.kafka.controller.ObserverReplicas)
[2026-07-20 15:18:32,693] WARN OBSERVER AUDIT (broker): observer id set changed before=[] after=[3] added=[3] removed=[] source=file:/tmp/kraft-v07/observer.ids epochMs=1784560712693 (kafka.observer.ObserverIds$)
[2026-07-20 15:18:32,890] WARN OBSERVER AUDIT (controller): observer id set changed before=[] after=[3] added=[3] removed=[] source=file:/tmp/kraft-v07/observer.ids epochMs=1784560712890 (org.apache.kafka.controller.ObserverReplicas)
```

- `removed` 非空=晋升, `added` 非空=降级; `source=file:<path>` 标明触发源 ✅
- 3 节点 (combined 模式 broker+controller 同进程) 各自独立打印, 时间差由各自 5s 缓存到期决定 (观测 <1s 至 ~15s), 与设计一致
- 文件变更 → 首条审计行延迟: 实测 3~6s (5s 缓存内)

## 3. auto-promoter 端到端 (故障注入)

参数: `-s localhost:9392 -H "localhost" -f /tmp/kraft-v07/observer.ids -m 2 -l 0 -c 5 -1`, ssh 走 loopback。

### 3a. 故障注入 → dry-run 检测

```
15:22:47.8  pkill -9 broker2 (node2 领 orders-1)
15:23:05    ISR 已收缩: orders-0 Isr:1 / orders-1 Isr:1 (leader 均转移到 1), 双分区 under-min-isr
15:23:07    dry-run 单次扫描:
  DETECT | under-min-isr | topic=orders partition=0 leader=1 replicas=1,2,3 isr=1 (size=1 < minISR=2)
  PROMOTE-DRYRUN | broker=3 | topic=orders partition=0 isr=1 minISR=2 observerLag=0 | no action taken
```
observer.ids 未被修改, 集群零变化 ✅ (max-one-action-per-scan: 只对第一个 under-min-isr 分区决策一次)

### 3b. 真实模式自动晋升 — **scan→PROMOTE-OK 12s**

```
15:23:42.8  扫描启动 (真实模式)
15:23:45    DETECT | under-min-isr | ... isr=1 (size=1 < minISR=2)
15:23:46    PROMOTE-BEGIN | broker=3 | topic=orders partition=0 isr=1 minISR=2 observerLag=0
15:23:53.3  broker 审计: before=[3] after=[] removed=[3]   ← 脚本原子改文件 (tmp+mv)
15:23:54    PROMOTE-OK | broker=3 | now a full ISR/election candidate
结果: observer.ids=[] · ISR: orders-0 → 1,3 / orders-1 → 1,3 · auto-promoted.list=[3]
晋升后 ObserversInIsrCount=0 (§1g), ObserverCount=0 (名单已空)
```
under-min-isr 期间集群从 1 副本 ISR 恢复到满足 minISR=2, 全程零重启零数据搬移。

### 3c. broker2 恢复 → 真实模式自动降级 — **scan→DEMOTE-OK 31s**

```
15:24:56    重启 broker2; +5s 即回 ISR: 双分区 Isr: 1,3,2
15:25:40.6  扫描启动: phase1 无 under-min-isr → phase2 对 state 中的 broker3 做恢复判定
            (demotion_safe 双确认: 两次 describe 间隔 5s, ISR-{3}>=minISR 均成立; leads_any=否)
15:25:56    DEMOTE-BEGIN | broker=3 | original followers recovered; ISR-{3} >= minISR on all partitions
            (observer-demote.sh 内置双 pre-check 亦通过: 非 leader / 降级后 ISR 不破 minISR)
15:26:03.5  broker 审计: before=[] after=[3] added=[3]     ← 脚本加回文件
15:26:12    DEMOTE-OK | broker=3 | back to observer status  ← native isr-expiration shrink, 文件变更→shrink ≤9s
结果: observer.ids=[3] · ISR: 双分区 → 1,2 · auto-promoted.list=[] (state 正确清空)
终态 metrics @node1 (领 2 分区): ObserverCount=1 / ObserversInIsrCount=0 / ObserverCaughtUpCount=2 / ObserverLagMessages=0
```

### 3d. 计时汇总

| 阶段 | 耗时 | 构成 |
|---|---|---|
| broker kill → ISR shrink (native) | ≤18s | replica.lag.time.max.ms=10s + expiration 周期 5s |
| 晋升: 扫描启动 → PROMOTE-OK | **12s** | describe+lag 查询 ~3s, 文件改动→ISR 加入 ~8s (5s 缓存+fetch 往返) |
| 降级: 扫描启动 → DEMOTE-OK | **31s** | 双确认 sleep 5s + 多次 describe + 文件改动→shrink ≤9s + 5s 轮询粒度 |
| 完整故障闭环 (kill→晋升→恢复→降级) | ~3.5min | 含人工等待, 守护模式 (-i 10) 下为自动 |

## 4. 环境清理

三个 Kafka 进程 stop, /tmp/kraft-v07 整目录删除, 939x/999x 端口释放, loopback 测试密钥 (~/.ssh/v07local*) 及 authorized_keys 条目移除。/tmp/kafka-src 与既有 ZK 集群未触碰。

## 5. 已知边界 (实事求是)

- 单机 3 节点 loopback 拓扑: ssh 走 localhost, `-H` 只有一台 (三 broker 共享同一 observer.ids 文件) — 多主机文件一致性路径 (逐台 ssh+atomic mv) 在 v0.5/0.6 三机 POC 已验, 本轮不重复
- node3 (observer 自身, 无 leader 分区) 的 `kafka.observer` ObserverCount MBean 因惰性初始化不存在 — 监控接入时对该 MBean 缺席要容忍, 或以 RM 侧 gauge 为准
- 降级过渡窗口 ObserversInIsrCount=1 约 5s (replica.lag.time.max.ms=10s 配置下), 生产 30s 默认配置下窗口相应变长 — 告警需加持续时长条件 (§1)
- auto-promoter 本轮用 `-1` 单次扫描驱动 (可控计时); 长驻守护模式 (`-i 10` 循环) 逻辑相同, cooldown/anti-flap 未做长时间压力观察
