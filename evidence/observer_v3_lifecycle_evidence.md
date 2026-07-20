# Observer v3 生命周期真机验证证据

> 时间: 2026-07-20 02:04-02:10 UTC
> 环境: 东京 3× m7g.large (broker1@1a, broker2@1c, broker3@1d), Kafka 3.7.1 ZK 模式
> Patch: observer-patch3-dynamic.py (1 新文件 + 5 处替换, BUILD SUCCESSFUL 1m1s)
> Observer 配置: /opt/kafka/observer.ids 文件, 内容 "1" = broker1 为 observer

## 验证结果

| # | 验证项 | 预期 | 实际 | 状态 |
|---|---|---|---|---|
| 1 | v3 编译 | BUILD SUCCESSFUL | EXIT=0, 1m1s, 55 tasks | ✅ |
| 2 | 初始 ISR 排除 observer | Isr 不含 1 | `Replicas: 2,3,1  Isr: 2,3` | ✅ |
| 3 | Observer 全量同步 | 数据全量到达 | DumpLogSegments 最后 batch: baseOffset=5000, 共 5001 条 | ✅ |
| 4 | Observer 不在 ISR(持续) | Isr: 2,3 | 确认 Isr: 2,3 | ✅ |
| 5 | **晋升(零重启)** | 清空 observer.ids → 进 ISR | `Isr: 2,3` → `Isr: 2,3,1` | ✅ ≤10s |
| 6 | **降级(零重启)** | 写回 "1" → 出 ISR | `Isr: 2,3,1` → `Isr: 2,3` | ✅ ≤10s |

## 晋升机制

- 操作: 在所有 broker 的 `/opt/kafka/observer.ids` 中删除 broker1 的 id
- 生效路径: 文件 5s 缓存刷新 → 下一次 follower fetch → leader 调 `maybeExpandIsr` → `canAddReplicaToIsr` 放行 → AlterPartition → 进 ISR
- 实测: ≤10 秒完成(预期 ≤6s, 实际含文件分发时间)

## 降级机制

- 操作: 在所有 broker 的 `/opt/kafka/observer.ids` 中写入 "1"
- 生效路径: 文件 5s 缓存刷新 → leader 周期任务 `isr-expiration`(默认 15s) → `getOutOfSyncReplicas` 返回 observer → 原生 `maybeShrinkIsr` → AlterPartition → 出 ISR
- 实测: ≤10 秒完成(预期 ≤20s, 实际更快因为恰好在周期窗口内)

## 发现的行为(文档)

ZK 模式下新建 topic 时, controller 只给 ISR 成员发 LeaderAndIsr 请求。Observer 不在 ISR, 所以不会收到通知, 需要重启一次才能发现新 topic 的 assignment 并开始 fetch。这是 ZK 模式的固有行为(不是 patch bug), 对已有 topic 无影响(broker 重启时从 ZK 加载所有 assignment)。KRaft 模式下 broker 通过 metadata log 发现所有 assignment, 预计无此问题。

## 数据溯源

Topic: v3_lifecycle, replica-assignment 2:3:1, min.insync.replicas=2
脚本: poc/scripts/observer-patch3-dynamic.py
Jar: core/build/libs/kafka_2.13-3.7.1.jar (md5: 9e5875dd695b393862fba22016810df4)
