# EOS 字节级验证证据 — DumpLogSegments 对比

> 时间: 2026-07-20 01:11 UTC
> Topic: lifecycle_test (replica-assignment 2:3:1, min.insync.replicas=2, 5000条 200B acks=all enable.idempotence=true)
> Leader: broker2@1c | Follower/Observer: broker1@1a
> 验证方法: kafka-dump-log.sh 对比两台 broker 的 log segment batch 元数据

## 结论

**Leader 和 Follower/Observer 的 RecordBatch 逐字节一致**(CRC 完全匹配),证明 `appendAsFollower` 原样复制 leader 已定 offset 的 batch,不重分配 offset、不修改 PID/epoch/sequence。

## 关键字段对比(前5个batch)

| 字段 | Leader(broker2) | Follower(broker1) | 一致? |
|---|---|---|---|
| baseOffset:0 lastOffset:77 | ✓ | ✓ | ✅ |
| producerId:5009 epoch:0 | ✓ | ✓ | ✅ |
| baseSequence:0 lastSequence:77 | ✓ | ✓ | ✅ |
| CreateTime:1784510462446 | ✓ | ✓ | ✅ |
| size:16377 position:0 | ✓ | ✓ | ✅ |
| **crc:3058053539** | ✓ | ✓ | ✅ **字节级一致** |
| baseOffset:78 crc:3245726146 | ✓ | ✓ | ✅ |
| baseOffset:156 crc:1308929752 | ✓ | ✓ | ✅ |
| baseOffset:234 crc:3337236274 | ✓ | ✓ | ✅ |
| baseOffset:312 crc:555025203 | ✓ | ✓ | ✅ |

所有 23+ batch 的 CRC 全部匹配 — 不存在任何一个 batch 被修改过。

## 技术机制(源码)

Kafka follower 同步走 `Log.appendAsFollower()`,参数 `validateAndAssignOffsets = false`:
- 源码注释: "we are taking the offsets we are given"
- 跳过 `LogValidator`(不重分配 offset、不校验 PID 冲突)
- RecordBatch header 里的 producerId / producerEpoch / baseSequence / 事务 COMMIT/ABORT marker 全部原样落盘
- ProducerState / LSO 在副本精确重建 → `read_committed` 视图一致

## 对比 MM2

MM2 = consume → reproduce:
- 目标集群 Producer 重新分配 offset(新 batch、新 baseOffset)
- 即使同 PID 也不保证 sequence 连续(跨集群 PID 空间独立)
- 崩溃重启从上次 committed offset 重发 → at-least-once
- KIP-618 只保 produce 侧幂等,不解决 offset 空间断裂

## 数据溯源

```bash
# leader
/opt/kafka/bin/kafka-dump-log.sh --files /data/kafka/lifecycle_test-0/00000000000000000000.log
# follower/observer
/opt/kafka/bin/kafka-dump-log.sh --files /data/kafka/lifecycle_test-0/00000000000000000000.log
```
