# 事务 + read_committed 一致性真机验证

> 时间: 2026-07-20 ~02:20 UTC
> Topic: txn_eos_test (replica-assignment 2:3:1, min.insync.replicas=2)
> Observer: broker1@1a (不在 ISR, Isr: 2,3)
> Producer: transactional.id=poc-txn-1, acks=all

## 写入

- Batch 1: commit 5 条 (commit1-0 ~ commit1-4)
- Batch 2: commit 5 条 (commit2-0 ~ commit2-4)  
- Batch 3: **abort** 3 条 (ABORT-0 ~ ABORT-2)
- Batch 4: commit 5 条 (commit3-0 ~ commit3-4)

总计: 15 条 committed + 3 条 aborted

## 验证结果

### Leader(broker2) 上 isolation.level=read_committed 消费
```
commit1-0 ~ commit1-4 (5)
commit2-0 ~ commit2-4 (5)
commit3-0 ~ commit3-4 (5)
```
共 15 条。abort 的 3 条不可见。✅

### Observer(broker1, 不在 ISR) 上 read_committed 消费
```
commit1-0 ~ commit1-4 (5)
commit2-0 ~ commit2-4 (5)
commit3-0 ~ commit3-4 (5)
```
共 15 条。abort 的 3 条不可见。✅

## 结论

Observer 通过 appendAsFollower 字节级复制了事务 COMMIT/ABORT control batch:
- ProducerState(PID/epoch/seq)从复制数据确定性重建
- LSO(Last Stable Offset)在 observer 上正确计算
- read_committed 视图: leader 与 observer **完全一致**
- 事务隔离语义原样保留,abort 数据在两端均不可见

EOS 对 Observer 是免费的——复制链路不在 EOS 设防路径上。
