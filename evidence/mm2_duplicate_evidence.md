# MM2 对照组真机证据：consume→reproduce，kill -9 造成重复投递

实验目的：真机复现 MirrorMaker 2 的语义本质——它是「消费源端 → 用自己的 producer 重新生产到目标」，因此
(1) 目标 offset 与源 offset 不可能恒等；(2) 崩溃恢复时按「至少一次」重投，产生重复。与同集群 Observer
（offset 恒等、CRC/PID 一致、纯字节复制）形成对照。所有数字均为实测，无编造。

## 环境
- 时间：2026-07-20 UTC
- loadgen：EC2 m7g.xlarge（Tokyo），Kafka 4.0 at `/opt/kafka4`，JDK 17
- 源集群（主 POC 集群）：bootstrap <source-broker>:9092，broker 2@ap-northeast-1c / broker 3@ap-northeast-1d
- 目标集群：loadgen 上单节点 KRaft（broker+controller combined），PLAINTEXT 127.0.0.1:9192，
  controller 127.0.0.1:9193，数据目录 /tmp/mm2-target，cluster id S-eFQmhUT_OtyIc9ACRK2A
- MM2：`/opt/kafka4/bin/connect-mirror-maker.sh /tmp/mm2.properties`，source→target 单向，
  topics=mm2_src，tasks.max=1，各 internal topic RF=1，KAFKA_HEAP_OPTS=-Xmx1g

## 源 topic 设置
- `mm2_src`，1 分区，replica-assignment 2:3（RF2，leader=2 ISR=[2,3]），min.insync.replicas=1
- 写入：kafka-producer-perf-test，20000 条 × 200B，acks=all，enable.idempotence=true
- 实测吞吐：15661 rec/s（2.99 MB/s），p99 916ms
- **源末端 offset = 20000**（kafka-get-offsets：`mm2_src:0:20000`）

## 时间线（UTC）
| 时刻 | 事件 |
|------|------|
| 04:44:57 | MM2 首次启动（PID 376824） |
| 04:45:08 | 首轮轮询，目标 source.mm2_src offset 已 = 20000（20000×200B≈4MB，~2s 内复制完成，无法 1s 粒度抓到「一半」，故改为在 Connect offset flush 窗口内 kill） |
| 04:45:29 | kill -9 MM2（启动后 ~32s，< 默认 60s offset flush 间隔）。kill 前目标 offset=20000 |
| — | 校验 mm2-offsets.target.internal：**无 mm2_src 源消费位点提交记录** → 位点未落盘 |
| 04:45:58 | 重启 MM2（PID 380063） |
| 04:46:06 | replication-consumer 日志：`Resetting offset for partition mm2_src-0 to position offset=0` → 从头重新消费全部源数据 |
| 04:46:08 | 目标 offset 跳到 40000，随后 3 次轮询稳定不变 |

## 核心结果
- 源总条数：**20000**（distinct md5 = 20000，无源端重复）
- 目标末端 offset：**40000**（kafka-get-offsets：`source.mm2_src:0:40000`）
- 目标 distinct 条数（sort|uniq）：**20000**
- 目标出现 ≥2 次的 payload：**20000**（全部；频率分布：20000 个 payload 各出现恰好 2 次）
- **目标 - 源 = 40000 - 20000 = 20000 条重复**

两种口径一致：md5 去重法 与 (目标总数−源总数) 法 都得出 20000 条重复。

## 字节级取证（DumpLogSegments，目标 segment 00000000000000000000.log）
- 总 batch 数 514。前半（baseOffset 0..19999）257 个 batch，后半（baseOffset 20000..39999）257 个 batch。
- 前半与后半的「CRC 序列」md5 **完全相同**：`2b4dad2aeb237066a660dc0ad329e7b8` → 重投的后半段与前半段 **逐 batch 字节一致**。
- 边界对照：baseOffset=0 与 baseOffset=20000 两个 batch 完全相同——
  count=78，CreateTime=1784522672014，crc=2265171308。即同一份源字节被以偏移 +20000 再投一次。
- **目标所有 batch 的 producerId = -1**：MM2 默认 producer 非幂等（无 PID/epoch/sequence），
  目标 broker 无从去重，每条重投都落成新 offset。

## PID/CRC 对照说明（vs Observer）
- 源端为 idempotent producer 写入（enable.idempotence=true，携带真实 PID）；目标端由 MM2 的
  **新** producer 重新生产，PID=-1（非幂等），这是「重新生产」而非「字节复制」的直接证据。
- 无法在 loadgen 上直接 DumpLogSegments 源 broker 磁盘（源集群私钥不在 loadgen 上），
  但目标端 producerId=-1 已充分证明目标记录的 PID 与源不同；且 offset 空间被 MM2 重排（0→20000 偏移）。
- 对照 Observer：Observer 是 broker 间副本拉取，offset 恒等、PID/epoch/sequence 保留、CRC 一致、
  崩溃恢复按 HW/leader epoch 精确续传，永不产生重复。MM2 是应用层 consume→produce，二者语义根本不同。

## 结论
- **成功复现**。MM2 = consume→reproduce：目标用独立的（默认非幂等）producer 重新生产，offset 空间
  与源不同；kill -9 丢失未提交的源消费位点后，恢复按「至少一次」从头重放，产生 **20000 条重复**
  （目标 40000 vs 源 20000，distinct 20000，每条恰好 2 份）。
- 重复条数（关键数字）：**20000**。目标 offset 40000，源 offset 20000。
- 字节取证证实重投段与原段逐 batch 字节一致（CRC 序列 md5 相同），目标 PID=-1（非幂等，无法去重）。
- 与 Observer（offset 恒等、CRC 一致、无重复）构成清晰对照，可直接用于交易所客户 deck。

## 产物
- 远端证据：loadgen `/tmp/mm2-evidence.md`（已保留）
- 本地证据：本文件
- 清理：MM2 进程已 kill，目标 KRaft broker 已停，源 topic mm2_src 已从主集群删除；主集群 50 个
  topic 健康未受影响。
