# kafka-3.7.1-kraft/observer.patch — Combined Patch (ZK + KRaft 两模式通吃)

⚠️ **虽然目录名叫 `-kraft`, 这是一个 combined patch**: 打在 vanilla Kafka 3.7.1 源码树上, 同时包含 ZK 模式 (v0.3) 和 KRaft 模式 (v0.5) 的全部 observer 改动。编译一次, 产物在两种模式下都具备完整 observer 能力。

## 内容 (5 文件, +271/-6)

| 文件 | 侧 | 作用 |
|---|---|---|
| `core/.../cluster/Partition.scala` | broker (两模式共用) | ISR 准入 gate + 降级钩子 (v3) |
| `core/.../controller/PartitionStateMachine.scala` | ZK controller | 选举排除 observer (v3) |
| `core/.../observer/ObserverIds.scala` (新) | broker (两模式共用) | observer.ids 文件读取, 5s 缓存 (v3) |
| `metadata/.../controller/ObserverReplicas.java` (新) | KRaft controller | 同语义纯 Java 版 (metadata 模块不能依赖 core) |
| `metadata/.../controller/ReplicationControlManager.java` | KRaft controller | 初始 ISR 过滤 / LeaderAcceptor 选举 gate (含 unclean) / AlterPartition 二次防御 |

## 应用与编译

```bash
cd kafka-3.7.1-src
git apply observer.patch
./gradlew :metadata:jar :core:jar :storage:jar -x test
```

## 部署 (KRaft 模式注意!)

需替换 **3 个 jar**: `libs/kafka_2.13-3.7.1.jar` (core)、`libs/kafka-metadata-3.7.1.jar`、`libs/kafka-storage-3.7.1.jar`。

- **controller 节点 (含 controller-only) 也必须部署 patched jar + `observer.ids` 文件** — KRaft 下初始 ISR 过滤和选举排除都在 controller 进程里执行
- observer.ids 路径: 默认 `/opt/kafka/observer.ids`, env `KAFKA_OBSERVER_IDS_FILE` 覆盖; 文件缺失回退 env `KAFKA_OBSERVER_BROKER_IDS`
- 晋升 SOP: 先更新 controller 节点的 observer.ids, 再更新 broker 节点 (顺序反了只会导致 AlterPartition 暂时被拒, fail-safe)

## 验证证据

`evidence/kraft_controller_patch_evidence.md` (KRaft controller 侧, 2026-07-20 东京真机) 与 `evidence/kraft_probe_evidence.md` (broker 侧 3 hook 在 KRaft 免费生效)。ZK 模式证据见 `evidence/observer_v3_lifecycle_evidence.md`。
