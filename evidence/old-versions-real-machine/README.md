# 老版本真机验证 — Kafka 2.7.2 / 2.8.1 / 2.8.2 / 3.3.2 (ZooKeeper mode)

将 observer patch 的验证矩阵向下扩展到四个更老的 ZooKeeper-mode 版本，
**编译 + 部署 + 完整 S1–S8 故障场景全部在真实 EC2 上跑通**。
原始命令输出见本目录 `scenario-<version>.txt`。

## 最早可行版本 = 2.7（有明确架构原因）

patch 的核心闸门是 leader 侧的 `canAddReplicaToIsr`。它随 **KIP-497 (AlterIsr)**
在 **Kafka 2.7** 引入——该 KIP 把 ISR 管理从「leader 直写 ZooKeeper」改成「leader
经 AlterIsr API 请求 controller 变更」。函数存在性探测（真机 `git clone` 逐版检查）：

| 版本 | `canAddReplicaToIsr` | ISR 管理模型 | 结论 |
|---|---|---|---|
| 2.4.1 / 2.5.1 / 2.6.3 | ❌ 不存在 | leader 直写 ZK（旧） | **架构性不兼容**，patch 核心 hook 无处可挂 |
| **2.7.2** | ✅ 存在 | AlterIsr API (KIP-497) | ✅ **最早可跑通版本**（本目录完整 S1–S8 验证） |
| 2.8.1 / 2.8.2 / 3.3.2 | ✅ 存在 | AlterIsr API | ✅ 完整 S1–S8 验证 |
| 3.6 – 4.1 | ✅ 存在 | AlterIsr / KRaft | ✅ 已验证（见上级 evidence 目录） |

**因此支持下限是 Kafka 2.7。2.6 及更早不支持，且不是"没适配"，是结构上没有可挂载点。**

## 环境

| 项 | 值 |
|---|---|
| 主机 | EC2, ap-northeast-1 (Tokyo), aarch64 (Graviton), 4 vCPU |
| JDK | Amazon Corretto 11.0.31（老版本 Kafka 需 JDK8/11） |
| 构建 | `git clone <tag>` → `git apply patches/kafka-<v>-zk/observer.patch` → `./gradlew :core:jar :tools:jar` |
| 拓扑 | 单机多进程 ZooKeeper(1) + 4 broker(id 1–4)，observer = broker 3 |
| topic | `smx`：`--replica-assignment 3:1:2:4`，`min.insync.replicas=2` |
| 配置 | `replica.lag.time.max.ms=10000`，每 broker 独立 `observer.ids` 文件 |

> 构建期只做了三处与 patch 无关的构建基础设施适配（JCenter/Bintray 2021 年已关停）：
> 删除失效的 `jcenter()`、grgit 插件 `4.x`→`5.0.0`、编译时移开 `.git` 跳过 rat license 检查。
> **Kafka 源码本身与 observer patch 一字未改。**

## 完整 S1–S8 验证矩阵（四版本全通过）

| 场景 | 验证点 | 2.7.2 | 2.8.1 | 2.8.2 | 3.3.2 |
|---|---|---|---|---|---|
| **构建** | git apply + `:core:jar` 编译 | ✅ | ✅ | ✅ | ✅ |
| **初始态** | observer 不进 ISR（Replicas 3,1,2,4 → Isr 1,2,4） | ✅ | ✅ | ✅ | ✅ |
| **S1** | leader 崩溃 → 新 leader 是 primary（非 observer），降级写入成功 | ✅ 9.1s | ✅ 11.1s | ✅ | ✅ 7.0s |
| **S2** | ISR follower 崩溃 → shrink，leader 不变，写入成功，自动 rejoin | ✅ | ✅ | ✅ | ✅ |
| **S3** | observer 崩溃 → ISR 不变，写入零影响，追平后段文件 **md5 逐字节相同** | ✅ `cd18c13a` | ✅ `469b6651` | ✅ | ✅ `f814c432` |
| **S4** | 全 primary 崩溃 → observer 拒绝当选(Leader:none) → 删 observer.ids + unclean 选举 → **秒级晋升为 leader** | ✅ 10.5s | ✅ | ✅ | ✅ |
| **S5** | 晋升滞后 observer → 先灌 30k 拉开 lag → 追平后才准入 ISR，HW 不回退 | ✅ 追平 6.6s | ✅ 6.7s | ✅ | ✅ 3.5s |
| **S6** | observer.ids 节点间不一致 → leader 侧闸门 fail-safe，ISR 仍排除 observer | ✅ | ✅ | ✅ | ✅ |
| **S7** | observer.ids 权限拒绝/垃圾内容/删除 → 保留缓存 + WARN，broker 不崩，写入正常 | ✅ | ✅ | ✅ | ✅ |
| **S8** | controller failover(kill controller broker) → 新 controller 仍正确排除 observer | ✅ 新ctrl 7.2s | ✅ 7.4s | ✅ | ✅ 9.3s |

每个场景的原始命令输出（含 md5、ISR 快照、时间戳、WARN 日志）见 `scenario-<version>.txt`。

## 每个场景验证了什么

- **初始稳态**：`Replicas: 3,1,2,4` 但 `Isr: 1,2,4` —— 核心闸门 `canAddReplicaToIsr` 把 observer 挡在 ISR 外。
- **S1**：kill -9 leader，新 leader 从 ISR 中的 primary 选出（**从不是 observer**），降级态 `acks=all` 写入继续。
- **S2**：ISR 收缩，leader 不变，写入继续；follower 重启后自动重入 ISR。
- **S3**：observer 崩溃时 ISR 完全不变、写入零影响；observer 重启追平后其段文件与 leader **逐字节 md5 恒等**（EOS 字节复制的直接证据）。
- **S4**：kill 全部 primary 后 `Leader: none`——observer 存活却拒绝当选（unclean 排除生效）；从 `observer.ids` 删 id → unclean 选举 → observer 秒级晋升顶上，晋升后成功读回数据。
- **S5**：先冻结 observer 并灌入 3 万条制造 lag，再重启+晋升——observer 必须追平 leader LEO 后才被准入 ISR，HW 全程不回退。
- **S6**：只在部分节点清空 observer.ids，leader 侧闸门仍拦住 observer（fail-safe 方向），文件对齐后自愈。
- **S7**：chmod 000 / 写垃圾 / 删文件三种破坏——broker 均不崩溃，保留上次缓存值并打 WARN（`keeping last value Set(3)`），写入全程正常。
- **S8**：ZK 模式下 kill 当前 controller broker，新 controller 选出后仍正确排除 observer，写入正常。

## 边界说明

- **Kafka 2.6 及更早不支持**：`canAddReplicaToIsr` 守门函数在 KIP-497(2.7)之前不存在，patch 核心 hook 无处可挂——架构性不兼容，非"未适配"。已通过真机函数探测确认（2.4.1/2.5.1/2.6.3 均无此函数）。
- 编译/运行使用 JDK 11（老版本 Kafka 不兼容 JDK 17）。生产部署时 broker 运行时 JDK 需与所用 Kafka 版本匹配。

## 复现

```bash
# 在装有 JDK11 + git 的机器上:
V=2.7.2                                   # 或 2.8.1 / 2.8.2 / 3.3.2
git clone --depth 1 --branch $V https://github.com/apache/kafka.git /tmp/build-$V
cd /tmp/build-$V
git apply <repo>/patches/kafka-$V-zk/observer.patch
# 构建基础设施适配(仅老版本需要, 与 patch 无关):
sed -i '/^    jcenter()$/d' build.gradle
sed -i -E 's/grgit: "4\.[0-9.]+"/grgit: "5.0.0"/' gradle/dependencies.gradle
mv .git .git-bak
./gradlew :core:jar :tools:jar -x test
# 完整 S1-S8 场景测试脚本: <repo>/tools/zk-scenario-test.sh <version>
```
