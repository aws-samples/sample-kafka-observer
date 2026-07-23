#!/usr/bin/env bash
# kraft-scenario-test.sh <version> — KRaft 模式 observer patch S1-S8 验证
# 拓扑: 独立 controller quorum(101/102/103) + 4 broker(1-4), observer=broker3
# 每进程独立 KAFKA_OBSERVER_IDS_FILE。前置: ~/work/build-<v>-kraft 已编译。
set -uo pipefail
V="${1:?version}"; SCALA=2.13
MAJOR=$(echo $V|cut -d. -f1)
if [ "$MAJOR" -ge 4 ]; then JH=/usr/lib/jvm/java-17-amazon-corretto.aarch64; else JH=/usr/lib/jvm/java-11-amazon-corretto.aarch64; fi
export JAVA_HOME=$JH; export PATH=$JH/bin:$PATH
SRC=$HOME/work/build-$V-kraft
DEP=$(ls -d $SRC/core/build/dependant-libs-*/ | head -1)
COREJAR=$SRC/core/build/libs/kafka_${SCALA}-${V}.jar
MDJAR=$(ls $SRC/metadata/build/libs/kafka-metadata-*.jar 2>/dev/null | head -1)
TOOLSDEP=$(ls -d $SRC/tools/build/dependant-libs-*/ 2>/dev/null | head -1)
# metadata patched jar 必须在 core jar 之前(覆盖原版)
export CLASSPATH="$MDJAR:$COREJAR:$DEP*:$SRC/tools/build/libs/*:${TOOLSDEP}*"
BIN=$SRC/bin
BASE=$HOME/work/obs-test-$V-kraft
EV=$BASE/EVIDENCE.txt
BOOTALL=localhost:19092,localhost:19094,localhost:19096,localhost:19098

log(){ echo "$@" | tee -a "$EV"; }
kt(){ timeout 25 $BIN/kafka-topics.sh --bootstrap-server $BOOTALL "$@"; }
rm -rf $BASE; mkdir -p $BASE/logs; : > $EV

log "########################################################"
log "# Observer patch 真机验证 — Kafka $V (KRaft 模式)"
log "# host: $(hostname)  arch: $(uname -m)  jdk: $(java -version 2>&1|head -1)"
log "# started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "# core jar md5: $(md5sum $COREJAR|cut -d' ' -f1)  metadata jar: $(basename $MDJAR)"
log "# ObserverIds in core: $(jar tf $COREJAR|grep -c ObserverIds)  ObserverReplicas in metadata: $(jar tf $MDJAR|grep -c ObserverReplicas)"
log "########################################################"

# log4j 降噪
cat > $BASE/log4j.properties <<EOF
log4j.rootLogger=WARN, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)%n
log4j.logger.kafka.observer=INFO, stdout
log4j.logger.org.apache.kafka.controller=INFO, stdout
EOF
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$BASE/log4j.properties"
export KAFKA_HEAP_OPTS="-Xmx512M -Xms256M"

CLUSTER_ID=$($BIN/kafka-storage.sh random-uuid 2>/dev/null)
log "cluster.id=$CLUSTER_ID"
# controller quorum voters
VOTERS="101@localhost:19101,102@localhost:19102,103@localhost:19103"

# 生成 controller 配置
ctrl_cfg(){
  local id=$1; local port=$((19000 + id))
  cat > $BASE/c$id.properties <<EOF
process.roles=controller
node.id=$id
controller.quorum.voters=$VOTERS
listeners=CONTROLLER://localhost:$port
controller.listener.names=CONTROLLER
log.dirs=$BASE/data-c$id
EOF
  echo "3" > $BASE/obs-c$id.ids
}
# 生成 broker 配置
brk_cfg(){
  local id=$1; local port=$((19092 + 2*(id-1)))
  cat > $BASE/b$id.properties <<EOF
process.roles=broker
node.id=$id
controller.quorum.voters=$VOTERS
listeners=PLAINTEXT://localhost:$port
advertised.listeners=PLAINTEXT://localhost:$port
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$BASE/data-b$id
replica.lag.time.max.ms=10000
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
num.partitions=1
default.replication.factor=1
auto.create.topics.enable=false
EOF
  echo "3" > $BASE/obs-b$id.ids
}

log ""; log "=== 生成配置 + format storage ==="
for id in 101 102 103; do ctrl_cfg $id; done
for id in 1 2 3 4; do brk_cfg $id; done
# format 每个节点
for id in 101 102 103; do
  $BIN/kafka-storage.sh format -t $CLUSTER_ID -c $BASE/c$id.properties >/dev/null 2>&1
done
for id in 1 2 3 4; do
  $BIN/kafka-storage.sh format -t $CLUSTER_ID -c $BASE/b$id.properties >/dev/null 2>&1
done

start_ctrl(){ local id=$1
  KAFKA_OBSERVER_IDS_FILE=$BASE/obs-c$id.ids KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$BASE/log4j.properties" KAFKA_HEAP_OPTS="-Xmx256M -Xms128M" \
    nohup $BIN/kafka-server-start.sh $BASE/c$id.properties > $BASE/logs/c$id.log 2>&1 & echo $! > $BASE/c$id.pid; }
start_brk(){ local id=$1
  KAFKA_OBSERVER_IDS_FILE=$BASE/obs-b$id.ids KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$BASE/log4j.properties" KAFKA_HEAP_OPTS="-Xmx512M -Xms256M" \
    nohup $BIN/kafka-server-start.sh $BASE/b$id.properties > $BASE/logs/b$id.log 2>&1 & echo $! > $BASE/b$id.pid; }

log "=== 启动 controller quorum(101/102/103) ==="
for id in 101 102 103; do start_ctrl $id; done
sleep 12
log "=== 启动 4 brokers(observer=3) ==="
for id in 1 2 3 4; do start_brk $id; done
sleep 20

desc(){ kt --describe --topic smx 2>/dev/null | grep -E "Partition: 0"; }
leader_of(){ desc | grep -oE "Leader: [0-9]+" | grep -oE "[0-9]+"; }
isr_of(){ desc | grep -oE "Isr: [0-9,]+" | sed 's/Isr: //'; }
pid_of(){ cat $BASE/b$1.pid; }
producetest(){ timeout 30 $BIN/kafka-producer-perf-test.sh --topic smx --num-records $1 --record-size 200 --throughput 500 --producer-props bootstrap.servers=$BOOTALL acks=all 2>&1 | grep -E "records sent|EXCEPTION|timed out|NotEnough" | tail -1 || echo "(write failed)"; }

log ""; log "=== 建 topic smx: replica-assignment 3:1:2:4, minISR=2 ==="
kt --create --topic smx --replica-assignment 3:1:2:4 --config min.insync.replicas=2 2>&1 | tee -a $EV
sleep 3
log "初始: $(desc)"; log "  → Leader=$(leader_of) ISR=$(isr_of) (期望不含 observer 3)"

# ---- S1 ----
log ""; log "===== S1: leader 崩溃 — $(date -u +%H:%M:%SZ) ====="
L0=$(leader_of); log "leader=$L0 ISR=$(isr_of)"
log "baseline: $(producetest 500)"
kill -9 $(pid_of $L0); T0=$(date +%s%3N)
for i in $(seq 1 30); do sleep 1; NL=$(leader_of); [ -n "$NL" ] && [ "$NL" != "$L0" ] && [ "$NL" != "none" ] && break; done
T1=$(date +%s%3N)
log "新 leader=$NL 用时 $((T1-T0))ms (observer 3 不当选) ISR=$(isr_of)"
log "降级写入: $(producetest 300)"
start_brk $L0; sleep 15; log "重启后 ISR=$(isr_of)"; log "===== S1 done ====="

# ---- S2 ----
log ""; log "===== S2: follower 崩溃 — $(date -u +%H:%M:%SZ) ====="
CL=$(leader_of); ISR=$(isr_of); FOL=""
for c in 1 2 4; do [ "$c" != "$CL" ] && echo "$ISR"|grep -qw "$c" && FOL=$c && break; done
log "leader=$CL 杀 follower=$FOL"; kill -9 $(pid_of $FOL)
for i in $(seq 1 20); do sleep 1; echo "$(isr_of)"|grep -qw "$FOL" || break; done
log "shrink 后 ISR=$(isr_of) leader=$(leader_of)"; log "写入: $(producetest 300)"
start_brk $FOL; sleep 15; log "重启后 ISR=$(isr_of)"; log "===== S2 done ====="

# ---- S3 ----
log ""; log "===== S3: observer 崩溃 — $(date -u +%H:%M:%SZ) ====="
log "pre ISR=$(isr_of)"; log "baseline latency: $(producetest 1000)"
kill -9 $(pid_of 3); sleep 8
log "observer 死后 ISR=$(isr_of) (不变)"; log "写入零影响: $(producetest 500)"
start_brk 3; sleep 15
LDR=$(leader_of)
log "追平后 leader($LDR) vs observer(3) 段 md5:"
for i in $(seq 1 15); do sleep 2; L=$(timeout 15 $BIN/kafka-get-offsets.sh --bootstrap-server $BOOTALL --topic smx --time -1 2>/dev/null|grep -oE ':[0-9]+$'|tr -d ':'); [ -n "$L" ] && break; done
md5sum $BASE/data-b$LDR/smx-0/00000000000000000000.log $BASE/data-b3/smx-0/00000000000000000000.log 2>&1 | tee -a $EV
log "===== S3 done ====="

# ---- S4 ----
log ""; log "===== S4: 全 primary 崩溃 → 晋升 — $(date -u +%H:%M:%SZ) ====="
log "pre ISR=$(isr_of)"; LDR=$(leader_of)
log "晋升前字节一致:"; md5sum $BASE/data-b$LDR/smx-0/00000000000000000000.log $BASE/data-b3/smx-0/00000000000000000000.log 2>&1 | tee -a $EV
for id in 1 2 4; do kill -9 $(pid_of $id) 2>/dev/null; done; sleep 12
log "全 primary 死后 Leader=$(leader_of) ISR=$(isr_of)"
log "写入应失败: $(producetest 100)"
log "--- PROMOTE: 所有 broker+controller 的 observer.ids 删 3 --- $(date -u +%H:%M:%S.%3NZ)"
TP0=$(date +%s%3N)
for id in 1 2 3 4; do echo "" > $BASE/obs-b$id.ids; done
for id in 101 102 103; do echo "" > $BASE/obs-c$id.ids; done
sleep 7
$BIN/kafka-leader-election.sh --bootstrap-server localhost:19096 --topic smx --partition 0 --election-type unclean 2>&1 | tee -a $EV || true
for i in $(seq 1 20); do sleep 1; [ "$(leader_of)" = "3" ] && break; done
TP1=$(date +%s%3N)
log "晋升后 Leader=$(leader_of) ISR=$(isr_of) 用时 $((TP1-TP0))ms"
log "从 observer 读回:"; $BIN/kafka-console-consumer.sh --bootstrap-server localhost:19096 --topic smx --from-beginning --max-messages 5 --timeout-ms 8000 2>&1 | head -6 | tee -a $EV
log "===== S4 done ====="

# KRaft 专用: 确保 observer(3) 既不是 leader 也不在 ISR。
# KRaft 限制: leader observer 不能热降级, 必须先移走 leadership(重启该 broker 强制交出)。
ensure_observer_demoted(){
  for attempt in 1 2 3 4 5; do
    local ldr=$(leader_of); local isr=$(isr_of)
    # observer 不在 ISR 且不是 leader → 完成
    if ! echo "$isr" | grep -qw 3 && [ "$ldr" != "3" ]; then return 0; fi
    if [ "$ldr" = "3" ]; then
      # observer 是 leader: 重启它强制交出 leadership
      kill -9 $(pid_of 3) 2>/dev/null; sleep 8
      # 先让 primary 接管 leadership
      $BIN/kafka-leader-election.sh --bootstrap-server $BOOTALL --topic smx --partition 0 --election-type preferred >/dev/null 2>&1 || true
      sleep 6; start_brk 3; sleep 12
    else
      # observer 在 ISR 但非 leader: preferred 选举 + 等 isr-expiration 降级
      $BIN/kafka-leader-election.sh --bootstrap-server $BOOTALL --topic smx --partition 0 --election-type preferred >/dev/null 2>&1 || true
      sleep 15
    fi
  done
}

# ---- 恢复集群 for S5-S8 ----
log ""; log "=== 恢复集群 ==="
for id in 1 2 3 4; do echo "3" > $BASE/obs-b$id.ids; done
for id in 101 102 103; do echo "3" > $BASE/obs-c$id.ids; done
for id in 1 2 4; do start_brk $id; done
sleep 20
ensure_observer_demoted
log "恢复后 Leader=$(leader_of) ISR=$(isr_of)"

# ---- S5 ----
log ""; log "===== S5: 晋升滞后 observer — $(date -u +%H:%M:%SZ) ====="
log "pre ISR=$(isr_of)"; kill -9 $(pid_of 3) 2>/dev/null; sleep 3
log "灌 30000: $(producetest 30000)"
start_brk 3
for id in 1 2 3 4; do echo "" > $BASE/obs-b$id.ids; done
for id in 101 102 103; do echo "" > $BASE/obs-c$id.ids; done
T5=$(date +%s%3N)
for i in $(seq 1 40); do sleep 2; echo "$(isr_of)"|grep -qw "3" && break; done
T5e=$(date +%s%3N)
log "observer 追平进 ISR 用时 $((T5e-T5))ms ISR=$(isr_of)"
log "晋升后写入: $(producetest 1000)"
for id in 1 2 3 4; do echo "3" > $BASE/obs-b$id.ids; done
for id in 101 102 103; do echo "3" > $BASE/obs-c$id.ids; done
sleep 15; ensure_observer_demoted; log "恢复后 Leader=$(leader_of) ISR=$(isr_of)"; log "===== S5 done ====="

# ---- S6: controller 侧文件不一致 ----
log ""; log "===== S6: observer.ids 不一致(broker删/controller留) — $(date -u +%H:%M:%SZ) ====="
log "pre ISR=$(isr_of)"
log "INJECT: 只清 broker 的 observer.ids, controller 保留 3 → controller 应拒绝 AlterPartition"
for id in 1 2 3 4; do echo "" > $BASE/obs-b$id.ids; done
sleep 30
log "不一致期间 ISR=$(isr_of) (期望仍不含 3, controller 侧 fail-safe)"
log "controller 拒绝日志:"; grep -h "ineligible\|INELIGIBLE\|observer" $BASE/logs/c*.log 2>/dev/null | tail -2 | sed 's/^/  /' | tee -a $EV || true
for id in 1 2 3 4; do echo "3" > $BASE/obs-b$id.ids; done
sleep 8; log "自愈后 ISR=$(isr_of)"; log "===== S6 done ====="

# ---- S7 ----
log ""; log "===== S7: observer.ids 损坏/权限/删除 — $(date -u +%H:%M:%SZ) ====="
CL=$(leader_of); log "pre ISR=$(isr_of)"
log "7a: chmod 000 leader($CL) 文件"; chmod 000 $BASE/obs-b$CL.ids 2>/dev/null; sleep 12
log "权限拒绝后 ISR=$(isr_of) (不变)"; grep -h "keeping last value" $BASE/logs/b$CL.log 2>/dev/null | tail -1 | sed 's/^/  /' | tee -a $EV || true
log "写入: $(producetest 100)"; chmod 644 $BASE/obs-b$CL.ids 2>/dev/null
log "7b: 写垃圾"; printf 'banana,3\n# c\n%%%%@@\n 3 ,xyz\n' > $BASE/obs-b$CL.ids; sleep 8
log "垃圾后 ISR=$(isr_of) (3 仍解析出)"
log "7c: 删文件"; rm -f $BASE/obs-b$CL.ids; sleep 8
log "删除后 ISR=$(isr_of)"
for id in 1 2 3 4; do echo "3" > $BASE/obs-b$id.ids; done
sleep 10; log "恢复后 ISR=$(isr_of)"; log "===== S7 done ====="

# ---- S8: controller failover (KRaft: kill active controller) ----
log ""; log "===== S8: controller failover(KRaft quorum) — $(date -u +%H:%M:%SZ) ====="
log "pre ISR=$(isr_of)"
# 找 active controller (leader of quorum) — 简单起见 kill 101, 观察 quorum 重选
log "INJECT: kill controller 101(quorum 成员)"; T8=$(date +%s%3N)
kill -9 $(cat $BASE/c101.pid) 2>/dev/null
sleep 10
log "controller 挂后 ISR=$(isr_of) (剩余 quorum 102/103 接管, 仍排除 observer)"
log "failover 后写入: $(producetest 200)"
start_ctrl 101; sleep 12; log "恢复后 ISR=$(isr_of)"; log "===== S8 done ====="

# 收尾
log ""; log "=== 停止集群 — $(date -u +%H:%M:%SZ) ==="
for id in 1 2 3 4; do kill -9 $(cat $BASE/b$id.pid 2>/dev/null) 2>/dev/null; done
for id in 101 102 103; do kill -9 $(cat $BASE/c$id.pid 2>/dev/null) 2>/dev/null; done
log "########################################################"
log "# Kafka $V (KRaft) observer patch 场景验证结束"
log "# 完成: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "########################################################"
echo ">>> EVIDENCE: $EV"
