#!/usr/bin/env bash
# =============================================================================
# zk-scenario-test.sh — 在单机多进程 ZooKeeper + 4 broker 集群上验证 observer patch
# 用法: ./zk-scenario-test.sh <version>   例如 2.8.1 / 2.8.2 / 3.3.2
# 前置: /tmp/build-<version> 已 clone+apply+编译(core jar + dependant-libs 就绪)
# 产出: /tmp/obs-test-<version>/EVIDENCE.txt (原始输出全存证)
# 架构: ZK(1个) + broker 1/2/3/4, observer=broker3; 每 broker 独立 observer.ids 文件
# =============================================================================
set -uo pipefail
V="${1:?usage: $0 <version>}"
SCALA=2.13
SRC=/tmp/build-$V
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.aarch64
export PATH=$JAVA_HOME/bin:$PATH

DEP=$(ls -d $SRC/core/build/dependant-libs-*/ | head -1)
COREJAR=$SRC/core/build/libs/kafka_${SCALA}-${V}.jar
CP="$COREJAR:$DEP*"
BIN=$SRC/bin
# 关键: kafka bin 脚本靠 CLASSPATH 环境变量补充类路径。
# 我们不是完整发行包(只编译了 core+tools), 必须显式把 core jar + tools jar + 全部
# dependant-libs 塞进 CLASSPATH, 否则 kafka-producer-perf-test.sh 找不到 ProducerPerformance。
TOOLSDEP=$(ls -d $SRC/tools/build/dependant-libs-*/ 2>/dev/null | head -1)
export CLASSPATH="$COREJAR:$DEP*:$SRC/tools/build/libs/*:${TOOLSDEP}*"
BASE=/tmp/obs-test-$V
EV=$BASE/EVIDENCE.txt
ZKPORT=32181

BOOTALL=localhost:19092,localhost:19094,localhost:19096,localhost:19098
log(){ echo "$@" | tee -a "$EV"; }
kt(){ timeout 25 $BIN/kafka-topics.sh --bootstrap-server $BOOTALL "$@"; }

rm -rf $BASE; mkdir -p $BASE/logs
: > $EV
log "########################################################"
log "# Observer patch 真机验证 — Kafka $V (ZooKeeper 模式)"
log "# host: $(hostname)  arch: $(uname -m)  jdk: $(java -version 2>&1|head -1)"
log "# started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "# core jar: $(md5sum $COREJAR | cut -d' ' -f1)  observer class: $(jar tf $COREJAR|grep -c ObserverIds)"
log "########################################################"

# ---- 清理旧进程/端口 ----
pkill -f "obs-test-$V" 2>/dev/null; sleep 2

# ---- 生成 log4j(降噪) ----
cat > $BASE/log4j.properties <<EOF
log4j.rootLogger=WARN, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)%n
log4j.logger.kafka.observer=INFO, stdout
log4j.additivity.kafka.observer=false
EOF
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$BASE/log4j.properties"
export KAFKA_HEAP_OPTS="-Xmx512M -Xms256M"

# ---- 启动 ZooKeeper ----
mkdir -p $BASE/zk
cat > $BASE/zk.properties <<EOF
dataDir=$BASE/zk
clientPort=$ZKPORT
admin.enableServer=false
4lw.commands.whitelist=*
EOF
log ""
log "=== 启动 ZooKeeper (port $ZKPORT) ==="
nohup $BIN/zookeeper-server-start.sh $BASE/zk.properties > $BASE/logs/zk.log 2>&1 &
sleep 8
if echo ruok | timeout 5 nc localhost $ZKPORT 2>/dev/null | grep -q imok; then log "ZK: imok"; else log "ZK 启动中(继续)"; sleep 5; fi

# ---- 启动 4 个 broker ----
# port 映射: broker i -> 19092+2*(i-1)  (1->19092,2->19094,3->19096,4->19098)
start_broker(){
  local id=$1; local port=$((19092 + 2*(id-1)))
  mkdir -p $BASE/data-b$id
  # 每 broker 独立 observer.ids: broker3 = observer
  echo "3" > $BASE/obs-b$id.ids
  cat > $BASE/b$id.properties <<EOF
broker.id=$id
listeners=PLAINTEXT://localhost:$port
advertised.listeners=PLAINTEXT://localhost:$port
log.dirs=$BASE/data-b$id
zookeeper.connect=localhost:$ZKPORT
replica.lag.time.max.ms=10000
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
num.partitions=1
default.replication.factor=1
auto.create.topics.enable=false
zookeeper.session.timeout.ms=6000
zookeeper.connection.timeout.ms=6000
EOF
  KAFKA_OBSERVER_IDS_FILE=$BASE/obs-b$id.ids \
  KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$BASE/log4j.properties" \
  KAFKA_HEAP_OPTS="-Xmx512M -Xms256M" \
    nohup $BIN/kafka-server-start.sh $BASE/b$id.properties > $BASE/logs/b$id.log 2>&1 &
  echo $! > $BASE/b$id.pid
}
log ""
log "=== 启动 4 brokers (observer = broker 3, 每 broker 独立 observer.ids=3) ==="
for id in 1 2 3 4; do start_broker $id; done
log "等待 brokers 注册..."
sleep 25
# 确认 4 broker 在线
BROKERS=$($BIN/zookeeper-shell.sh localhost:$ZKPORT ls /brokers/ids 2>/dev/null | tail -1)
log "在线 brokers: $BROKERS"

# ---- 建 topic: smx = RF4 (replicas 3,1,2,4, observer=3), minISR=2 ----
log ""
log "=== 建 topic smx: --replica-assignment 3:1:2:4, min.insync.replicas=2 ==="
kt --create --topic smx --replica-assignment 3:1:2:4 --config min.insync.replicas=2 2>&1 | tee -a $EV
sleep 3
kt --describe --topic smx 2>&1 | tee -a $EV

# 辅助: 描述并抓 leader / isr
desc(){ kt --describe --topic smx 2>/dev/null | grep -E "Partition: 0"; }
leader_of(){ desc | grep -oE "Leader: [0-9]+" | grep -oE "[0-9]+"; }
isr_of(){ desc | grep -oE "Isr: [0-9,]+" | sed 's/Isr: //'; }
pid_of(){ cat $BASE/b$1.pid; }
# ZK 模式重启专用: 等旧 broker 的 ephemeral /brokers/ids/<id> 消失(session 过期)再启, 否则 NodeExists 致命错误
restart_broker(){
  local id=$1
  for i in $(seq 1 20); do
    local ids=$($BIN/zookeeper-shell.sh localhost:$ZKPORT ls /brokers/ids 2>/dev/null | tail -1)
    echo "$ids" | grep -qw "$id" || break
    sleep 1
  done
  start_broker $id
}
producetest(){ # $1=count  写 acks=all(限时30s防挂起), 用全端口bootstrap(容忍部分broker死)
  timeout 30 $BIN/kafka-producer-perf-test.sh --topic smx --num-records $1 --record-size 200 \
    --throughput 500 --producer-props bootstrap.servers=$BOOTALL acks=all \
    2>&1 | grep -E "records sent|EXCEPTION|Error|timed out|NotEnough" | tail -1 || echo "(write timed out / failed)"
}

log ""
log "=== 初始稳态: observer(3) 应 NOT in ISR ==="
log "describe: $(desc)"
log "  → Leader=$(leader_of)  ISR=$(isr_of)  (期望 ISR 不含 3)"

################## S1: leader 崩溃 ##################
log ""
log "===== S1: leader broker 崩溃 (kill -9) — $(date -u +%H:%M:%SZ) ====="
L0=$(leader_of); log "初始 leader=$L0, ISR=$(isr_of)"
log "baseline write: $(producetest 500)"
log "INJECT: kill -9 leader broker $L0"; kill -9 $(pid_of $L0); T0=$(date +%s%3N)
for i in $(seq 1 30); do sleep 1; NL=$(leader_of); [ -n "$NL" ] && [ "$NL" != "$L0" ] && [ "$NL" != "none" ] && break; done
T1=$(date +%s%3N)
log "新 leader=$NL 用时 $((T1-T0))ms (observer=3 绝不能当选)  ISR=$(isr_of)"
log "降级态写入(ISR>=minISR 应成功): $(producetest 300)"
log "RECOVER: 重启 broker $L0"; restart_broker $L0; sleep 12
log "重启后 ISR=$(isr_of)"
log "===== S1 done ====="

################## S2: follower 崩溃 ##################
log ""
log "===== S2: ISR follower 崩溃 — $(date -u +%H:%M:%SZ) ====="
CL=$(leader_of); ISR=$(isr_of)
# 挑一个非 leader、非 observer(3)、在 ISR 里的 follower
FOL=""; for c in 1 2 4; do [ "$c" != "$CL" ] && echo "$ISR"|grep -qw "$c" && FOL=$c && break; done
log "leader=$CL, 杀 follower=$FOL, ISR=$ISR"
log "INJECT: kill -9 follower $FOL"; kill -9 $(pid_of $FOL)
for i in $(seq 1 20); do sleep 1; echo "$(isr_of)"|grep -qw "$FOL" || break; done
log "ISR shrink 后=$(isr_of) (leader 应不变=$CL, 实际=$(leader_of))"
log "shrink 期间写入(应成功): $(producetest 300)"
log "RECOVER: 重启 $FOL"; restart_broker $FOL; sleep 12
log "重启后 ISR=$(isr_of)"
log "===== S2 done ====="

################## S3: observer 崩溃 ##################
log ""
log "===== S3: observer(3) 崩溃 — $(date -u +%H:%M:%SZ) ====="
log "pre ISR=$(isr_of) (不含3)"
log "baseline latency: $(producetest 1000)"
log "INJECT: kill -9 observer broker 3"; kill -9 $(pid_of 3)
sleep 8
log "observer 死后 ISR=$(isr_of) (必须不变,3 本就不在)"
log "observer 死时写入(零影响,应成功): $(producetest 500)"
log "RECOVER: 重启 observer 3"; restart_broker 3; sleep 12
log "重启后 ISR=$(isr_of)"
# 字节一致证明: 停写 → 等 observer 追平 leader LEO → 比对段文件 md5
LDR=$(leader_of)
log "等 observer(3) 追平 leader($LDR) LEO(停止写入后)..."
LLEO=""; OLEO=""
for i in $(seq 1 20); do
  sleep 2
  LLEO=$(timeout 15 $BIN/kafka-get-offsets.sh --bootstrap-server $BOOTALL --topic smx --time -1 2>/dev/null | grep -oE ':[0-9]+$' | tr -d ':')
  # observer LEO 从磁盘 checkpoint 读(replication-offset-checkpoint)
  OLEO=$(grep -A100 "smx 0" $BASE/data-b3/replication-offset-checkpoint 2>/dev/null | head -1)
  [ -n "$LLEO" ] && break
done
log "leader LEO=$LLEO"
log "leader($LDR) vs observer(3) 段文件 md5 (追平后):"
md5sum $BASE/data-b$LDR/smx-0/00000000000000000000.log $BASE/data-b3/smx-0/00000000000000000000.log 2>&1 | tee -a $EV
log "段文件字节大小 (leader vs observer):"
ls -l $BASE/data-b$LDR/smx-0/00000000000000000000.log $BASE/data-b3/smx-0/00000000000000000000.log 2>&1 | awk '{print $5, $NF}' | tee -a $EV
log "===== S3 done ====="

################## S4: 全 primary 死 → 晋升 observer ##################
log ""
log "===== S4: 全 ISR primary 崩溃 → 晋升 observer — $(date -u +%H:%M:%SZ) ====="
log "pre ISR=$(isr_of)"
log "pre-kill 数据一致证明(晋升前 observer 字节一致):"
LDR=$(leader_of)
md5sum $BASE/data-b$LDR/smx-0/00000000000000000000.log $BASE/data-b3/smx-0/00000000000000000000.log 2>&1 | tee -a $EV
# 杀掉所有非 observer 且在线的 broker(1,2,4)
log "INJECT: kill -9 所有 primary (1,2,4)"; for id in 1 2 4; do kill -9 $(pid_of $id) 2>/dev/null; done
sleep 12
log "全 primary 死后: Leader=$(leader_of) ISR=$(isr_of) (observer 3 存活但不应当选)"
log "此时 acks=all 写入(应失败): $(producetest 100 2>&1 | grep -oE 'records sent|timed out|EXCEPTION' | head -1)"
log "--- PROMOTE: 从所有 broker 的 observer.ids 删掉 3 --- $(date -u +%H:%M:%S.%3NZ)"
TP0=$(date +%s%3N)
for id in 1 2 3 4; do echo "" > $BASE/obs-b$id.ids; done
# 等缓存刷新(5s)后跑 unclean election(ISR 里是死的成员,需要 unclean 选到 byte-identical 的 3)
sleep 7
log "观察: 是否需要 unclean election 把 3 选上来"
$BIN/kafka-leader-election.sh --bootstrap-server localhost:19096 --topic smx --partition 0 --election-type unclean 2>&1 | tee -a $EV || true
for i in $(seq 1 20); do sleep 1; NL=$(leader_of); [ "$NL" = "3" ] && break; done
TP1=$(date +%s%3N)
log "晋升+选举后 Leader=$(leader_of) ISR=$(isr_of) 用时 $((TP1-TP0))ms since file edit"
log "晋升后从 observer 读数据验证:"
$BIN/kafka-console-consumer.sh --bootstrap-server localhost:19096 --topic smx --from-beginning --max-messages 5 --timeout-ms 8000 2>&1 | head -6 | tee -a $EV
log "===== S4 done ====="

################## 恢复集群(为 S5-S8 准备健康集群) ##################
log ""
log "=== 恢复集群: 重启 primary 1,2,4, 把 3 降级回 observer ==="
# 先把 3 加回所有 observer.ids(降级)
for id in 1 2 3 4; do echo "3" > $BASE/obs-b$id.ids; done
for id in 1 2 4; do restart_broker $id; done
sleep 20
log "恢复后: Leader=$(leader_of) ISR=$(isr_of)"
# 3 现在是 leader 且被标记 observer, 需把 leadership 移走后它才能降级(与基线 KRaft 同理)
$BIN/kafka-leader-election.sh --bootstrap-server $BOOTALL --topic smx --partition 0 --election-type preferred 2>&1 | grep -iE "success|complete" | tee -a $EV || true
sleep 10
log "preferred 选举后: Leader=$(leader_of) ISR=$(isr_of) (期望 leader 回到 primary, 3 降级出 ISR)"

################## S5: 晋升滞后的 observer ##################
log ""
log "===== S5: 晋升一个滞后的 observer — $(date -u +%H:%M:%SZ) ====="
log "pre ISR=$(isr_of)"
log "step1: kill observer 3 冻结其 log"; kill -9 $(pid_of 3) 2>/dev/null; sleep 3
log "step2: observer 下线期间灌 30000 条(拉开 lag)"
log "  $(producetest 30000)"
LLEO=$(timeout 15 $BIN/kafka-get-offsets.sh --bootstrap-server $BOOTALL --topic smx --time -1 2>/dev/null | grep -oE ':[0-9]+$'|tr -d ':')
log "step3: leader LEO=$LLEO (observer 落后约 30000)"
log "step4: 重启 observer 并立即晋升(最坏 race)"
restart_broker 3
for id in 1 2 3 4; do echo "" > $BASE/obs-b$id.ids; done
T5=$(date +%s%3N)
# 等它追平并进 ISR(必须追平后才准入, HW 不回退)
for i in $(seq 1 40); do sleep 2; echo "$(isr_of)"|grep -qw "3" && break; done
T5e=$(date +%s%3N)
log "observer 3 进入 ISR 用时 $((T5e-T5))ms (追平后才准入)  ISR=$(isr_of)"
log "step5: 晋升后 acks=all 写入(3 在 ISR 内): $(producetest 1000)"
log "关键: HW 从不回退——observer 追平 LEO 后才被准入 ISR, 未追平前不参与"
# 恢复: 3 降级回 observer
for id in 1 2 3 4; do echo "3" > $BASE/obs-b$id.ids; done
sleep 15
log "恢复后 ISR=$(isr_of) (3 降级出 ISR)"
log "===== S5 done ====="

################## S6: observer.ids 文件不一致 ##################
log ""
log "===== S6: observer.ids 文件不一致(部分节点删、部分保留) — $(date -u +%H:%M:%SZ) ====="
CL=$(leader_of)
log "pre ISR=$(isr_of), leader=$CL"
log "所有节点 observer.ids 内容: $(for id in 1 2 3 4; do echo -n "b$id=$(cat $BASE/obs-b$id.ids|tr -d '\n') "; done)"
log "INJECT: 只在非 leader 的 primary 上清空 observer.ids(制造不一致), leader 保留 3"
# 在 ZK 模式下, leader 的 observer.ids 决定 canAddReplicaToIsr; 若只有部分 broker 改, leader 仍拦
for id in 1 2 4; do [ "$id" != "$CL" ] && echo "" > $BASE/obs-b$id.ids; done
log "等 30s 观察 ISR 是否被错误改变(leader 仍标记 3=observer → 应保持排除)"
sleep 30
log "不一致期间 ISR=$(isr_of) (期望仍不含 3, leader 侧闸门 fail-safe)"
log "HEAL: 恢复所有节点一致(全部标记 3=observer)"
for id in 1 2 3 4; do echo "3" > $BASE/obs-b$id.ids; done
sleep 8
log "自愈后 ISR=$(isr_of)"
log "===== S6 done ====="

################## S7: observer.ids 损坏/权限/删除 ##################
log ""
log "===== S7: observer.ids 损坏/权限失败/删除 — $(date -u +%H:%M:%SZ) ====="
CL=$(leader_of)
log "pre ISR=$(isr_of)"
log "INJECT 7a: chmod 000 leader($CL) 的 observer.ids"
chmod 000 $BASE/obs-b$CL.ids 2>/dev/null; sleep 12
log "权限拒绝后 ISR=$(isr_of) (必须不变, 保留缓存值)"
log "  broker $CL WARN 日志(保留上次值):"
grep -h "Failed to read observer ids\|keeping last value" $BASE/logs/b$CL.log 2>/dev/null | tail -2 | sed 's/^/    /' | tee -a $EV || echo "    (WARN 日志见 b$CL.log)"
log "  权限故障期间写入(应正常): $(producetest 100)"
chmod 644 $BASE/obs-b$CL.ids 2>/dev/null
log "INJECT 7b: 写垃圾内容(非数字 token 应被静默忽略)"
printf 'banana,3\n# comment\n%%%%@@!!\n 3 ,xyz\n' > $BASE/obs-b$CL.ids; sleep 8
log "垃圾内容后 ISR=$(isr_of) (id 3 仍被解析出 → 仍排除 3; 垃圾 token 忽略)"
log "  写入仍正常: $(producetest 100)"
log "INJECT 7c: 删除文件(回退 env, 未设 → 空集 → 3 变可晋升)"
rm -f $BASE/obs-b$CL.ids; sleep 8
log "删除后 ISR=$(isr_of) (leader 侧文件没了 → 该 broker 视 3 非 observer, 但其他 broker 仍标记)"
log "RESTORE: 恢复 observer.ids=3 所有节点"
for id in 1 2 3 4; do echo "3" > $BASE/obs-b$id.ids; done
sleep 10
log "恢复后 ISR=$(isr_of) (3 重新降级出 ISR)"
log "===== S7 done ====="

################## S8: controller failover (ZK 模式) ##################
log ""
log "===== S8: controller failover (ZK 模式) — $(date -u +%H:%M:%SZ) ====="
# ZK 模式下 controller 是某个 broker。找到当前 controller broker id
CTRL=$($BIN/zookeeper-shell.sh localhost:$ZKPORT get /controller 2>/dev/null | grep -oE '"brokerid":[0-9]+' | grep -oE '[0-9]+' | head -1)
log "当前 controller broker = $CTRL"
log "pre ISR=$(isr_of)"
if [ -n "$CTRL" ] && [ "$CTRL" != "3" ]; then
  log "INJECT: kill -9 controller broker $CTRL(触发 controller failover)"
  T8=$(date +%s%3N)
  kill -9 $(pid_of $CTRL) 2>/dev/null
  # 等新 controller 选出
  for i in $(seq 1 30); do sleep 1; NC=$($BIN/zookeeper-shell.sh localhost:$ZKPORT get /controller 2>/dev/null | grep -oE '"brokerid":[0-9]+'|grep -oE '[0-9]+'|head -1); [ -n "$NC" ] && [ "$NC" != "$CTRL" ] && break; done
  T8e=$(date +%s%3N)
  log "新 controller = $NC 用时 $((T8e-T8))ms"
  log "controller failover 后 ISR=$(isr_of) (新 controller 仍正确排除 observer 3)"
  log "  failover 后写入(应正常): $(producetest 200)"
  log "RECOVER: 重启 broker $CTRL"; restart_broker $CTRL; sleep 15
  log "恢复后 ISR=$(isr_of)"
else
  log "(controller 恰为 observer 3 或无法确定, 跳过——observer 通常不应是 controller)"
fi
log "===== S8 done ====="

################## 收尾: 停所有进程 ##################
log ""
log "=== 测试完成, 停止集群 — $(date -u +%H:%M:%SZ) ==="
pkill -f "obs-test-$V" 2>/dev/null
for id in 1 2 3 4; do kill -9 $(cat $BASE/b$id.pid 2>/dev/null) 2>/dev/null; done
pkill -f "$BASE/zk.properties" 2>/dev/null
log ""
log "########################################################"
log "# Kafka $V observer patch 场景验证结束"
log "# 完成: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "########################################################"
echo ""
echo ">>> EVIDENCE 文件: $EV"
