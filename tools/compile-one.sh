#!/usr/bin/env bash
# compile-one.sh <version> <mode:zk|kraft>
# clone + apply 对应 patch + 构建适配 + 编译。ZK 编 core+tools; KRaft 编 core+metadata+tools。
# 自动选 JDK: 4.0+ 用 17, 其余用 11。产出 jar 留在 /tmp/build-<v>-<mode>/。
set -uo pipefail
V="${1:?version}"; MODE="${2:?mode zk|kraft}"
MAJOR=$(echo $V | cut -d. -f1); MINOR=$(echo $V | cut -d. -f2)
# JDK 选择
if [ "$MAJOR" -ge 4 ]; then JH=/usr/lib/jvm/java-17-amazon-corretto.aarch64; else JH=/usr/lib/jvm/java-11-amazon-corretto.aarch64; fi
export JAVA_HOME=$JH; export PATH=$JH/bin:$PATH
PATCH=~/obs-patches/observer-$V$([ "$MODE" = kraft ] && echo "-kraft").patch
[ -f "$PATCH" ] || PATCH=~/obs-patches/observer-$V.patch
W=/tmp/build-$V-$MODE
echo "=== [$V-$MODE] JDK=$(basename $JH) patch=$(basename $PATCH) ==="
rm -rf $W
git clone --depth 1 --branch $V https://github.com/apache/kafka.git $W >/dev/null 2>&1 || { echo "[$V-$MODE] CLONE FAIL"; exit 1; }
cd $W
git apply "$PATCH" 2>/tmp/apply-$V-$MODE.log || { echo "[$V-$MODE] APPLY FAIL"; cat /tmp/apply-$V-$MODE.log; exit 2; }
M=$(grep -rc "OBSERVER PATCH" core/src/main/scala/ metadata/src/main/java/ 2>/dev/null | awk -F: '{s+=$2} END{print s}')
echo "[$V-$MODE] applied, markers=$M"
# 构建基础设施适配(与 patch 无关)
sed -i '/^    jcenter()$/d' build.gradle 2>/dev/null || true
[ -f gradle/dependencies.gradle ] && sed -i -E 's/grgit: "4\.[0-9.]+"/grgit: "5.0.0"/' gradle/dependencies.gradle
[ -f gradle/buildscript.gradle ] && sed -i "s|url 'https://dl.bintray.com/content/netflixoss/external-gradle-plugins/'|url 'https://repo.maven.apache.org/maven2/'|" gradle/buildscript.gradle
mv .git .git-bak 2>/dev/null || true
# 编译目标
TARGETS=":core:jar :tools:jar :core:copyDependantLibs :tools:copyDependantLibs"
[ "$MODE" = kraft ] && TARGETS="$TARGETS :metadata:jar :metadata:copyDependantLibs"
echo "[$V-$MODE] building: $TARGETS"
./gradlew $TARGETS -x test --console=plain > /tmp/build-$V-$MODE.log 2>&1
RC=$?
echo "[$V-$MODE] gradle exit=$RC"
tail -3 /tmp/build-$V-$MODE.log
CJAR=core/build/libs/kafka_2.13-$V.jar
if [ -f "$CJAR" ]; then
  echo "[$V-$MODE] CORE JAR OK ($(du -h $CJAR|cut -f1))"
  jar tf "$CJAR" | grep -q "kafka/observer/ObserverIds" && echo "[$V-$MODE] ObserverIds in jar ✓" || echo "[$V-$MODE] ObserverIds MISSING ✗"
  if [ "$MODE" = kraft ]; then
    MJAR=$(ls metadata/build/libs/kafka-metadata-*.jar 2>/dev/null | head -1)
    [ -n "$MJAR" ] && jar tf "$MJAR" | grep -q "ObserverReplicas" && echo "[$V-$MODE] ObserverReplicas in metadata jar ✓" || echo "[$V-$MODE] ObserverReplicas MISSING ✗"
  fi
  echo "[$V-$MODE] RESULT=COMPILE_OK"
else
  echo "[$V-$MODE] RESULT=COMPILE_FAIL"
fi
