#!/usr/bin/env bash
# =============================================================================
# apply-and-build.sh — clone Kafka source, apply observer patch, build patched jar
#
# Prerequisites:
#   - JDK 17 with javac (Amazon Corretto: sudo dnf install java-17-amazon-corretto-devel)
#     NOTE: headless JRE (-headless) does NOT include javac; the Scala compiler needs it.
#   - git, ~2 GB disk for source + build artifacts
#   - ~4 vCPU recommended (2 vCPU works but slow; tested: m7g.xlarge 1m 1s)
#
# Usage:
#   ./tools/apply-and-build.sh [--kafka-version 3.7.1] [--output-dir /opt/kafka/libs]
#
# Output:
#   core/build/libs/kafka_2.13-<version>.jar     (patched core module)
#   storage/build/libs/kafka-storage-<version>.jar  (patched storage, if applicable)
# =============================================================================
set -euo pipefail

KAFKA_VERSION="${1:-3.7.1}"
PATCH_DIR="$(cd "$(dirname "$0")/.." && pwd)/patches"
WORK_DIR="${WORK_DIR:-/tmp/kafka-src-build}"

echo "═══ sample-kafka-observer: apply-and-build ═══"
echo "Kafka version: $KAFKA_VERSION"
echo "Patch dir: $PATCH_DIR"
echo "Work dir: $WORK_DIR"

# Determine patch file
MODE="zk"  # v0.3 only supports ZK; v0.5 will add kraft
PATCH_FILE="$PATCH_DIR/kafka-${KAFKA_VERSION}-${MODE}/observer.patch"
if [ ! -f "$PATCH_FILE" ]; then
  echo "ERROR: patch file not found: $PATCH_FILE"
  echo "Available patches:"; ls "$PATCH_DIR"/*/observer.patch 2>/dev/null || echo "  (none)"
  exit 1
fi

# Clone
echo ""
echo "── Step 1: Clone Kafka $KAFKA_VERSION (shallow) ──"
rm -rf "$WORK_DIR"
git clone --depth 1 --branch "$KAFKA_VERSION" https://github.com/apache/kafka.git "$WORK_DIR"
cd "$WORK_DIR"

# Apply patch
echo ""
echo "── Step 2: Apply observer patch ──"
git apply --3way "$PATCH_FILE"

# Verify 8 OBSERVER PATCH markers (safety gate)
MARKERS=$(grep -r "OBSERVER PATCH" core/src/main/scala/ | wc -l | tr -d ' ')
if [ "$MARKERS" -lt 6 ]; then
  echo "ERROR: expected >=6 OBSERVER PATCH markers, found $MARKERS — patch may have failed"
  exit 1
fi
echo "  ✅ $MARKERS OBSERVER PATCH markers found"

# Build
echo ""
echo "── Step 3: Build core + storage modules ──"
./gradlew :core:jar :storage:jar -x test --console=plain

# Verify output
echo ""
echo "── Step 4: Verify build artifacts ──"
CORE_JAR="core/build/libs/kafka_2.13-${KAFKA_VERSION}.jar"
STORAGE_JAR="storage/build/libs/kafka-storage-${KAFKA_VERSION}.jar"
if [ ! -f "$CORE_JAR" ]; then echo "ERROR: $CORE_JAR not found"; exit 1; fi
echo "  ✅ $CORE_JAR ($(du -h "$CORE_JAR" | cut -f1))"
[ -f "$STORAGE_JAR" ] && echo "  ✅ $STORAGE_JAR ($(du -h "$STORAGE_JAR" | cut -f1))"

echo ""
echo "═══ BUILD SUCCESSFUL ═══"
echo "Deploy these jars to your brokers (replace originals in /opt/kafka/libs/):"
echo "  $WORK_DIR/$CORE_JAR"
[ -f "$STORAGE_JAR" ] && echo "  $WORK_DIR/$STORAGE_JAR"
echo ""
echo "Then create /opt/kafka/observer.ids with observer broker ids (one per line),"
echo "and do a rolling restart. See docs/deployment.md for full instructions."
