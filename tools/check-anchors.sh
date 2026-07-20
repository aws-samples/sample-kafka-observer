#!/usr/bin/env bash
# =============================================================================
# check-anchors.sh — verify the observer patch anchors exist verbatim in a
# given Kafka version, WITHOUT cloning the full source tree.
#
# This is the local/offline counterpart of the CI matrix in
# .github/workflows/build-verify.yml: it downloads only the two files the
# canonical patch touches (via GitHub raw) and checks that each of the five
# anchor lines appears EXACTLY ONCE. If all five anchors are intact, the
# single canonical patch (patches/kafka-3.7.1-zk/observer.patch) is expected
# to apply cleanly with `git apply --3way`.
#
# Usage:
#   ./tools/check-anchors.sh                      # default matrix: 3.6.2 3.7.1 3.8.1 3.9.1
#   ./tools/check-anchors.sh 3.6.2 4.0.0          # explicit versions
#
# Exit code: 0 if every anchor is exactly-once in every version, 1 otherwise.
# =============================================================================
set -euo pipefail

DEFAULT_VERSIONS=(3.6.2 3.7.1 3.8.1 3.9.1)
VERSIONS=("$@")
if [ "${#VERSIONS[@]}" -eq 0 ]; then
  VERSIONS=("${DEFAULT_VERSIONS[@]}")
fi

RAW_BASE="https://raw.githubusercontent.com/apache/kafka"
PARTITION_PATH="core/src/main/scala/kafka/cluster/Partition.scala"
PSM_PATH="core/src/main/scala/kafka/controller/PartitionStateMachine.scala"

# The five anchors the patch hunks attach to. Each must appear exactly once.
# Keep these byte-identical to the context lines in observer.patch — if you
# edit the patch, update this table.
#   A1  Partition.canAddReplicaToIsr    — promotion gate insertion point
#   A2  Partition.maybeIncrementLeaderHW — HW-wait predicate insertion point
#   A3  Partition.getOutOfSyncReplicas  — demotion hook replaced line
#   A4  PartitionStateMachine initial ISR — replaced line (new-topic ISR)
#   A5  PartitionStateMachine unclean election — replaced line
ANCHOR_KEYS=(A1 A2 A3 A4 A5)

anchor_file() {
  case "$1" in
    A1|A2|A3) echo "$PARTITION_PATH" ;;
    A4|A5)    echo "$PSM_PATH" ;;
  esac
}

anchor_text() {
  case "$1" in
    A1) echo '  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {' ;;
    A2) echo '      def shouldWaitForReplicaToJoinIsr: Boolean = {' ;;
    A3) echo '      candidateReplicaIds.filter(replicaId => isFollowerOutOfSync(replicaId, leaderEndOffset, currentTimeMs, maxLagMs))' ;;
    A4) echo '      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)' ;;
    A5) echo '        val leaderOpt = assignment.find(liveReplicas.contains)' ;;
  esac
}

anchor_desc() {
  case "$1" in
    A1) echo "canAddReplicaToIsr (promotion gate)" ;;
    A2) echo "shouldWaitForReplicaToJoinIsr (HW gate)" ;;
    A3) echo "getOutOfSyncReplicas (demotion hook)" ;;
    A4) echo "initial ISR at topic creation" ;;
    A5) echo "unclean leader election" ;;
  esac
}

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

fetch() { # fetch <version> <repo-path> <out-file>; returns non-zero on HTTP error
  curl -fsSL --retry 3 --retry-delay 2 "$RAW_BASE/$1/$2" -o "$3"
}

echo "═══ observer patch anchor check ═══"
echo "Versions: ${VERSIONS[*]}"
echo ""

overall_ok=1
declare -a report_rows=()

for version in "${VERSIONS[@]}"; do
  partition_file="$TMP_DIR/Partition-$version.scala"
  psm_file="$TMP_DIR/PartitionStateMachine-$version.scala"

  echo "── Kafka $version ──"
  download_ok=1
  if ! fetch "$version" "$PARTITION_PATH" "$partition_file"; then
    echo "  ERROR: failed to download Partition.scala (tag $version missing on GitHub?)"
    download_ok=0
  fi
  if ! fetch "$version" "$PSM_PATH" "$psm_file"; then
    echo "  ERROR: failed to download PartitionStateMachine.scala"
    echo "         (KRaft-only trees >= 4.0 removed this ZK controller file — expected)"
    download_ok=0
  fi
  if [ "$download_ok" -eq 0 ]; then
    overall_ok=0
    report_rows+=("$version|DL-FAIL|DL-FAIL|DL-FAIL|DL-FAIL|DL-FAIL")
    echo ""
    continue
  fi

  row="$version"
  for key in "${ANCHOR_KEYS[@]}"; do
    target_file="$partition_file"
    [ "$(anchor_file "$key")" = "$PSM_PATH" ] && target_file="$psm_file"

    # -F: fixed string, -x: whole-line match (anchors are full source lines)
    count=$(grep -Fxc -- "$(anchor_text "$key")" "$target_file" || true)
    if [ "$count" -eq 1 ]; then
      status="OK"
    else
      status="x$count"
      overall_ok=0
      echo "  FAIL $key ($(anchor_desc "$key")): found $count occurrence(s), expected exactly 1"
    fi
    row="$row|$status"
  done
  report_rows+=("$row")
  echo "  done"
  echo ""
done

echo "═══ anchor matrix (expect OK everywhere; xN = found N times; DL-FAIL = download failed) ═══"
printf '%-10s %-6s %-6s %-6s %-6s %-6s\n' "version" "A1" "A2" "A3" "A4" "A5"
for row in "${report_rows[@]}"; do
  IFS='|' read -r v a1 a2 a3 a4 a5 <<< "$row"
  printf '%-10s %-6s %-6s %-6s %-6s %-6s\n' "$v" "$a1" "$a2" "$a3" "$a4" "$a5"
done
echo ""
echo "Legend: A1 promotion gate / A2 HW gate / A3 demotion hook / A4 initial ISR / A5 unclean election"

if [ "$overall_ok" -eq 1 ]; then
  echo "RESULT: all anchors intact — the canonical patch is expected to apply cleanly."
  exit 0
else
  echo "RESULT: anchor drift detected — a version-specific patch is needed for the failing version(s)."
  echo "        See docs/multi-version.md for the per-version hook matrix."
  exit 1
fi
