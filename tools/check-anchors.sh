#!/usr/bin/env bash
# =============================================================================
# check-anchors.sh — verify the observer patch anchors exist verbatim in a
# given Kafka version, WITHOUT cloning the full source tree.
#
# This is the local/offline counterpart of the CI matrix in
# .github/workflows/build-verify.yml: it downloads only the files the
# canonical patches touch (via GitHub raw) and checks that each anchor line
# appears EXACTLY ONCE. If all applicable anchors are intact, the matching
# canonical patch is expected to apply cleanly with `git apply --3way`.
#
# Anchor groups (which apply depends on the Kafka version):
#   A1–A3  broker side, Partition.scala           — all versions (3.6.x–4.1.x)
#   A4–A5  ZK controller, PartitionStateMachine   — 3.x only; SKIPPED on 4.x
#          (Kafka 4.0 removed the ZooKeeper controller entirely — the file
#          does not exist, and the ZK hunks are dropped from the 4.x patches)
#   K1–K3  KRaft controller, ReplicationControlManager.java — 3.7+ only
#          (the KRaft patch targets 3.7.1+; K3's LeaderAcceptor structure
#          does not exist in 3.6.x)
#
# Usage:
#   ./tools/check-anchors.sh                      # default matrix: 3.6.2 3.7.1 3.8.1 3.9.1 4.0.0 4.1.0
#   ./tools/check-anchors.sh 3.6.2 4.0.0          # explicit versions
#
# Exit code: 0 if every applicable anchor is exactly-once in every version,
# 1 otherwise.
# =============================================================================
set -euo pipefail

DEFAULT_VERSIONS=(3.6.2 3.7.1 3.8.1 3.9.1 4.0.0 4.1.0)
VERSIONS=("$@")
if [ "${#VERSIONS[@]}" -eq 0 ]; then
  VERSIONS=("${DEFAULT_VERSIONS[@]}")
fi

RAW_BASE="https://raw.githubusercontent.com/apache/kafka"
PARTITION_PATH="core/src/main/scala/kafka/cluster/Partition.scala"
PSM_PATH="core/src/main/scala/kafka/controller/PartitionStateMachine.scala"
RCM_PATH="metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java"

# The anchors the patch hunks attach to. Each must appear exactly once.
# Keep these byte-identical to the context lines in the observer.patch files —
# if you edit a patch, update this table.
#   A1  Partition.canAddReplicaToIsr     — promotion gate insertion point
#   A2  Partition.maybeIncrementLeaderHW — HW-wait predicate insertion point
#   A3  Partition.getOutOfSyncReplicas   — demotion hook replaced line
#   A4  PartitionStateMachine initial ISR — replaced line (new-topic ISR, ZK)
#   A5  PartitionStateMachine unclean election — replaced line (ZK)
#   K1  RCM.buildPartitionRegistration   — initial-ISR filter replaced line
#   K2  RCM.ineligibleReplicasForIsr     — AlterPartition defense-in-depth
#   K3  RCM LeaderAcceptor.test          — election gate insertion point
ANCHOR_KEYS=(A1 A2 A3 A4 A5 K1 K2 K3)

anchor_file() {
  case "$1" in
    A1|A2|A3) echo "$PARTITION_PATH" ;;
    A4|A5)    echo "$PSM_PATH" ;;
    K1|K2|K3) echo "$RCM_PATH" ;;
  esac
}

anchor_text() {
  case "$1" in
    A1) echo '  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {' ;;
    A2) echo '      def shouldWaitForReplicaToJoinIsr: Boolean = {' ;;
    A3) echo '      candidateReplicaIds.filter(replicaId => isFollowerOutOfSync(replicaId, leaderEndOffset, currentTimeMs, maxLagMs))' ;;
    A4) echo '      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)' ;;
    A5) echo '        val leaderOpt = assignment.find(liveReplicas.contains)' ;;
    K1) echo '            setIsr(Replicas.toArray(isr)).' ;;
    K2) echo '                ineligibleReplicas.add(new IneligibleReplica(brokerId, "not registered"));' ;;
    K3) echo '            if (!isAcceptableLeader.test(brokerId)) {' ;;
  esac
}

anchor_desc() {
  case "$1" in
    A1) echo "canAddReplicaToIsr (promotion gate)" ;;
    A2) echo "shouldWaitForReplicaToJoinIsr (HW gate)" ;;
    A3) echo "getOutOfSyncReplicas (demotion hook)" ;;
    A4) echo "initial ISR at topic creation (ZK)" ;;
    A5) echo "unclean leader election (ZK)" ;;
    K1) echo "buildPartitionRegistration initial ISR (KRaft)" ;;
    K2) echo "ineligibleReplicasForIsr AlterPartition gate (KRaft)" ;;
    K3) echo "LeaderAcceptor election gate (KRaft)" ;;
  esac
}

# Whether an anchor applies to a given version.
#   ZK anchors (A4/A5): 3.x only — Kafka 4.0 deleted the ZK controller.
#   KRaft anchors (K1–K3): 3.7+ — the KRaft patch does not target 3.6.x.
anchor_applies() { # anchor_applies <key> <version>; echoes 1 or 0
  local key="$1" version="$2"
  case "$key" in
    A4|A5)
      case "$version" in
        4.*) echo 0 ;;
        *)   echo 1 ;;
      esac ;;
    K1|K2|K3)
      case "$version" in
        3.6.*) echo 0 ;;
        *)     echo 1 ;;
      esac ;;
    *) echo 1 ;;
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
  rcm_file="$TMP_DIR/RCM-$version.java"

  echo "── Kafka $version ──"
  download_ok=1
  if ! fetch "$version" "$PARTITION_PATH" "$partition_file"; then
    echo "  ERROR: failed to download Partition.scala (tag $version missing on GitHub?)"
    download_ok=0
  fi
  # ZK controller file exists only on 3.x trees.
  if [ "$(anchor_applies A4 "$version")" -eq 1 ]; then
    if ! fetch "$version" "$PSM_PATH" "$psm_file"; then
      echo "  ERROR: failed to download PartitionStateMachine.scala"
      download_ok=0
    fi
  fi
  # KRaft controller file needed only where K anchors apply.
  if [ "$(anchor_applies K1 "$version")" -eq 1 ]; then
    if ! fetch "$version" "$RCM_PATH" "$rcm_file"; then
      echo "  ERROR: failed to download ReplicationControlManager.java"
      download_ok=0
    fi
  fi
  if [ "$download_ok" -eq 0 ]; then
    overall_ok=0
    report_rows+=("$version|DL-FAIL|DL-FAIL|DL-FAIL|DL-FAIL|DL-FAIL|DL-FAIL|DL-FAIL|DL-FAIL")
    echo ""
    continue
  fi

  row="$version"
  for key in "${ANCHOR_KEYS[@]}"; do
    if [ "$(anchor_applies "$key" "$version")" -eq 0 ]; then
      row="$row|n/a"
      continue
    fi

    target_file="$partition_file"
    case "$(anchor_file "$key")" in
      "$PSM_PATH") target_file="$psm_file" ;;
      "$RCM_PATH") target_file="$rcm_file" ;;
    esac

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

echo "═══ anchor matrix (expect OK everywhere; xN = found N times; n/a = not applicable; DL-FAIL = download failed) ═══"
printf '%-10s %-6s %-6s %-6s %-6s %-6s %-6s %-6s %-6s\n' "version" "A1" "A2" "A3" "A4" "A5" "K1" "K2" "K3"
for row in "${report_rows[@]}"; do
  IFS='|' read -r v a1 a2 a3 a4 a5 k1 k2 k3 <<< "$row"
  printf '%-10s %-6s %-6s %-6s %-6s %-6s %-6s %-6s %-6s\n' "$v" "$a1" "$a2" "$a3" "$a4" "$a5" "$k1" "$k2" "$k3"
done
echo ""
echo "Legend: A1 promotion gate / A2 HW gate / A3 demotion hook / A4 initial ISR (ZK) / A5 unclean election (ZK)"
echo "        K1 initial ISR (KRaft) / K2 AlterPartition gate (KRaft) / K3 LeaderAcceptor gate (KRaft)"
echo "        A4/A5 are n/a on 4.x (ZK controller removed upstream); K1–K3 are n/a on 3.6.x"

if [ "$overall_ok" -eq 1 ]; then
  echo "RESULT: all applicable anchors intact — the canonical patches are expected to apply cleanly."
  exit 0
else
  echo "RESULT: anchor drift detected — a version-specific patch is needed for the failing version(s)."
  echo "        See docs/multi-version.md for the per-version hook matrix."
  exit 1
fi
