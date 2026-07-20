# Evidence: Multi-Version Patch Compatibility (apply + compile, real hardware)

**Date:** 2026-07-20 (JST)
**Purpose:** Upgrade the multi-version compatibility claim from *source-level anchor
comparison* ("the patched broker anchor code is byte-identical across 3.6–3.9") to
*empirical proof*: apply the canonical patch to each representative version and compile.

## Environment

| Item | Value |
|---|---|
| Host | loadgen EC2, ap-northeast-1 (Tokyo), 4 vCPU / 16 GB RAM |
| JDK | Corretto 17.0.19 (OpenJDK 17.0.19+10-LTS) |
| Git | 2.50.1 |
| Patch under test | `patches/kafka-3.7.1-zk/observer.patch` (canonical, 151-line diff, unmodified) |
| Source | `git clone --depth 1 --branch <tag> https://github.com/apache/kafka.git` |
| Compile command | `./gradlew :core:compileScala -x test --console=plain` |

Notes on method:

- `git apply --3way` on a `--depth 1` clone cannot find ancestor blobs, so git prints
  `repository lacks the necessary blob to perform 3-way merge` and **falls back to direct
  application**. This makes the test *stricter*, not weaker: direct application requires
  every hunk context to match the target tree exactly (default fuzz rules, no 3-way rescue).
- `:core:compileScala` compiles the entire `core` module (where all three patched/added
  files live) plus its upstream Java modules — sufficient to prove the patch compiles,
  much faster than `:core:jar`.
- Each source tree (~2 GB after build) was deleted before cloning the next version.

## Results

| Version | Tag / commit | `git apply` | `:core:compileScala` | Compile wall time |
|---|---|---|---|---|
| 3.6.2 | `3.6.2` | ✅ all 3 files applied cleanly, exit 0 | ✅ BUILD SUCCESSFUL (17 tasks) | 2m 08s |
| 3.7.1 | `3.7.1` | ✅ (patch origin version; running patched in the Tokyo POC cluster) | ✅ (full `:core:jar` built and deployed earlier in this POC) | n/a (baseline) |
| 3.8.1 | `3.8.1` = `70d6ff42debf` | ✅ all 3 files applied cleanly, exit 0 | ✅ BUILD SUCCESSFUL (20 tasks) | 1m 50s |
| 3.9.1 | `3.9.1` | ✅ all 3 files applied cleanly, exit 0 | ✅ BUILD SUCCESSFUL (20 tasks) | 2m 08s |

Per-version `git status --short` after apply (identical for 3.6.2 / 3.8.1 / 3.9.1):

```
M  core/src/main/scala/kafka/cluster/Partition.scala
M  core/src/main/scala/kafka/controller/PartitionStateMachine.scala
A  core/src/main/scala/kafka/observer/ObserverIds.scala
```

Representative build tail (same for all three versions, from `gradlew` output):

```
BUILD SUCCESSFUL in 2m 8s        # 3.6.2: 17 actionable tasks / 3.8.x, 3.9.x: 20 tasks
```

## Conclusions

1. **The single canonical patch (`kafka-3.7.1-zk/observer.patch`) applies cleanly and
   compiles on Kafka 3.6.2, 3.7.1, 3.8.1 and 3.9.1 (ZooKeeper mode) with zero
   modification.** No conflicts, no fuzz, no per-version patch needed across the entire
   3.6–3.9 line.
2. This empirically confirms the earlier source-comparison finding: the three anchor
   points (`canAddReplicaToIsr`, `shouldWaitForReplicaToJoinIsr` /
   `getOutOfSyncReplicas` in `Partition.scala`; initial-ISR construction and
   unclean-election candidate selection in `PartitionStateMachine.scala`) are stable
   across these releases, and `ObserverIds.scala` is a new self-contained file with no
   version-sensitive dependencies (`kafka.utils.Logging` exists unchanged in all four).
3. Scope limit (honest boundary): this proves **apply + compile**, not runtime behavior,
   for 3.6.2 / 3.8.1 / 3.9.1. Full runtime lifecycle evidence (ISR exclusion, HW
   independence, promotion/demotion, election exclusion) exists only for 3.7.1 — see
   `observer_v3_lifecycle_evidence.md`. Kafka 4.x has no ZooKeeper mode;
   `PartitionStateMachine.scala` does not exist there, so this patch does not apply to
   4.x by design (see `docs/multi-version` notes on the KRaft path).
4. ZK-mode caveat still applies on every version: **new topics created while an observer
   is listed still require the documented workaround** (controller places initial ISR;
   see runbooks) — this evidence does not change that.

## Cleanup

All `/tmp/mv-<ver>` source trees were deleted after each build (build logs
`/tmp/mv-<ver>-build.log` / `-time.log` retained on loadgen). The production POC tree
`/tmp/kafka-src` (patched 3.7.1) was verified untouched (5 `OBSERVER PATCH` markers
present in `Partition.scala`).
