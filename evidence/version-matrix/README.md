# Version-matrix evidence — raw S1–S8 output per Kafka version

Raw, unedited command output from the full-matrix real-machine run (Tokyo `ap-northeast-1`,
Graviton `m7g`). Each file is one Kafka version taken through the complete S1–S8 failure suite.
Summary and analysis: [`docs/version-matrix.md`](../../docs/version-matrix.md).

## Files

| File | Kafka | Mode |
|---|---|---|
| `zk-3.0.2.txt` … `zk-3.9.2.txt` | 3.0.2 / 3.1.2 / 3.2.3 / 3.4.1 / 3.5.2 / 3.6.2 / 3.7.2 / 3.8.1 / 3.9.2 | ZooKeeper |
| `kraft-3.7.2.txt` … `kraft-4.3.1.txt` | 3.7.2 / 3.8.1 / 3.9.2 / 4.0.2 / 4.1.2 / 4.2.1 / 4.3.1 | KRaft |
| `timing-data.txt` | all | extracted timing summary |

The earliest supported versions (2.7.2 / 2.8.1 / 2.8.2 / 3.3.2, ZooKeeper) have their raw output
in the sibling directory [`../old-versions-real-machine/`](../old-versions-real-machine/).

## Each file contains, inline and timestamped

- Cluster bring-up: cluster.id, broker/controller registration, `ObserverIds`/`ObserverReplicas`
  class presence confirmed in the deployed jars, core-jar md5.
- Initial steady state proving the observer (broker 3) is **excluded from ISR**
  (`Replicas: 3,1,2,4` but `Isr: 1,2,4`).
- S1–S8, each with the injection, the observed ISR/leader transitions, `acks=all` write results,
  and — for S3/S4 — per-segment **md5 proving the observer's log is byte-identical to the leader's**.

## How these were validated

Beyond "the script ran," each file was checked for the actual assertions:
initial ISR excludes the observer, S1's new leader is a primary (never the observer),
S3's leader/observer segment md5s match, S4 promotes the observer to leader, S5 admits the
lagging observer to ISR only after catch-up, S6/S7 preconditions are clean (observer not stuck
as leader), and S8 completes with the observer still excluded. All 20 builds passed all checks.

> **KRaft S6–S8 note:** the first KRaft pass left the observer stuck as leader after the S4/S5
> promotion cycle (KRaft won't hot-demote a leader observer — leadership must be moved first),
> which contaminated the S6–S8 preconditions. The scenario harness was fixed to force leadership
> off the observer during recovery (`ensure_observer_demoted`), and **all seven KRaft versions
> were re-run** — the files here are the corrected runs, with S6/S7/S8 preconditions confirmed
> observer-free. This limitation is real and documented; the fix is operational (move leadership
> before demoting), not a patch change.
