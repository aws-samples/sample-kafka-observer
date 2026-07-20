# Auto-Promotion Policy (`under-min-isr`) — Design

> **Status: shipped OFF by default.** For financial workloads we recommend
> staying with the manual runbooks (`scripts/observer-promote.sh` /
> `observer-demote.sh`). This document explains the optional automation, why
> it lives *outside* the Kafka kernel, and the SOP + risk boundary for
> enabling it.

## 1. What it does

`scripts/observer-auto-promoter.sh` is an external watchdog implementing the
semantics of Confluent's `observerPromotionPolicy=under-min-isr`:

| Event | Action |
|---|---|
| A partition's ISR size drops below `min.insync.replicas` **and** a caught-up observer replica exists (`offsetLag <= threshold`, default 0) | Promote the observer: atomically remove its id from `observer.ids` on every broker → native `maybeExpandIsr` pulls it into ISR (≤10 s) |
| The original followers recover: for every partition the auto-promoted broker serves, `ISR − {broker} ≥ min.insync.replicas` (verified twice, 5 s apart) | Demote it back: preferred leader election first if it became leader, then add the id back to `observer.ids` → native isr-expiration shrinks it out (≤10–20 s) |

Everything else — catch-up replication, ISR membership changes, HW
advancement — is native Kafka machinery; the daemon only edits the same file
the manual runbooks edit.

## 2. Why an external tool, not a kernel change

The broker/controller patch stays minimal (one hook in `Partition.scala`, one
in RCM). The promotion *policy* is deliberately kept out of the data plane:

| Property | External watchdog | In-kernel policy |
|---|---|---|
| Auditability | Append-only log of every decision (`/var/log/observer-promoter.log`), reviewable by compliance | Scattered broker log lines |
| Kill switch | `systemctl stop` / `kill` — brokers unaffected | Requires config change + behavior verification, possibly restart |
| Dry-run | `-n` flag: full decision trace, zero cluster mutation | Not possible |
| Blast radius of a bug | Worst case: a wrong promote/demote, same as a mistaken manual run; brokers keep serving | Worst case: data-plane correctness bug on the ISR path |
| Kafka upgrades | Independent — script only uses public CLI tools (`kafka-topics.sh`, `kafka-configs.sh`, `kafka-log-dirs.sh`, `kafka-leader-election.sh`) | Patch must be re-ported and re-validated per version |
| Policy iteration | Edit a shell script | Rebuild + rolling restart |

This mirrors the project's design principles: minimal intrusion, reuse of
native mechanisms, everything auditable.

## 3. Semantic mapping to Confluent

| Confluent (`confluent.placement.constraints` + automatic observer promotion) | This project |
|---|---|
| `observerPromotionPolicy: none` | Daemon not running (default) |
| `observerPromotionPolicy: under-min-isr` | Daemon running with `-e` |
| `observerPromotionPolicy: under-replicated` | **Not implemented** (deliberate: promoting on any under-replication is too aggressive for the target use case) |
| `observerPromotionPolicy: leader-is-observer` | Not applicable — our observers are never leaders by construction (RCM hook excludes them from election) |
| Automatic demotion when constraint is satisfiable again | Phase-2 recovery detection with double-check + preferred election |
| Per-topic policy via placement JSON | `-t topic1,topic2` allowlist (cluster-wide default: all topics) |
| Caught-up definition: replica in ISR or within lag threshold | `kafka-log-dirs.sh` `offsetLag ≤ -l` threshold (default 0 = byte-identical) |

Key behavioral difference: Confluent promotion happens in the controller
within seconds of the ISR change; ours happens at the next scan (default 10 s
interval) plus the ≤10 s native expand path. Bound the total by tuning `-i`.

## 4. Safety design

- **Explicit enable interlock** — without `-e` the script prints the OFF
  status and exits 0. The systemd unit is a template; nothing installs or
  enables it automatically.
- **Dry-run mode** (`-n`) — full detection + decision logging, zero mutations.
  Mandatory first step of the enable SOP.
- **Caught-up gate** — an observer is promoted only when its `offsetLag` is at
  or below threshold (default 0). Promoting a laggy observer would stall the
  high-watermark; the daemon logs the skip reason instead.
- **Anti-flapping cooldown** (`-c`, default 300 s) — a broker that was just
  promoted/demoted (or whose action failed) is untouchable for the cooldown
  window. Prevents oscillation during flappy networks. At most **one action
  per scan** cluster-wide.
- **Demotion double-check** — recovery must hold across two `describe` calls
  5 s apart before demoting, and the demotion itself re-runs the hard
  pre-checks inside `observer-demote.sh` (never demote a leader; never drop a
  partition below minISR).
- **Leader safety** — if the auto-promoted broker became a leader, the daemon
  runs a preferred leader election and re-verifies before demoting; if
  leadership doesn't move, demotion is deferred and logged.
- **Scoped ownership** — the daemon only auto-demotes brokers **it** promoted
  (persisted in `/var/lib/observer-promoter/auto-promoted.list`, surviving
  restarts). Manually promoted brokers are never touched.
- **Audit-or-die** — if the audit log is not writable, the daemon refuses to
  start.

## 5. Audit log format

Append-only, one decision per line, `tee`'d to stdout (journald) and the file:

```
2026-07-20T14:02:11+0900 | START | policy=under-min-isr enabled=1 dry_run=0 interval=10s ...
2026-07-20T14:03:31+0900 | DETECT | under-min-isr | topic=orders partition=3 leader=1 replicas=1,2,4 isr=1 (size=1 < minISR=2)
2026-07-20T14:03:32+0900 | PROMOTE-BEGIN | broker=4 | topic=orders partition=3 isr=1 minISR=2 observerLag=0
2026-07-20T14:03:41+0900 | PROMOTE-OK | broker=4 | now a full ISR/election candidate
2026-07-20T14:12:52+0900 | DEMOTE-BEGIN | broker=4 | original followers recovered; ISR-{4} >= minISR on all partitions
2026-07-20T14:13:09+0900 | DEMOTE-OK | broker=4 | back to observer status
```

Full output of the underlying promote/demote scripts is appended to the same
file, so every ISR transition can be reconstructed post-hoc.

## 6. Enable SOP

1. **Read this document end to end**, including §7.
2. Dry-run against production topology for at least one business day:
   ```bash
   scripts/observer-auto-promoter.sh -e -n \
     -s broker1:9092 -H "broker1 broker2 broker3 broker4" \
     -L /tmp/observer-promoter-dryrun.log
   ```
   Inject a follower failure in a test window and confirm the log shows the
   expected `PROMOTE-DRYRUN` / `DEMOTE-DRYRUN` pair with correct reasoning.
3. Edit `deploy/observer-auto-promoter.service` (bootstrap, hosts, user),
   install it, and `systemctl start` **without** `enable`.
4. Observe at least one real or drilled failure cycle in the audit log.
5. Only then `systemctl enable`. Record the enablement decision (who/when/why)
   in your change-management system.
6. To disable at any time: `systemctl stop` (immediate at next scan boundary).

## 7. Risk statement — the boundary of automated decisions

Automation trades human judgment for reaction time. Know what you are buying:

- **A promoted observer changes your failure domain.** While promoted, the
  observer counts toward `acks=all` and is electable. If your observers live
  in a distant AZ/region, produce latency rises and a subsequent leader
  election may move leadership across the WAN. The daemon will demote on
  recovery, but for the duration of the incident your durability/latency
  trade-off is different from steady state.
- **The daemon reacts to symptoms, not causes.** ISR < minISR during a rolling
  restart, a network partition, or a misconfiguration all look identical to
  it. The topic allowlist (`-t`), cooldown, and dry-run exist to narrow the
  blast radius — use them. Pause the daemon during planned maintenance.
- **Split decisions are possible under partial visibility.** If the daemon's
  view of the cluster is degraded (e.g., it can reach the bootstrap but not
  all brokers' files), a promote can partially apply. The underlying scripts
  update files atomically per broker and the brokers re-read with a 5 s cache,
  so the system converges, but the audit log — not the daemon's intent — is
  the source of truth for what happened.
- **This is one daemon, not a consensus system.** Run exactly one instance.
  Running two instances (or one instance plus a concurrent manual operation)
  can interleave decisions; the cooldown and single-action-per-scan design
  reduce but do not eliminate this. The manual runbooks remain authoritative:
  if an operator is acting, stop the daemon first.
- **Financial-workload recommendation.** If your compliance posture requires a
  human in the loop for any change to replication guarantees, do not enable
  this. Use the daemon in dry-run mode (`-n`) purely as a *detector* that
  pages an operator, and keep promotion manual.

## 8. Files

| File | Purpose |
|---|---|
| `scripts/observer-auto-promoter.sh` | The watchdog daemon (default OFF, `-e` interlock) |
| `deploy/observer-auto-promoter.service` | systemd unit template (shipped, never auto-installed) |
| `scripts/observer-promote.sh` / `observer-demote.sh` | The primitives the daemon drives — identical to manual operation |
| `/var/log/observer-promoter.log` | Append-only audit trail |
| `/var/lib/observer-promoter/auto-promoted.list` | Persistent set of brokers this daemon promoted (ownership scope) |
