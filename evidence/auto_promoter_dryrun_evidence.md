# observer-auto-promoter.sh — dry-run verification evidence

- Date: 2026-07-20 (local macOS; mocked Kafka CLIs — real-cluster validation on Tokyo POC still pending, see "Remaining validation")
- Script under test: `scripts/observer-auto-promoter.sh` (v0.7, under-min-isr policy, default OFF)
- Method: mock `kafka-topics.sh` / `kafka-configs.sh` / `kafka-log-dirs.sh` / `ssh` in `/tmp/apromo-mock/bin`, drive the daemon with `-e -n -1` (enabled, dry-run, single scan).

## Static checks

```
$ bash -n scripts/observer-auto-promoter.sh   # OK
$ shellcheck scripts/observer-auto-promoter.sh
ALL CLEAN                                     # zero findings
```

## Interlock checks

```
$ ./scripts/observer-auto-promoter.sh          # no -e
observer auto-promotion policy: OFF (default).
...exit 0                                      # default-off interlock works

$ ./scripts/observer-auto-promoter.sh -e       # -e but missing args
line 69: BOOT: -s bootstrap required           # exit 1 — required-arg guard works
```

## Test A — under-min-isr detection → promote decision

Mock state: `orders-0` has `Replicas: 1,2,4  Isr: 1` (size 1 < minISR 2); broker 4 is in `observer.ids`; `kafka-log-dirs` reports `offsetLag: 0` for broker 4.

```
DETECT | under-min-isr | topic=orders partition=0 leader=1 replicas=1,2,4 isr=1 (size=1 < minISR=2)
PROMOTE-DRYRUN | broker=4 | topic=orders partition=0 isr=1 minISR=2 observerLag=0 | no action taken
```

## Test B — recovery detection → demote decision

Mock state: ISR restored to `1,2,4` on all partitions; `auto-promoted.list` contains broker 4. Double-check (two describes 5 s apart) passed.

```
DEMOTE-DRYRUN | broker=4 | original followers recovered; ISR-{4} >= minISR on all partitions | no action taken
```

## Test C — laggy observer skip (HW-stall protection)

Mock state: same under-min-isr as Test A but `offsetLag: 523`.

```
DETECT | under-min-isr | topic=orders partition=0 leader=1 replicas=1,2,4 isr=1 (size=1 < minISR=2)
SKIP | broker=4 not caught up (offsetLag=523 > 0) — promoting a laggy observer would stall the HW
```

No promote issued — correct.

## Parser unit checks

`awk` describe-parser handles: normal ISR, single-member ISR, and **empty ISR followed by `Elr:` token** (KRaft 4.x output) — empty ISR is normalized to `-` and never mistaken for a broker id. `in_csv` has no substring false-positives (`2` not matched inside `12,24`).

## Remaining validation (before relying on it)

- Real-cluster run on Tokyo POC (3.7.1 combined): non-dry-run promote/demote cycle with an actual follower kill, confirming end-to-end audit trail and native expand/shrink timings.
- `-t` allowlist behavior against multiple topics on a live cluster.
