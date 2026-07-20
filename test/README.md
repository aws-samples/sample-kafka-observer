# Integration tests

End-to-end tests for the observer-replica patch, run against a **live cluster**
built from `patches/kafka-3.7.1-zk/observer.patch`. Nothing runs Kafka on the
test machine — every Kafka CLI invocation is executed *on a broker host*
(via `docker compose exec` or ssh), so locally you only need Python 3.10+,
[uv](https://docs.astral.sh/uv/), and docker **or** ssh access.

## What is covered

| File | Assertions |
|---|---|
| `test_observer_lifecycle.py` | (a) new topic's initial ISR excludes the observer, (b) observer fully syncs (log sizes converge), (c) ISR stays clean under traffic, (d) promotion: clear `observer.ids` → in ISR within **30 s**, (e) demotion: write id back → out of ISR within **45 s**, (f) promoted observer is elected leader and serves writes after all other brokers die (`destructive`) |
| `test_eos.py` | per-batch `(baseOffset, crc)` from `kafka-dump-log.sh` identical on leader vs observer (byte-identical replication ⇒ EOS preserved); transactional commit/abort markers byte-copied + `read_committed` view correct (needs `javac` on a broker, otherwise skipped) |

Thresholds: promotion ≤ 30 s, demotion ≤ 45 s. These are **CI-relaxed**
ceilings — real clusters measure ≤ 10 s for both (see `evidence/` and
`scripts/observer-{promote,demote}.sh`). Tighten with `--promote-timeout` /
`--demote-timeout` when running against real hardware.

## Backend 1: docker (default)

Expects a compose file with one service per broker (default
`../docker/docker-compose.yml`, service names `kafka-1`, `kafka-2`, `kafka-3`),
each container running the patched Kafka with:

- Kafka CLI at `/opt/kafka/bin`, data at `/data/kafka`
- observer ids file at `/opt/kafka/observer.ids` (broker 1 is the observer by default)
- inter-container listener on `<service-name>:9092`

```bash
cd test
uv sync
uv run pytest --backend docker                       # non-destructive suite
uv run pytest --backend docker --destructive        # + leader-election test
```

Override the layout without editing code:

```bash
uv run pytest --backend docker \
  --compose-file ../docker/docker-compose.yml \
  --container-pattern 'kafka-{id}' \
  --observer-id 1
# env overrides: OBSERVER_TEST_BROKER_IDS=1,2,3
#                OBSERVER_TEST_KAFKA_BIN=/opt/kafka/bin
#                OBSERVER_TEST_LOG_DIR=/data/kafka
#                OBSERVER_TEST_IDS_FILE=/opt/kafka/observer.ids
```

## Backend 2: aws (terraform + ssh)

Reads the topology from `terraform output -json cluster_json` in `../terraform`
(override with `--terraform-dir`). The terraform module must expose:

```hcl
output "cluster_json" {
  value = jsonencode({
    bootstrap = "10.0.1.10:9092,10.0.2.10:9092,10.0.3.10:9092"
    ssh_user  = "ec2-user"
    ssh_key   = "~/.ssh/kafka-poc.pem"       # optional; ssh-agent works too
    kafka_bin = "/opt/kafka/bin"              # optional, defaults shown
    log_dir   = "/data/kafka"                 # optional
    observer_ids_file = "/opt/kafka/observer.ids"  # optional
    brokers = [
      { id = 1, host = "10.0.1.10", observer = true },
      { id = 2, host = "10.0.2.10" },
      { id = 3, host = "10.0.3.10" },
    ]
  })
}
```

Requirements on each broker host: passwordless `sudo` for the ssh user
(file writes and `systemctl {start,stop,restart} kafka`), and Kafka managed
as the `kafka` systemd unit.

```bash
cd test
uv sync
uv run pytest --backend aws --terraform-dir ../terraform \
  --promote-timeout 15 --demote-timeout 25          # tight real-machine SLOs
uv run pytest --backend aws --destructive           # stops real brokers!
```

## Markers and safety

- `destructive` — stops/kills brokers; **skipped unless `--destructive`** is
  passed. Never point it at a cluster you are not willing to disrupt. All
  destructive tests restore the brokers and the `observer.ids` baseline in a
  finalizer, but a hard test-runner crash can still leave brokers down.
- `docker` / `aws` — restrict a test to one backend (currently none of the
  tests need this; the markers exist for future backend-specific cases).

The suite mutates cluster state: it creates/deletes `observer_*` and `eos_*`
topics and rewrites `/opt/kafka/observer.ids` on every broker (always restoring
"observer id only" on teardown). Do not run it against a production cluster.

## Known ZK-mode caveat (deliberately encoded in the fixtures)

In ZooKeeper mode the controller sends `LeaderAndIsr` only to ISR members at
topic creation, so **an observer does not discover a new topic until its next
restart or a controller failover** (existing topics are unaffected; KRaft does
not behave this way). Every topic-creating fixture in `conftest.py` therefore
restarts the observer broker once right after `--create`. This is an inherent
ZK-mode behavior, not a patch bug — see `docs/architecture.md` (Known
limitations) and `evidence/observer_v3_lifecycle_evidence.md`.

## Transactional test prerequisites

`kafka-console-producer.sh` cannot drive the transaction protocol (no CLI path
to `beginTransaction`/`commitTransaction`/`abortTransaction`), so
`test_eos.py::test_transaction_markers_replicated_to_observer` compiles a tiny
Java producer on a broker host against the broker's own `libs/`. If `javac`
is absent there (JRE-only images), the test **skips** with an explanatory
message — install a JDK in the broker image/AMI to enable it.
