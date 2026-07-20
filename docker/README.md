# Local verification environment (Docker)

Spin up a 3-broker Kafka 3.7.1 cluster (ZooKeeper mode) with the observer
patch applied, entirely on your machine, and walk through the full observer
lifecycle: **sync-but-never-ISR → file-driven promotion → file-driven demotion**
— all with zero broker restarts.

Everything is built from upstream Apache Kafka source at image build time;
the canonical [`observer.patch`](../patches/kafka-3.7.1-zk/observer.patch) is
applied inside the Docker build. No pre-built jars are distributed.

## Topology

| Container | Broker id | Host port | Role at startup |
|---|---|---|---|
| `zookeeper` | — | — | ZooKeeper 3.8 (official image) |
| `kafka1` | 1 | `localhost:19092` | **Observer** (listed in `observer.ids`) |
| `kafka2` | 2 | `localhost:19093` | Normal replica |
| `kafka3` | 3 | `localhost:19094` | Normal replica |

All three brokers share one image (built once by the `kafka1` service) and all
bind-mount the same host file `./observer.ids` to `/opt/kafka/observer.ids`.
The patch re-reads that file every 5 seconds — **editing the file on your host
IS the promote/demote operation**.

## Prerequisites

- Docker with Compose v2, and **≥ 6 GB RAM for the Docker VM** (the Gradle
  build stage compiles Scala with a 2 GB JVM heap).
- ~4 GB free disk for the build stage (Kafka source + Gradle caches).
- Network access to `github.com` and `archive.apache.org`.

## Quick start

```bash
cd docker

# 1. Build + start. FIRST BUILD TAKES ~10-20 MINUTES (Gradle compiles patched
#    Kafka from source). Subsequent starts reuse the cached image in seconds.
docker compose up -d --build

# 2. Wait until all 4 containers report healthy
docker compose ps

# 3. Create a topic with replicas on all 3 brokers
docker compose exec kafka2 kafka-topics.sh --bootstrap-server kafka2:9092 \
  --create --topic demo --partitions 1 --replication-factor 3

# 3b. ⚠️ ZK-MODE CAVEAT — REQUIRED for NEW topics: the controller sends
#     LeaderAndIsr only to ISR members at creation, so the observer does not
#     start fetching a brand-new topic until its next restart (or a controller
#     failover). Existing topics are unaffected. One restart makes broker 1
#     fetch 'demo' (KRaft mode does not have this limitation):
docker compose restart kafka1

# 4. Verify: broker 1 is in Replicas but NOT in Isr — that is the observer
docker compose exec kafka2 kafka-topics.sh --bootstrap-server kafka2:9092 \
  --describe --topic demo
#   ... Replicas: <includes 1>   Isr: <excludes 1>

# 5. PROMOTE broker 1: empty the file on the HOST, wait <=10 s, describe again
echo '' > observer.ids
sleep 10
docker compose exec kafka2 kafka-topics.sh --bootstrap-server kafka2:9092 \
  --describe --topic demo
#   ... Isr now includes 1 — fully electable, zero restart, zero data movement

# 6. DEMOTE broker 1: put its id back, native isr-expiration shrinks it out
echo '1' > observer.ids
sleep 15
docker compose exec kafka2 kafka-topics.sh --bootstrap-server kafka2:9092 \
  --describe --topic demo
#   ... Isr excludes 1 again (it keeps syncing all data)
```

Or run the whole lifecycle unattended:

```bash
./demo.sh          # cluster already up
./demo.sh --up     # builds + starts the cluster first
```

`demo.sh` creates a topic, verifies ISR exclusion, produces 1000 records with
`acks=all`, promotes, demotes, and cleans up after itself.

## Command cheat sheet

```bash
docker compose ps                              # health of all containers
docker compose logs -f kafka1                  # observer broker logs
                                               #   (grep "Observer id set changed" to see file reloads)
cat observer.ids                               # current observer set
echo '' > observer.ids                         # promote broker 1 (empty = no observers)
echo '1' > observer.ids                        # demote broker 1

# Produce / consume from the HOST via published ports
kafka-console-producer.sh --bootstrap-server localhost:19093 --topic demo
kafka-console-consumer.sh --bootstrap-server localhost:19093 --topic demo --from-beginning

# Tear down (add -v to also delete broker data)
docker compose down -v
```

## ⚠️ ZK-mode caveat: observers discover NEW topics only after a restart

In ZooKeeper mode, the controller sends `LeaderAndIsr` for a **new** topic
only to its **ISR members** — and the patch keeps observers out of the initial
ISR. So an observer does **not** start replicating a newly created topic until
its **next restart or a controller failover** (verified on this local cluster:
the partition directory does not appear on the observer until it restarts).

Consequences for this environment:

- After creating a new topic, run `docker compose restart kafka1` once before
  expecting the observer to sync it (`demo.sh` does this automatically).
- Promotion of an observer that never fetched a topic cannot put it in that
  topic's ISR — it has nothing to catch up from.
- Topics that existed before the observer's last (re)start are unaffected;
  promote/demote on them is fully zero-restart.

KRaft mode does not have this limitation — brokers read assignments
from the shared metadata log. See
[docs/multi-version.md](../docs/multi-version.md) and
[docs/architecture.md](../docs/architecture.md#known-behavior-notes).

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Build fails in the Gradle step with OOM / exit 137 | Increase Docker VM memory to ≥ 6 GB |
| Build is slow | Expected: 10–20 min on first build. Nothing is cached yet. |
| Broker 1 appears in ISR right after `up` | Check `cat observer.ids` on the host — the compose bind-mount must contain `1` |
| Promotion takes longer than 10 s | File cache is 5 s + one fetch round-trip; up to ~15 s is normal here |
| `docker compose exec` says container not running | `docker compose logs kafka1` — most often ZooKeeper wasn't healthy yet; compose restarts brokers automatically |

## Scope

This environment is for **local verification and demos only**. It runs
PLAINTEXT listeners, one partition per internal topic default, and small
heaps. For real deployments (rolling jar replacement, multi-AZ layout,
promotion SOPs) see [docs/deployment.md](../docs/deployment.md) and
[docs/runbooks/](../docs/runbooks/).
