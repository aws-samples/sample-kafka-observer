# Deployment guide

Three ways to get a patched Kafka, from most to least hands-on. All paths end with the same runtime artifacts: a patched `kafka_2.13-<version>.jar` (+ `kafka-storage-<version>.jar`) and an `observer.ids` file on every broker.

## Path 1 — build from source (recommended, fully auditable)

```bash
./tools/apply-and-build.sh 3.7.1
# → /tmp/kafka-src-build/core/build/libs/kafka_2.13-3.7.1.jar
# → /tmp/kafka-src-build/storage/build/libs/kafka-storage-3.7.1.jar
```

Prerequisites: JDK 17 **with javac** (`java-17-amazon-corretto-devel`, not `-headless` — the Scala compiler needs javac and this is the #1 first-build failure), git, ~2 GB disk. Measured build time: ~1 min on 4 vCPUs.

## Path 2 — Docker (local evaluation)

```bash
cd docker && docker compose up -d     # first build ≈ 10–20 min (compiles Kafka from source)
./demo.sh                              # scripted promote/demote walk-through
```

See [`docker/README.md`](../docker/README.md).

## Path 3 — manual patch application

```bash
git clone --depth 1 --branch 3.7.1 https://github.com/apache/kafka.git
cd kafka && git apply --3way ../patches/kafka-3.7.1-zk/observer.patch
grep -rc "OBSERVER PATCH" core/src/main/scala/     # expect >= 6 markers
./gradlew :core:jar :storage:jar -x test
```

## Rolling deployment to a live cluster

Tested procedure (this is exactly how the reference cluster was upgraded):

```bash
# 0. Back up original jars on every broker
sudo cp /opt/kafka/libs/kafka_2.13-3.7.1.jar{,.orig}
sudo cp /opt/kafka/libs/kafka-storage-3.7.1.jar{,.orig}

# 1. Copy patched jars to every broker (brokers AND — in KRaft mode — controller nodes)
sudo cp kafka_2.13-3.7.1.jar /opt/kafka/libs/
sudo cp kafka-storage-3.7.1.jar /opt/kafka/libs/

# 2. Create the observer list on EVERY broker (identical content, atomic write)
echo "1" | sudo tee /opt/kafka/observer.ids.tmp >/dev/null
sudo mv /opt/kafka/observer.ids.tmp /opt/kafka/observer.ids

# 3. Rolling restart, one broker at a time; wait for URP=0 between brokers
sudo systemctl restart kafka
```

Notes:

- **The patch is inert without the file**: brokers running the patched jar with an empty/absent `observer.ids` behave exactly like stock Kafka (env-var fallback also empty → no observers). You can therefore roll the jar out first and enable observers later.
- **File must be identical everywhere** — the leader uses it for expand/shrink/HW, the (ZK-mode) controller broker uses it for initial ISR and unclean election. Push with one script; verify with checksums.
- **Rollback** = restore `.orig` jars + rolling restart. The observer.ids file is harmless to stock Kafka.

## ⚠️ Known limitation — ZK mode, new topics

In ZooKeeper mode, the controller sends `LeaderAndIsr` only to ISR members at **topic creation**. An observer (excluded from the initial ISR by design) therefore never receives a *new* topic's assignment — **even while running**: the partition directory does not appear on disk, no fetching happens, and a promotion attempted in this state would fail. The observer learns the assignment only on its next restart or a controller failover. (Docker-demo verified.)

- Existing topics: unaffected (assignments load from ZK at startup).
- Workaround for new topics: restart the observer broker once after creating topics, or create topics before designating the broker as observer.
- **KRaft mode does not have this limitation** (brokers learn all assignments from the metadata log — probe-verified, see `evidence/kraft_probe_evidence.md`).

## Verifying the deployment

```bash
# a) Patched jar active? Check for the marker class:
unzip -l /opt/kafka/libs/kafka_2.13-3.7.1.jar | grep ObserverIds     # kafka/observer/ObserverIds*.class

# b) Observer semantics live? Create a test topic spanning the observer:
kafka-topics.sh --bootstrap-server $BS --create --topic obs_check --replica-assignment 2:3:1
kafka-topics.sh --bootstrap-server $BS --describe --topic obs_check
#    Expect: Replicas: 2,3,1   Isr: 2,3      (observer id absent)

# c) Full verification suite: see test/README.md
```
