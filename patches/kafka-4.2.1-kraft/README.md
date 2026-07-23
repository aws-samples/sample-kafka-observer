# observer.patch — Kafka 4.2.1 (kraft mode)

Apply to a clean Apache Kafka 4.2.1 checkout, then build. **Real-machine verified: full S1–S8 scenario suite passed** (see [`docs/version-matrix.md`](../../docs/version-matrix.md)).

```bash
git clone --depth 1 --branch 4.2.1 https://github.com/apache/kafka.git build && cd build
git apply /path/to/patches/kafka-4.2.1-kraft/observer.patch
./gradlew :core:jar :tools:jar :metadata:jar -x test   # JDK 17
```

Then create `/opt/kafka/observer.ids` with the observer broker id(s), deploy the patched
jar(s) (core + metadata) to brokers **and** controller nodes, and rolling-restart. Promote/demote = edit that file. Full guide:
[`docs/deployment.md`](../../docs/deployment.md).
