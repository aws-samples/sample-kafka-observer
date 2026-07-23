# observer.patch — Kafka 3.7.2 (kraft mode)

Apply to a clean Apache Kafka 3.7.2 checkout, then build. **Real-machine verified: full S1–S8 scenario suite passed** (see [`docs/version-matrix.md`](../../docs/version-matrix.md)).

```bash
git clone --depth 1 --branch 3.7.2 https://github.com/apache/kafka.git build && cd build
git apply /path/to/patches/kafka-3.7.2-kraft/observer.patch
./gradlew :core:jar :tools:jar :metadata:jar -x test   # JDK 11
```

Then create `/opt/kafka/observer.ids` with the observer broker id(s), deploy the patched
jar(s) (core + metadata) to brokers **and** controller nodes, and rolling-restart. Promote/demote = edit that file. Full guide:
[`docs/deployment.md`](../../docs/deployment.md).
