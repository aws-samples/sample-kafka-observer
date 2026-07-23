# observer.patch — Kafka 3.2.3 (zk mode)

Apply to a clean Apache Kafka 3.2.3 checkout, then build. **Real-machine verified: full S1–S8 scenario suite passed** (see [`docs/version-matrix.md`](../../docs/version-matrix.md)).

```bash
git clone --depth 1 --branch 3.2.3 https://github.com/apache/kafka.git build && cd build
git apply /path/to/patches/kafka-3.2.3-zk/observer.patch
./gradlew :core:jar :tools:jar -x test   # JDK 11
```

Then create `/opt/kafka/observer.ids` with the observer broker id(s), deploy the patched
jar(s), and rolling-restart. Promote/demote = edit that file. Full guide:
[`docs/deployment.md`](../../docs/deployment.md).
