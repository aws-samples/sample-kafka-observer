# =============================================================================
# sample-kafka-observer — local verification image
#
# Multi-stage build:
#   Stage 1 (builder): clone Apache Kafka source at ${KAFKA_VERSION}, apply the
#     canonical observer patch, build the patched :core and :storage jars,
#     then download the official binary distribution and swap in the patched
#     jars. No pre-built jars are distributed — the .patch is the canonical
#     artifact and everything is built from upstream source at image build time.
#   Stage 2 (runtime): slim JRE image with the patched distribution only.
#
# Build context MUST be the repository root (so patches/ is visible):
#   docker build -f docker/Dockerfile -t kafka-observer:3.7.1 .
# or simply `docker compose up -d --build` from docker/ (compose sets context).
#
# First build takes ~10-20 minutes (Gradle downloads + Scala compilation).
# Give the Docker VM at least 6 GB RAM; the Gradle JVM alone is capped at 2 GB.
# =============================================================================

ARG KAFKA_VERSION=3.7.1
ARG SCALA_VERSION=2.13

# ---------------------------------------------------------------------------
# Stage 1: patch + build from source, assemble patched distribution
# ---------------------------------------------------------------------------
FROM eclipse-temurin:17-jdk AS builder
ARG KAFKA_VERSION
ARG SCALA_VERSION

# git: clone + `git apply --3way`; curl/ca-certs: Kafka's gradlew bootstraps
# the wrapper jar over HTTPS, and we fetch the official binary tgz.
RUN apt-get update \
 && apt-get install -y --no-install-recommends git curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Shallow clone of the exact release tag (~350 MB; dominated by network).
RUN git clone --depth 1 --branch "${KAFKA_VERSION}" \
      https://github.com/apache/kafka.git kafka-src

# The canonical distribution artifact of this project.
COPY patches/ /patches/

WORKDIR /build/kafka-src

# Apply patch, then verify the OBSERVER PATCH markers landed (same safety
# gate as tools/apply-and-build.sh).
RUN git apply --3way "/patches/kafka-${KAFKA_VERSION}-zk/observer.patch" \
 && MARKERS=$(grep -r "OBSERVER PATCH" core/src/main/scala/ | wc -l) \
 && if [ "$MARKERS" -lt 6 ]; then \
      echo "ERROR: expected >=6 OBSERVER PATCH markers, found $MARKERS"; exit 1; \
    fi \
 && echo "OK: $MARKERS OBSERVER PATCH markers found"

# Gradle needs real memory for the Scala compiler; no daemon inside Docker.
ENV GRADLE_OPTS="-Dorg.gradle.daemon=false"
RUN ./gradlew :core:jar :storage:jar -x test --no-daemon --console=plain \
      -Dorg.gradle.jvmargs="-Xmx2g -XX:MaxMetaspaceSize=512m"

# Official binary distribution, with only the two patched jars replaced.
RUN curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
      -o /tmp/kafka.tgz \
 && tar -xzf /tmp/kafka.tgz -C /opt \
 && mv "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka \
 && rm /tmp/kafka.tgz \
 && cp "core/build/libs/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.jar" /opt/kafka/libs/ \
 && cp "storage/build/libs/kafka-storage-${KAFKA_VERSION}.jar" /opt/kafka/libs/ \
 && ls -la "/opt/kafka/libs/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.jar" \
          "/opt/kafka/libs/kafka-storage-${KAFKA_VERSION}.jar"

# ---------------------------------------------------------------------------
# Stage 2: runtime (JRE only, patched distribution, config from env)
# ---------------------------------------------------------------------------
FROM eclipse-temurin:17-jre AS runtime
ARG KAFKA_VERSION

LABEL org.opencontainers.image.title="kafka-observer (patched Apache Kafka ${KAFKA_VERSION}, ZK mode)" \
      org.opencontainers.image.description="Kafka broker with observer-replica patch applied at build time from source. Local verification only — not a production image." \
      org.opencontainers.image.licenses="Apache-2.0"

ENV KAFKA_HOME=/opt/kafka \
    PATH="/opt/kafka/bin:${PATH}" \
    KAFKA_OBSERVER_IDS_FILE=/opt/kafka/observer.ids

COPY --from=builder /opt/kafka /opt/kafka
COPY docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh && mkdir -p /opt/kafka/data

# 9092 = INTERNAL (inter-broker / in-network clients); the EXTERNAL listener
# port is per-broker (19092/19093/19094) and published by docker-compose.
EXPOSE 9092

ENTRYPOINT ["/entrypoint.sh"]
