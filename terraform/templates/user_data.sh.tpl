#!/usr/bin/env bash
# =============================================================================
# sample-kafka-observer — EC2 user_data (templatefile)
#
# Rendered per node with: node_role, broker_id, kafka_version, scala_version, mode.
#
# What this does:
#   - Install JDK 17 (Amazon Corretto *devel* — javac is required by the
#     Scala/Gradle build on the builder; headless JRE is NOT enough) + git
#   - Download the official vanilla Kafka binary into /opt/kafka
#   - Write node identity to /etc/kafka-poc.env
#
# What this deliberately does NOT do:
#   - Apply the observer patch or replace jars — that is done on the builder
#     via tools/apply-and-build.sh so the workflow matches a manual install
#   - Start ZooKeeper/Kafka — broker configs and startup are runbook steps
# =============================================================================
set -euxo pipefail
exec > >(tee /var/log/user-data.log | logger -t user-data) 2>&1

KAFKA_VERSION="${kafka_version}"
SCALA_VERSION="${scala_version}"
NODE_ROLE="${node_role}"
BROKER_ID="${broker_id}"
MODE="${mode}"

# --- Packages ----------------------------------------------------------------
dnf install -y java-17-amazon-corretto-devel git tar gzip

# --- Vanilla Kafka binary ------------------------------------------------------
# archive.apache.org hosts every release; dlcdn only hosts the latest.
KAFKA_TGZ="kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/$${KAFKA_VERSION}/$${KAFKA_TGZ}"

cd /opt
for attempt in 1 2 3; do
  curl -fSL --retry 3 -o "$${KAFKA_TGZ}" "$${KAFKA_URL}" && break
  echo "download attempt $${attempt} failed, retrying in 10s" >&2
  sleep 10
done
[ -s "$${KAFKA_TGZ}" ] || { echo "FATAL: could not download $${KAFKA_URL}" >&2; exit 1; }

tar -xzf "$${KAFKA_TGZ}"
rm -f "$${KAFKA_TGZ}"
ln -sfn "/opt/kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}" /opt/kafka
chown -R ec2-user:ec2-user "/opt/kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}"

# --- Node identity -------------------------------------------------------------
# Consumed by runbooks / pytest aws backend; brokers get their intended id here
# (broker.id is still set explicitly in server.properties by the runbook).
cat > /etc/kafka-poc.env <<EOF
KAFKA_POC_ROLE=$${NODE_ROLE}
KAFKA_POC_BROKER_ID=$${BROKER_ID}
KAFKA_POC_KAFKA_VERSION=$${KAFKA_VERSION}
KAFKA_POC_MODE=$${MODE}
KAFKA_HOME=/opt/kafka
EOF
chmod 0644 /etc/kafka-poc.env

# PATH convenience for interactive SSH sessions.
cat > /etc/profile.d/kafka-poc.sh <<'EOF'
export KAFKA_HOME=/opt/kafka
export PATH="$PATH:$KAFKA_HOME/bin"
EOF

echo "user_data complete: role=$${NODE_ROLE} broker_id=$${BROKER_ID} kafka=$${KAFKA_VERSION} mode=$${MODE}"
