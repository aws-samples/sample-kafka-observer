"""
Exactly-once / byte-level integrity tests, replaying the real-cluster evidence
in evidence/eos_byte_level_evidence.md and evidence/txn_read_committed_evidence.md:

  * CRC parity: kafka-dump-log.sh (DumpLogSegments) on the leader and the
    observer must report identical (baseOffset, crc) for the first N batches —
    the patch never touches the replication path, appendAsFollower byte-copies
    leader batches, so the observer's log is bit-identical, preserving
    producer-id / epoch / sequence / transaction markers.

  * Transactions: a transactional producer commits and aborts; the observer's
    log must contain the same control records (COMMIT/ABORT markers) as the
    leader, and a read_committed consumer sees only the committed messages.

    kafka-console-producer.sh cannot open transactions (it exposes
    transactional.id but never calls beginTransaction/commit — there is no CLI
    knob for the commit/abort protocol), so the transactional workload is a
    small Java program compiled on a broker host against the local Kafka libs.
    The test is skipped when no javac is available on the broker.
"""

from __future__ import annotations

import textwrap

import pytest

from conftest import ClusterBackend, ObserverIdsFile, wait_synced, wait_until

N_BATCHES = 20  # compare at least the first N batches, like the evidence run


# ---------------------------------------------------------------------------
# CRC parity: leader vs observer
# ---------------------------------------------------------------------------
def test_batch_crc_identical_leader_vs_observer(cluster: ClusterBackend,
                                                observer: ObserverIdsFile,
                                                make_topic):
    """Per-batch CRC parity is the strongest cheap proof of byte-identical
    replication: the CRC covers the batch payload including PID/epoch/sequence."""
    observer.ensure_observer()
    topic = make_topic("eos_crc_test")

    # Many small sends -> many batches (console producer flushes per line-ish;
    # linger.ms=0 keeps batches small so we exceed N_BATCHES comfortably).
    for _ in range(5):
        cluster.produce(topic, 200)
    wait_synced(cluster, topic)

    leader = cluster.describe_topic(topic)[0]["leader"]
    assert leader is not None and leader != cluster.observer_id

    leader_batches = cluster.dump_log_batches(leader, topic)
    observer_batches = cluster.dump_log_batches(cluster.observer_id, topic)

    assert leader_batches, "no batches parsed from leader dump-log output"
    n = min(N_BATCHES, len(leader_batches))
    assert len(observer_batches) >= n, (
        f"observer has {len(observer_batches)} batches, leader has "
        f"{len(leader_batches)} — observer not fully synced?")

    mismatches = [
        (l, o) for l, o in zip(leader_batches[:n], observer_batches[:n]) if l != o
    ]
    assert not mismatches, (
        "leader/observer batch (baseOffset, crc) mismatch — replication is NOT "
        f"byte-identical: {mismatches[:5]}")


# ---------------------------------------------------------------------------
# Transactions: commit/abort markers replicated, read_committed view identical
# ---------------------------------------------------------------------------
TXN_PRODUCER_JAVA = textwrap.dedent("""\
    import java.util.Properties;
    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerRecord;

    /** Commits {topic}: txn-committed-0..9, then aborts txn-aborted-0..9. */
    public class TxnProducer {
        public static void main(String[] args) throws Exception {
            String bootstrap = args[0];
            String topic = args[1];
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrap);
            p.put("key.serializer",
                  "org.apache.kafka.common.serialization.StringSerializer");
            p.put("value.serializer",
                  "org.apache.kafka.common.serialization.StringSerializer");
            p.put("transactional.id", "observer-eos-test");
            p.put("acks", "all");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
                producer.initTransactions();

                producer.beginTransaction();
                for (int i = 0; i < 10; i++)
                    producer.send(new ProducerRecord<>(topic, "txn-committed-" + i));
                producer.commitTransaction();

                producer.beginTransaction();
                for (int i = 0; i < 10; i++)
                    producer.send(new ProducerRecord<>(topic, "txn-aborted-" + i));
                producer.abortTransaction();
            }
            System.out.println("TXN_PRODUCER_DONE");
        }
    }
""")


@pytest.fixture(scope="module")
def javac_available(cluster: ClusterBackend) -> None:
    """Transactional workload needs javac on the admin broker (JDK, not JRE)."""
    if cluster.exec(cluster.admin_id, "command -v javac", check=False).rc != 0:
        pytest.skip(
            "javac not available on the broker host — transactional producer "
            "cannot be compiled (kafka-console-producer.sh cannot drive "
            "begin/commit/abort transactions; a JDK on the broker is required "
            "for this test)")


def test_transaction_markers_replicated_to_observer(cluster: ClusterBackend,
                                                    observer: ObserverIdsFile,
                                                    make_topic, javac_available):
    """Commit + abort one transaction each; assert (1) the observer's log holds
    the same control records as the leader's, (2) read_committed consumers see
    exactly the committed messages and none of the aborted ones."""
    observer.ensure_observer()
    topic = make_topic("eos_txn_test")

    # Compile and run the transactional producer on a broker, against the
    # broker-local Kafka client libs (guaranteed version match with the cluster).
    workdir = "/tmp/observer-eos-test"
    cluster.exec(cluster.admin_id, f"mkdir -p {workdir}")
    cluster.write_file(cluster.admin_id, f"{workdir}/TxnProducer.java",
                       TXN_PRODUCER_JAVA)
    cp = f"{cluster.kafka_libs}/*"
    cluster.exec(cluster.admin_id,
                 f"cd {workdir} && javac -cp '{cp}' TxnProducer.java", timeout=120)
    run = cluster.exec(
        cluster.admin_id,
        f"cd {workdir} && java -cp '{cp}:.' TxnProducer "
        f"{cluster.bootstrap} {topic}", timeout=120)
    assert "TXN_PRODUCER_DONE" in run.stdout, f"producer did not finish:\n{run.stderr}"

    wait_synced(cluster, topic)

    # (1) Control records (endTxnMarker COMMIT/ABORT) present and identical
    #     on leader and observer — they are part of the byte-copied log.
    leader = cluster.describe_topic(topic)[0]["leader"]

    def control_lines(dump: str) -> list[str]:
        return [l.strip() for l in dump.splitlines()
                if "endTxnMarker" in l or "isControl: true" in l]

    leader_ctrl = control_lines(cluster.dump_log_raw(leader, topic))
    obs_ctrl = control_lines(cluster.dump_log_raw(cluster.observer_id, topic))
    assert leader_ctrl, "no transaction control records found in the leader log"
    assert len(obs_ctrl) == len(leader_ctrl), (
        f"observer has {len(obs_ctrl)} control records, leader {len(leader_ctrl)}")
    # Full-line comparison: offsets, producerId, epoch and marker type all match.
    assert obs_ctrl == leader_ctrl, (
        "observer control records differ from leader — txn markers were not "
        "byte-copied")

    # CRC parity holds for transactional batches too.
    assert (cluster.dump_log_batches(leader, topic)
            == cluster.dump_log_batches(cluster.observer_id, topic))

    # (2) read_committed view: exactly the 10 committed messages, 0 aborted.
    def committed_view_ok() -> bool:
        msgs = cluster.consume(topic, isolation="read_committed")
        committed = [m for m in msgs if m.startswith("txn-committed-")]
        aborted = [m for m in msgs if m.startswith("txn-aborted-")]
        return len(committed) == 10 and not aborted

    wait_until(committed_view_ok, timeout=60,
               desc="read_committed view showing 10 committed / 0 aborted messages")
