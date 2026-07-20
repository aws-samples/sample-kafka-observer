"""
Core observer lifecycle assertions, replaying the real-cluster evidence in
evidence/observer_v3_lifecycle_evidence.md against a live cluster:

  (a) a new topic's initial ISR excludes the observer
  (b) the observer fully syncs the log (end offsets / sizes converge)
  (c) the ISR keeps excluding the observer under sustained traffic
  (d) promotion: clear the ids file -> observer joins the ISR within the SLO
  (e) demotion: write the id back -> observer leaves the ISR within the SLO
  (f) a promoted observer can lead: kill the other brokers, verify it is
      elected leader and accepts writes (destructive, opt-in)

SLOs asserted here: promote <= 30 s, demote <= 45 s (CI-relaxed; real
clusters measure <= 10 s / <= 10-20 s respectively — see scripts/).

Tests are ordered as a single narrative on one topic where possible, but each
test re-establishes the state it needs, so they can also run individually.
"""

from __future__ import annotations

import pytest

from conftest import ClusterBackend, ObserverIdsFile, wait_synced, wait_until

TOPIC = "observer_lifecycle_test"
N_MESSAGES = 500


@pytest.fixture(scope="module")
def lifecycle_topic(cluster: ClusterBackend, observer: ObserverIdsFile):
    """One topic shared by the whole module, observer as the third replica."""
    observer.ensure_observer()
    cluster.delete_topic(TOPIC)  # tolerate leftovers from an aborted run
    e = cluster.electable_ids
    cluster.create_topic(TOPIC, f"{e[0]}:{e[1]}:{cluster.observer_id}", min_isr=2)
    # ZK-mode caveat (docs/architecture.md): the controller only sends
    # LeaderAndIsr to ISR members at creation, so the observer must restart
    # once to discover the new topic's assignment and start fetching.
    cluster.restart_broker(cluster.observer_id)
    cluster.wait_broker_up(cluster.observer_id)
    yield TOPIC
    observer.ensure_observer()
    cluster.delete_topic(TOPIC)


# ---------------------------------------------------------------------------
# (a) initial ISR excludes the observer
# ---------------------------------------------------------------------------
def test_initial_isr_excludes_observer(cluster: ClusterBackend, lifecycle_topic: str):
    """Controller-side hook: a brand-new topic is born with Isr = electable
    replicas only, even though the observer is in the replica assignment."""
    part = cluster.describe_topic(lifecycle_topic)[0]
    assert cluster.observer_id in part["replicas"], (
        f"test setup broken: observer {cluster.observer_id} missing from "
        f"replica assignment {part['replicas']}")
    assert cluster.observer_id not in part["isr"], (
        f"observer {cluster.observer_id} found in initial ISR {part['isr']} — "
        "the canAddReplicaToIsr / initial-ISR gate is not active")
    assert sorted(part["isr"]) == sorted(cluster.electable_ids)


# ---------------------------------------------------------------------------
# (b) observer fully syncs the data
# ---------------------------------------------------------------------------
def test_observer_fully_syncs(cluster: ClusterBackend, lifecycle_topic: str):
    """The observer replicates everything: its on-disk log converges to the
    same size as the leader's (appendAsFollower byte-copies leader batches)."""
    cluster.produce(lifecycle_topic, N_MESSAGES)
    wait_synced(cluster, lifecycle_topic)

    sizes = cluster.replica_sizes(lifecycle_topic)
    leader = cluster.describe_topic(lifecycle_topic)[0]["leader"]
    assert sizes[cluster.observer_id] == sizes[leader] > 0, (
        f"observer log size {sizes.get(cluster.observer_id)} != "
        f"leader log size {sizes.get(leader)}")


# ---------------------------------------------------------------------------
# (c) ISR keeps excluding the observer under traffic
# ---------------------------------------------------------------------------
def test_isr_stays_clean_under_traffic(cluster: ClusterBackend, lifecycle_topic: str):
    """A fully caught-up observer is exactly what would normally trigger ISR
    expansion — assert the gate holds it out even while it keeps fetching."""
    for _ in range(3):
        cluster.produce(lifecycle_topic, 100)
        isr = cluster.describe_topic(lifecycle_topic)[0]["isr"]
        assert cluster.observer_id not in isr, (
            f"observer {cluster.observer_id} leaked into ISR {isr} under traffic")


# ---------------------------------------------------------------------------
# (d) promotion: file change -> ISR membership within 30 s
# ---------------------------------------------------------------------------
def test_promotion_within_slo(cluster: ClusterBackend, observer: ObserverIdsFile,
                              lifecycle_topic: str):
    """Zero-restart promotion: remove the id from observer.ids on every broker
    and the native maybeExpandIsr path adds it to the ISR. SLO: 30 s here
    (measured <= 10 s on real clusters)."""
    wait_synced(cluster, lifecycle_topic)  # promote only a caught-up replica

    elapsed = observer.promote(lifecycle_topic)

    isr = cluster.describe_topic(lifecycle_topic)[0]["isr"]
    assert cluster.observer_id in isr
    assert elapsed <= observer.promote_timeout, (
        f"promotion took {elapsed:.1f}s > {observer.promote_timeout}s SLO")


# ---------------------------------------------------------------------------
# (e) demotion: file change -> out of ISR within 45 s
# ---------------------------------------------------------------------------
def test_demotion_within_slo(cluster: ClusterBackend, observer: ObserverIdsFile,
                             lifecycle_topic: str):
    """Zero-restart demotion: write the id back and the native isr-expiration
    task (replica.lag.time.max.ms / 2 period) shrinks it out. SLO: 45 s here
    (measured <= 10-20 s on real clusters)."""
    # Establish the promoted state if (d) did not run in this session.
    if cluster.observer_id not in cluster.describe_topic(lifecycle_topic)[0]["isr"]:
        wait_synced(cluster, lifecycle_topic)
        observer.promote(lifecycle_topic)

    # Demotion pre-check (same as scripts/observer-demote.sh): never demote a
    # leader — the shrink path cannot remove the leader itself.
    leader = cluster.describe_topic(lifecycle_topic)[0]["leader"]
    assert leader != cluster.observer_id, (
        "observer is currently the leader; test ordering violated")

    elapsed = observer.demote(lifecycle_topic)

    isr = cluster.describe_topic(lifecycle_topic)[0]["isr"]
    assert cluster.observer_id not in isr
    assert elapsed <= observer.demote_timeout, (
        f"demotion took {elapsed:.1f}s > {observer.demote_timeout}s SLO")

    # Round-trip sanity: demoted observer still replicates new data.
    cluster.produce(lifecycle_topic, 100)
    wait_synced(cluster, lifecycle_topic)


# ---------------------------------------------------------------------------
# (f) promoted observer becomes leader and serves writes (destructive)
# ---------------------------------------------------------------------------
@pytest.mark.destructive
def test_promoted_observer_can_lead(cluster: ClusterBackend, observer: ObserverIdsFile,
                                    make_topic, request: pytest.FixtureRequest):
    """Scenario B (docs/runbooks/scenario-b-total-loss.md): promote the
    observer, kill every other broker, verify the observer is elected leader
    and accepts a write with acks=1 (acks=all would need min.insync.replicas=1).

    Uses its own topic so a failure cannot poison the shared lifecycle topic.
    """
    topic = make_topic("observer_leader_test", min_isr=1)
    stopped: list[int] = []

    def _recover():
        for bid in stopped:
            cluster.start_broker(bid)
        for bid in stopped:
            cluster.wait_broker_up(bid)
        observer.ensure_observer()

    request.addfinalizer(_recover)

    cluster.produce(topic, 100)
    wait_synced(cluster, topic)
    observer.promote(topic)

    # Kill everything except the observer. From here on, all CLI commands must
    # run on the observer host against the observer's own listener.
    obs = cluster.observer_id
    obs_boot = cluster.bootstrap_for(obs)
    for bid in cluster.electable_ids:
        cluster.stop_broker(bid)
        stopped.append(bid)

    wait_until(
        lambda: cluster.describe_topic(topic, bootstrap=obs_boot,
                                       broker_id=obs)[0]["leader"] == obs,
        timeout=90,
        desc=f"broker {obs} elected leader of {topic} after ISR loss")

    # The promoted observer must accept writes on its own.
    res = cluster.kafka_cli(
        "kafka-console-producer.sh",
        f"--topic {topic} --producer-property acks=1 --request-timeout-ms 15000",
        stdin="written-by-promoted-observer\n",
        bootstrap=obs_boot, broker_id=obs, check=False)
    assert res.rc == 0, (
        f"write to promoted-observer leader failed:\n{res.stderr or res.stdout}")

    # And serve reads of what it just accepted.
    msgs = []

    def _read():
        nonlocal msgs
        res = cluster.kafka_cli(
            "kafka-console-consumer.sh",
            f"--topic {topic} --from-beginning --timeout-ms 10000",
            bootstrap=obs_boot, broker_id=obs, check=False, timeout=120)
        msgs = res.stdout.splitlines()
        return any("written-by-promoted-observer" in m for m in msgs)

    wait_until(_read, timeout=60,
               desc="reading back the write from the promoted-observer leader")
