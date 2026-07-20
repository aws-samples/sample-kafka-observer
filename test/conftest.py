"""
Pytest harness for the Kafka observer-replica patch (patches/kafka-3.7.1-zk/observer.patch).

Two cluster backends are supported, selected with --backend:

  * docker : brokers run as docker compose services (one container per broker);
             every command is executed via `docker compose exec`.
  * aws    : brokers run on EC2 instances provisioned by terraform/; the
             topology is read from `terraform output -json cluster_json` and
             every command is executed over ssh.

All Kafka CLI tools run *on a broker host* (never on the test machine), so the
test machine needs only python + docker/ssh — no local Kafka installation.

IMPORTANT (ZK mode caveat — see docs/architecture.md "Known limitations"):
  In ZooKeeper mode the controller sends LeaderAndIsr only to ISR members at
  topic creation. An observer therefore does NOT start fetching a *new* topic
  until its next restart or a controller failover. Every fixture that creates
  a topic here restarts the observer broker once, right after creation.
  Existing topics are unaffected; KRaft mode does not have this behavior.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Promotion/demotion SLOs. Measured on real clusters both complete in <= 10 s
# (file 5 s cache + fetch round-trip for promotion; isr-expiration period,
# replica.lag.time.max.ms / 2 = 15 s worst case, for demotion). CI containers
# are slower and noisier, so the asserted ceilings are relaxed.
DEFAULT_PROMOTE_TIMEOUT_S = 30
DEFAULT_DEMOTE_TIMEOUT_S = 45
SYNC_TIMEOUT_S = 90  # observer full-sync (log size convergence) ceiling


# ---------------------------------------------------------------------------
# pytest wiring: options and markers
# ---------------------------------------------------------------------------

def pytest_addoption(parser: pytest.Parser) -> None:
    g = parser.getgroup("kafka-observer")
    g.addoption("--backend", choices=("docker", "aws"), default="docker",
                help="cluster backend to run against (default: docker)")
    g.addoption("--compose-file", default=str(REPO_ROOT / "docker" / "docker-compose.yml"),
                help="[docker] compose file describing the cluster")
    g.addoption("--container-pattern", default="kafka-{id}",
                help="[docker] container/service name pattern, {id} = broker id")
    g.addoption("--terraform-dir", default=str(REPO_ROOT / "terraform"),
                help="[aws] directory holding terraform state with a cluster_json output")
    g.addoption("--observer-id", type=int, default=None,
                help="broker id configured as observer (default: from backend metadata, "
                     "falling back to 1)")
    g.addoption("--promote-timeout", type=int, default=DEFAULT_PROMOTE_TIMEOUT_S,
                help=f"seconds allowed for observer -> ISR (default {DEFAULT_PROMOTE_TIMEOUT_S}; "
                     "real clusters measure <= 10)")
    g.addoption("--demote-timeout", type=int, default=DEFAULT_DEMOTE_TIMEOUT_S,
                help=f"seconds allowed for ISR -> observer (default {DEFAULT_DEMOTE_TIMEOUT_S}; "
                     "real clusters measure <= 10-20)")
    g.addoption("--destructive", action="store_true", default=False,
                help="run tests marked 'destructive' (they stop brokers)")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "docker: test only meaningful on the docker backend")
    config.addinivalue_line("markers", "aws: test only meaningful on the aws backend")
    config.addinivalue_line(
        "markers",
        "destructive: stops/kills brokers; opt in with --destructive (never run "
        "against a cluster you are not willing to disrupt)")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    backend = config.getoption("--backend")
    run_destructive = config.getoption("--destructive")
    skip_backend = pytest.mark.skip(reason=f"test restricted to a backend != {backend}")
    skip_destr = pytest.mark.skip(reason="destructive test: pass --destructive to enable")
    for item in items:
        restricted = {m.name for m in item.iter_markers()} & {"docker", "aws"}
        if restricted and backend not in restricted:
            item.add_marker(skip_backend)
        if item.get_closest_marker("destructive") and not run_destructive:
            item.add_marker(skip_destr)


# ---------------------------------------------------------------------------
# Backend abstraction
# ---------------------------------------------------------------------------

@dataclass
class ExecResult:
    rc: int
    stdout: str
    stderr: str


class ClusterBackend(ABC):
    """Uniform view of a 3-broker (or larger) cluster running the observer patch."""

    broker_ids: list[int]
    observer_id: int
    bootstrap: str                       # bootstrap as seen from *broker* hosts
    kafka_bin: str = "/opt/kafka/bin"
    log_dir: str = "/data/kafka"
    ids_file: str = "/opt/kafka/observer.ids"

    # -- raw execution ------------------------------------------------------
    @abstractmethod
    def exec(self, broker_id: int, cmd: str, *, stdin: str | None = None,
             check: bool = True, timeout: int = 180) -> ExecResult:
        """Run a shell command on the given broker host/container."""

    @abstractmethod
    def start_broker(self, broker_id: int) -> None: ...

    @abstractmethod
    def stop_broker(self, broker_id: int) -> None: ...

    @abstractmethod
    def restart_broker(self, broker_id: int) -> None: ...

    @abstractmethod
    def bootstrap_for(self, broker_id: int) -> str:
        """bootstrap string reaching exactly this broker (used when others are down)."""

    # -- shared helpers -----------------------------------------------------
    @property
    def electable_ids(self) -> list[int]:
        return [b for b in self.broker_ids if b != self.observer_id]

    @property
    def admin_id(self) -> int:
        """Broker on which we run Kafka CLI tools by default (never the observer)."""
        return self.electable_ids[0]

    @property
    def kafka_libs(self) -> str:
        return str(Path(self.kafka_bin).parent / "libs")

    def _run_check(self, res: ExecResult, cmd: str, check: bool) -> ExecResult:
        if check and res.rc != 0:
            raise RuntimeError(
                f"command failed (rc={res.rc}): {cmd}\n--- stdout ---\n{res.stdout}"
                f"\n--- stderr ---\n{res.stderr}")
        return res

    def kafka_cli(self, tool: str, args: str, *, broker_id: int | None = None,
                  bootstrap: str | None = None, stdin: str | None = None,
                  check: bool = True, timeout: int = 180) -> ExecResult:
        """Run a Kafka CLI tool on a broker. `{args}` may reference --bootstrap-server."""
        bid = self.admin_id if broker_id is None else broker_id
        boot = bootstrap or self.bootstrap
        cmd = f"{self.kafka_bin}/{tool} --bootstrap-server {shlex.quote(boot)} {args}"
        return self.exec(bid, cmd, stdin=stdin, check=check, timeout=timeout)

    def write_file(self, broker_id: int, path: str, content: str) -> None:
        """Atomic write (tmp + mv), the same discipline as scripts/observer-*.sh."""
        q = shlex.quote(path)
        self.exec(broker_id,
                  f"{self.sudo_prefix()}sh -c 'cat > {q}.tmp && mv {q}.tmp {q}'",
                  stdin=content)

    def read_file(self, broker_id: int, path: str) -> str:
        return self.exec(broker_id, f"cat {shlex.quote(path)}", check=False).stdout

    def sudo_prefix(self) -> str:
        return ""

    # -- topic / data helpers -----------------------------------------------
    def create_topic(self, topic: str, assignment: str, *, min_isr: int = 2) -> None:
        """Create with an explicit replica assignment (observer id included).

        ZK-mode caveat: the caller MUST restart the observer broker afterwards
        or the observer will never learn about this topic (see module docstring).
        """
        self.kafka_cli(
            "kafka-topics.sh",
            f"--create --topic {shlex.quote(topic)} --replica-assignment {assignment} "
            f"--config min.insync.replicas={min_isr}")

    def delete_topic(self, topic: str) -> None:
        self.kafka_cli("kafka-topics.sh",
                       f"--delete --topic {shlex.quote(topic)}", check=False)

    def describe_topic(self, topic: str, *, bootstrap: str | None = None,
                       broker_id: int | None = None) -> list[dict]:
        """Parse `kafka-topics.sh --describe` into per-partition dicts."""
        out = self.kafka_cli("kafka-topics.sh",
                             f"--describe --topic {shlex.quote(topic)}",
                             bootstrap=bootstrap, broker_id=broker_id).stdout
        parts = []
        for line in out.splitlines():
            m = re.search(
                r"Partition:\s*(\d+)\s+Leader:\s*(\S+)\s+Replicas:\s*([\d,]+)\s+Isr:\s*([\d,]*)",
                line)
            if m:
                parts.append({
                    "partition": int(m.group(1)),
                    "leader": None if m.group(2) == "none" else int(m.group(2)),
                    "replicas": [int(x) for x in m.group(3).split(",") if x],
                    "isr": [int(x) for x in m.group(4).split(",") if x],
                })
        if not parts:
            raise RuntimeError(f"could not parse describe output for {topic}:\n{out}")
        return parts

    def produce(self, topic: str, n: int, *, prefix: str = "msg",
                bootstrap: str | None = None, broker_id: int | None = None) -> None:
        """acks=all idempotent produce of n lines via console producer (on a broker)."""
        payload = "".join(f"{prefix}-{i}\n" for i in range(n))
        self.kafka_cli(
            "kafka-console-producer.sh",
            f"--topic {shlex.quote(topic)} --producer-property acks=all "
            f"--producer-property enable.idempotence=true",
            stdin=payload, bootstrap=bootstrap, broker_id=broker_id)

    def consume(self, topic: str, *, isolation: str = "read_uncommitted",
                timeout_ms: int = 15000) -> list[str]:
        # console consumer exits non-zero on --timeout-ms expiry: expected, check=False
        res = self.kafka_cli(
            "kafka-console-consumer.sh",
            f"--topic {shlex.quote(topic)} --from-beginning "
            f"--isolation-level {isolation} --timeout-ms {timeout_ms}",
            check=False, timeout=(timeout_ms // 1000) + 120)
        return [l for l in res.stdout.splitlines()
                if l and not l.startswith("Processed a total of")]

    def replica_sizes(self, topic: str) -> dict[int, int]:
        """{broker_id: partition-0 size in bytes} from kafka-log-dirs.sh JSON."""
        blist = ",".join(str(b) for b in self.broker_ids)
        out = self.kafka_cli(
            "kafka-log-dirs.sh",
            f"--describe --topic-list {shlex.quote(topic)} --broker-list {blist}").stdout
        payload = next((l for l in out.splitlines() if l.startswith("{")), None)
        if payload is None:
            raise RuntimeError(f"no JSON in kafka-log-dirs output:\n{out}")
        sizes: dict[int, int] = {}
        for broker in json.loads(payload)["brokers"]:
            for ld in broker["logDirs"]:
                for p in ld.get("partitions", []):
                    if p["partition"] == f"{topic}-0":
                        sizes[int(broker["broker"])] = int(p["size"])
        return sizes

    def dump_log_batches(self, broker_id: int, topic: str,
                         partition: int = 0) -> list[tuple[int, int]]:
        """[(baseOffset, crc), ...] for every batch in the first log segment.

        Tests keep data volumes far below segment.bytes, so a single segment
        (00000000000000000000.log) always holds everything we produced.
        """
        seg = f"{self.log_dir}/{topic}-{partition}/00000000000000000000.log"
        out = self.exec(
            broker_id,
            f"{self.sudo_prefix()}{self.kafka_bin}/kafka-dump-log.sh "
            f"--deep-iteration --files {shlex.quote(seg)}").stdout
        return [(int(m.group(1)), int(m.group(2)))
                for m in re.finditer(r"baseOffset:\s*(\d+).*?\bcrc:\s*(\d+)", out)]

    def dump_log_raw(self, broker_id: int, topic: str, partition: int = 0) -> str:
        seg = f"{self.log_dir}/{topic}-{partition}/00000000000000000000.log"
        return self.exec(
            broker_id,
            f"{self.sudo_prefix()}{self.kafka_bin}/kafka-dump-log.sh "
            f"--deep-iteration --files {shlex.quote(seg)}").stdout

    def wait_broker_up(self, broker_id: int, timeout: int = 120) -> None:
        boot = self.bootstrap_for(broker_id)
        wait_until(
            lambda: self.kafka_cli("kafka-broker-api-versions.sh", "",
                                   bootstrap=boot, check=False).rc == 0,
            timeout=timeout, desc=f"broker {broker_id} answering ApiVersions")


class DockerBackend(ClusterBackend):
    """Brokers as docker compose services; commands via `docker compose exec -T -u root`."""

    def __init__(self, compose_file: str, container_pattern: str,
                 broker_ids: list[int], observer_id: int):
        self.compose_file = compose_file
        self.container_pattern = container_pattern
        self.broker_ids = broker_ids
        self.observer_id = observer_id
        # Inside the compose network every service resolves by name on 9092.
        self.bootstrap = ",".join(self.bootstrap_for(b) for b in broker_ids)
        self.kafka_bin = os.environ.get("OBSERVER_TEST_KAFKA_BIN", self.kafka_bin)
        self.log_dir = os.environ.get("OBSERVER_TEST_LOG_DIR", self.log_dir)
        self.ids_file = os.environ.get("OBSERVER_TEST_IDS_FILE", self.ids_file)

    def _container(self, broker_id: int) -> str:
        return self.container_pattern.format(id=broker_id)

    def _compose(self, *args: str) -> list[str]:
        return ["docker", "compose", "-f", self.compose_file, *args]

    def exec(self, broker_id, cmd, *, stdin=None, check=True, timeout=180) -> ExecResult:
        argv = self._compose("exec", "-T", "-u", "root",
                             self._container(broker_id), "sh", "-c", cmd)
        p = subprocess.run(argv, input=stdin, capture_output=True, text=True,
                           timeout=timeout)
        return self._run_check(ExecResult(p.returncode, p.stdout, p.stderr), cmd, check)

    def _lifecycle(self, verb: str, broker_id: int) -> None:
        subprocess.run(self._compose(verb, self._container(broker_id)),
                       check=True, capture_output=True, text=True, timeout=180)

    def start_broker(self, broker_id):
        self._lifecycle("start", broker_id)

    def stop_broker(self, broker_id):
        self._lifecycle("stop", broker_id)

    def restart_broker(self, broker_id):
        self._lifecycle("restart", broker_id)

    def bootstrap_for(self, broker_id):
        return f"{self._container(broker_id)}:9092"


class AwsBackend(ClusterBackend):
    """EC2 brokers described by `terraform output -json cluster_json`.

    Expected output shape (see test/README.md for the terraform contract):
      {
        "bootstrap":          "10.0.1.10:9092,10.0.2.10:9092,10.0.3.10:9092",
        "ssh_user":           "ec2-user",
        "ssh_key":            "~/.ssh/kafka-poc.pem",        # optional
        "kafka_bin":          "/opt/kafka/bin",              # optional
        "log_dir":            "/data/kafka",                 # optional
        "observer_ids_file":  "/opt/kafka/observer.ids",     # optional
        "brokers": [
          {"id": 1, "host": "10.0.1.10", "observer": true},
          {"id": 2, "host": "10.0.2.10"},
          {"id": 3, "host": "10.0.3.10"}
        ]
      }
    """

    def __init__(self, terraform_dir: str, observer_id_override: int | None):
        raw = subprocess.run(
            ["terraform", f"-chdir={terraform_dir}", "output", "-json", "cluster_json"],
            capture_output=True, text=True, timeout=60)
        if raw.returncode != 0:
            raise RuntimeError(
                f"terraform output cluster_json failed in {terraform_dir}:\n{raw.stderr}")
        cfg = json.loads(raw.stdout)
        if isinstance(cfg, str):  # output declared as a jsonencode()'d string
            cfg = json.loads(cfg)

        self._hosts = {int(b["id"]): b["host"] for b in cfg["brokers"]}
        self.broker_ids = sorted(self._hosts)
        self.bootstrap = cfg["bootstrap"]
        self.ssh_user = cfg.get("ssh_user", "ec2-user")
        self.ssh_key = cfg.get("ssh_key")
        self.kafka_bin = cfg.get("kafka_bin", self.kafka_bin)
        self.log_dir = cfg.get("log_dir", self.log_dir)
        self.ids_file = cfg.get("observer_ids_file", self.ids_file)

        declared = [int(b["id"]) for b in cfg["brokers"] if b.get("observer")]
        self.observer_id = (observer_id_override if observer_id_override is not None
                            else (declared[0] if declared else 1))

    def sudo_prefix(self) -> str:
        return "sudo "

    def _ssh_argv(self, broker_id: int) -> list[str]:
        argv = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
                "-o", "BatchMode=yes"]
        if self.ssh_key:
            argv += ["-i", os.path.expanduser(self.ssh_key)]
        return argv + [f"{self.ssh_user}@{self._hosts[broker_id]}"]

    def exec(self, broker_id, cmd, *, stdin=None, check=True, timeout=180) -> ExecResult:
        p = subprocess.run(self._ssh_argv(broker_id) + [cmd], input=stdin,
                           capture_output=True, text=True, timeout=timeout)
        return self._run_check(ExecResult(p.returncode, p.stdout, p.stderr), cmd, check)

    def start_broker(self, broker_id):
        self.exec(broker_id, "sudo systemctl start kafka")

    def stop_broker(self, broker_id):
        self.exec(broker_id, "sudo systemctl stop kafka")

    def restart_broker(self, broker_id):
        self.exec(broker_id, "sudo systemctl restart kafka")

    def bootstrap_for(self, broker_id):
        return f"{self._hosts[broker_id]}:9092"


# ---------------------------------------------------------------------------
# observer.ids manipulation (promotion / demotion) — mirrors scripts/observer-*.sh
# ---------------------------------------------------------------------------

class ObserverIdsFile:
    """Drive zero-restart promotion/demotion by rewriting observer.ids everywhere."""

    def __init__(self, cluster: ClusterBackend, promote_timeout: int, demote_timeout: int):
        self.cluster = cluster
        self.promote_timeout = promote_timeout
        self.demote_timeout = demote_timeout

    # -- file plumbing ------------------------------------------------------
    def write_ids(self, ids: list[int]) -> None:
        """Atomically write the observer id list on EVERY broker (tmp + mv)."""
        content = "".join(f"{i}\n" for i in sorted(set(ids)))
        for bid in self.cluster.broker_ids:
            self.cluster.write_file(bid, self.cluster.ids_file, content)

    def current_ids(self, broker_id: int | None = None) -> list[int]:
        bid = broker_id if broker_id is not None else self.cluster.broker_ids[0]
        raw = self.cluster.read_file(bid, self.cluster.ids_file)
        ids: list[int] = []
        for line in raw.splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                ids += [int(x) for x in line.replace(",", " ").split()]
        return sorted(set(ids))

    def ensure_observer(self) -> None:
        """Known-good baseline: exactly the configured observer id in the file."""
        self.write_ids([self.cluster.observer_id])

    # -- lifecycle transitions ----------------------------------------------
    def _isr(self, topic: str, **kw) -> list[int]:
        return self.cluster.describe_topic(topic, **kw)[0]["isr"]

    def promote(self, topic: str, *, timeout: int | None = None, **kw) -> float:
        """Remove the observer id from the file, wait for it to enter the ISR.

        Path: 5 s file-cache refresh -> next follower fetch -> maybeExpandIsr
        -> AlterPartition. Returns elapsed seconds.
        """
        obs = self.cluster.observer_id
        self.write_ids([i for i in self.current_ids() if i != obs])
        t0 = time.monotonic()
        wait_until(lambda: obs in self._isr(topic, **kw),
                   timeout=timeout or self.promote_timeout,
                   desc=f"broker {obs} joining ISR of {topic}")
        return time.monotonic() - t0

    def demote(self, topic: str, *, timeout: int | None = None, **kw) -> float:
        """Add the observer id back, wait for the native isr-expiration shrink.

        Path: 5 s file-cache refresh -> isr-expiration task (period =
        replica.lag.time.max.ms / 2, default 15 s) -> maybeShrinkIsr ->
        AlterPartition. Returns elapsed seconds.
        """
        obs = self.cluster.observer_id
        self.write_ids(self.current_ids() + [obs])
        t0 = time.monotonic()
        wait_until(lambda: obs not in self._isr(topic, **kw),
                   timeout=timeout or self.demote_timeout,
                   desc=f"broker {obs} leaving ISR of {topic}")
        return time.monotonic() - t0


# ---------------------------------------------------------------------------
# generic helpers
# ---------------------------------------------------------------------------

def wait_until(predicate, *, timeout: int, interval: float = 2.0, desc: str = "condition"):
    """Poll predicate() until truthy or fail the test with a clear message."""
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as e:  # transient CLI failures while brokers restart
            last_err = e
        time.sleep(interval)
    detail = f" (last error: {last_err})" if last_err else ""
    pytest.fail(f"timed out after {timeout}s waiting for: {desc}{detail}")


def wait_synced(cluster: ClusterBackend, topic: str, *, timeout: int = SYNC_TIMEOUT_S):
    """Wait until the observer's partition-0 log size equals the leader's.

    The patch byte-copies leader batches (appendAsFollower), so a fully caught
    up observer replica is byte-identical -> identical on-disk size.
    """
    leader = cluster.describe_topic(topic)[0]["leader"]
    assert leader is not None, f"{topic} has no leader"

    def _synced() -> bool:
        sizes = cluster.replica_sizes(topic)
        return (cluster.observer_id in sizes and leader in sizes
                and sizes[cluster.observer_id] == sizes[leader] > 0)

    wait_until(_synced, timeout=timeout,
               desc=f"observer log size == leader log size for {topic}")


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def cluster(request: pytest.FixtureRequest) -> ClusterBackend:
    backend = request.config.getoption("--backend")
    obs_override = request.config.getoption("--observer-id")
    if backend == "docker":
        ids_env = os.environ.get("OBSERVER_TEST_BROKER_IDS", "1,2,3")
        broker_ids = sorted(int(x) for x in ids_env.split(","))
        c: ClusterBackend = DockerBackend(
            compose_file=request.config.getoption("--compose-file"),
            container_pattern=request.config.getoption("--container-pattern"),
            broker_ids=broker_ids,
            observer_id=obs_override if obs_override is not None else broker_ids[0],
        )
    else:
        c = AwsBackend(
            terraform_dir=request.config.getoption("--terraform-dir"),
            observer_id_override=obs_override,
        )
    # Fail fast with a readable message if the cluster is unreachable.
    probe = c.kafka_cli("kafka-broker-api-versions.sh", "", check=False, timeout=60)
    if probe.rc != 0:
        pytest.exit(f"cluster unreachable via {backend} backend "
                    f"(bootstrap={c.bootstrap}):\n{probe.stderr or probe.stdout}",
                    returncode=3)
    return c


@pytest.fixture(scope="session")
def observer(request: pytest.FixtureRequest, cluster: ClusterBackend) -> ObserverIdsFile:
    o = ObserverIdsFile(
        cluster,
        promote_timeout=request.config.getoption("--promote-timeout"),
        demote_timeout=request.config.getoption("--demote-timeout"),
    )
    o.ensure_observer()  # deterministic baseline for the whole session
    yield o
    o.ensure_observer()  # never leave a promoted observer behind


@pytest.fixture()
def make_topic(cluster: ClusterBackend):
    """Factory: create a topic (observer as last replica) and make the observer
    fetch it. Handles the ZK new-topic caveat by restarting the observer broker
    once after creation (see module docstring). Topics are deleted on teardown.
    """
    created: list[str] = []

    def _make(name: str, *, min_isr: int = 2) -> str:
        e = cluster.electable_ids
        assignment = f"{e[0]}:{e[1]}:{cluster.observer_id}"
        cluster.create_topic(name, assignment, min_isr=min_isr)
        # ZK-mode caveat: controller only notifies ISR members of new topics;
        # the observer needs one restart to pick up the new assignment.
        cluster.restart_broker(cluster.observer_id)
        cluster.wait_broker_up(cluster.observer_id)
        created.append(name)
        return name

    yield _make
    for t in created:
        cluster.delete_topic(t)
