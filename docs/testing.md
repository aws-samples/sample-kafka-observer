# Testing — methodology, harness, and the capability matrix

## Methodology: real machines, raw evidence

This project follows one rule: **only state what was measured.** Every behavioral claim in the README maps to a raw-output evidence file, and every evidence file was produced on a real cluster (EC2, Tokyo, 3 AZs — or the Docker environment for ZK-mode topic-creation semantics).

Three verification layers, cheapest first:

| Layer | What it proves | Where | Cost |
|---|---|---|---|
| **1. Static anchors** | The patch's anchor lines still exist verbatim in each supported Kafka tag | `tools/check-anchors.sh` (offline, greps a source tree) | seconds |
| **2. Apply + compile** | `git apply` is clean and the patched modules build | `tools/apply-and-build.sh`; CI `build-verify` matrix (7 legs: 3.6.2/3.7.1/3.8.1/3.9.1 ZK + 3.7.1 KRaft-combined + 4.0.0/4.1.0 KRaft, weekly drift sentinel) | minutes |
| **3. Runtime capability matrix** | Observer semantics actually hold on a running cluster | `test/` pytest suite (Docker or AWS backend); manual runbook drills for destructive scenarios | minutes–hours |

Layers 1–2 run in CI on every push and weekly against upstream tags. Layer 3 is run on demand — it mutates cluster state and (with `--destructive`) kills brokers.

## The capability matrix

"Observer support verified" for a Kafka version/mode means **all** of these passed on a live cluster:

| # | Capability | Pass criterion | Automated? |
|---|---|---|---|
| 1 | Initial-ISR exclusion | New topic spanning the observer: `Isr` excludes the observer id from creation (KRaft: controller log `Filtered observers [...] from initial ISR`) | ✅ `test_observer_lifecycle.py` |
| 2 | Full sync | Observer log size/offsets converge to the leader's under `acks=all` traffic | ✅ `test_observer_lifecycle.py` |
| 3 | ISR stays clean under traffic | Observer never appears in `Isr` while producing | ✅ `test_observer_lifecycle.py` |
| 4 | Promotion | Remove id from `observer.ids` → in ISR within 30 s (CI ceiling; ≤10 s measured on real clusters) | ✅ `test_observer_lifecycle.py` |
| 5 | Demotion | Add id back → out of ISR within 45 s (CI ceiling; ≤10–20 s measured) | ✅ `test_observer_lifecycle.py` |
| 6 | Promoted observer leads | Kill all other brokers after promotion → elected leader, serves writes | ✅ (`--destructive`) |
| 7 | Unclean-election refusal | Kill all ISR members with the observer *un-promoted* → `Leader: none`, even with `unclean.leader.election.enable=true` | manual drill (runbook B) |
| 8 | EOS preservation | Per-batch `(baseOffset, crc)` identical leader vs observer; txn COMMIT/ABORT markers byte-copied; `read_committed` views identical | ✅ `test_eos.py` |
| 9 | ELR compatibility (4.0+/4.1) | Observer never appears in `Elr:` / `LastKnownElr:` through kill-ISR sequences | manual drill ([evidence](../evidence/elr_verification_evidence.md)) |

KRaft additionally checks: AlterPartition defense-in-depth (`INELIGIBLE_REPLICA "observer"`) and new-topic instant fetch (the ZK-mode limitation must be absent).

## Running the pytest suite

Full harness documentation (backends, fixtures, safety markers, per-option reference): [`test/README.md`](../test/README.md). Summary:

```bash
cd test
uv sync

# Docker backend (local, uses ../docker/docker-compose.yml)
uv run pytest --backend docker                  # non-destructive
uv run pytest --backend docker --destructive    # + kill-brokers leader-election test

# AWS backend (reads topology from `terraform output -json cluster_json`)
uv run pytest --backend aws --terraform-dir ../terraform \
  --promote-timeout 15 --demote-timeout 25      # tight real-machine SLOs
```

Key properties of the harness:

- **Nothing runs Kafka locally** — every Kafka CLI call executes on a broker host (via `docker compose exec` or ssh). Locally you need only Python 3.10+, [uv](https://docs.astral.sh/uv/), and docker or ssh access.
- **Destructive tests are opt-in** (`--destructive`) and restore brokers plus the `observer.ids` baseline in finalizers. Never point the suite at a production cluster — it creates/deletes `observer_*` / `eos_*` topics and rewrites `observer.ids` on every broker.
- **Skips are explicit and explained**: e.g. the transactional-markers test compiles a small Java producer on a broker host and skips with a message if `javac` is absent there (JRE-only images).
- **CI-relaxed thresholds**: promotion ≤ 30 s / demotion ≤ 45 s in the suite defaults; real clusters measure ≤ 10 s for both — tighten with `--promote-timeout` / `--demote-timeout` on real hardware.

## Evidence discipline

- Every runtime claim lands in [`evidence/`](../evidence/) as raw command output plus a short interpretation, labeled **[fact]** (measured) vs **[inference]** (derived).
- Contributions that change behavior must come with evidence — see [CONTRIBUTING.md](../CONTRIBUTING.md).
- Known limitations are documented where they bite (e.g. the ZK-mode new-topic caveat is encoded directly into the test fixtures, which restart the observer after each topic creation — see `test/README.md`).
