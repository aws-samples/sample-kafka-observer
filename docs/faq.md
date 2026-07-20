# FAQ

**Q: How does this relate to KIP-966 (Eligible Leader Replicas)?**
KIP-966 is upstream's own admission that "ISR membership" and "leader eligibility" should be separable — the same conceptual direction as observers. But ELR solves election *safety* (avoiding data loss on unclean election), not the observer use case (a replica that syncs without affecting latency). The two compose: observers never enter ISR ⇒ never enter ELR (`maybePopulateTargetElr` candidates = current ELR ∪ current ISR), so the designs don't conflict — verified on real clusters on both 4.0 (ELR manually enabled) and 4.1 (ELR default-on), see [evidence](../evidence/elr_verification_evidence.md).

**Q: Why not wait for upstream?**
There is no upstream observer KIP with content. KIP-929 "Observer Replicas" exists as a wiki page with a **zero-length body** (verified via Confluence API) — a placeholder, not a plan. Users who need this capability today have three options: buy Confluent MRC, fork, or use a maintained patch set like this one.

**Q: How is this different from Confluent MRC Observers?**
Same core semantics (sync without ISR membership, controlled promotion). Differences: MRC is a supported commercial product with topic-level placement config (`confluent.placement.constraints`) and automatic promotion policies; this project is a minimal open reference (~60–115 lines) with file-driven identity and deliberately **manual** promotion (determinism first — appropriate for financial workloads; an auto-promotion policy is on the roadmap as an opt-in, default off).

**Q: Is redistribution of patched brokers legal?**
Apache License 2.0 permits modified redistribution with conditions: mark the artifact as modified (see our NOTICE), retain licenses, and do not call the result "Apache Kafka" (trademark). This project distributes **patches**, not binaries, which keeps the compliance surface minimal. If you distribute built jars internally, keep the NOTICE with them.

**Q: What is the maintenance cost across Kafka upgrades?**
Lower than expected: the three broker-side anchors are **byte-identical across 3.6.2 → 4.1.0** (six versions, verified by source comparison), and the two ZK controller anchors are identical across 2.8.2 → 3.9.1. The patch script fails loudly (`exact-match count == 1`) on any drift, and CI re-verifies apply+compile weekly against every supported tag. When an anchor finally moves, that version gets its own patch directory.

**Q: Does the observer slow down producers at all?**
No — structurally. The HW gate (hook 2b) means the high-watermark calculation never waits for observers even inside the replica-lag window. Measured: acks=all latency with an observer in the slowest AZ equals the fast-pair baseline (2.04–2.35 ms in our reference topology).

**Q: What happens if the observer.ids file is lost or corrupted?**
The broker keeps running. The loader treats a missing file as "fall back to the env var, else empty set," and an unreadable/corrupt file as "keep the last cached value + WARN." A lost file therefore means observers may gradually promote (fail-open toward availability) — monitor the WARN log line. This trade-off is deliberate: a config file must never take a broker down.

**Q: Can I run multiple observers?**
Yes — the file takes multiple ids. Typical layouts: fast-pair primaries + one observer in a third AZ; 3-AZ primaries + a remote-site observer; several observers where promotion picks the least-lagged. Promotion choice is explicit (an operator decision), not automatic.

**Q: ZooKeeper mode or KRaft?**
Both are supported (asymmetric strategy): ZK mode is the verified v0.3 for existing fleets; KRaft is the mainline — fully verified since v0.5 (broker-side hooks plus a controller-side Java patch in the `metadata` module), extended to Kafka 4.0/4.1 in v0.6. Observer semantics, file format, and runbooks are identical across modes, so the capability survives a ZK→KRaft migration with no gap. See [multi-version.md](multi-version.md).
