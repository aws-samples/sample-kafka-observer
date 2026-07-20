# aws-samples publication checklist

Pre-submission checklist for publishing this repository to the `aws-samples` GitHub organization. Status reflects the repo-hygiene review of 2026-07-20.

## 1. Required files (aws-samples baseline)

- [x] `LICENSE` — full canonical Apache-2.0 text (202 lines; the earlier abbreviated version was replaced)
- [x] `NOTICE` — standard Amazon copyright line + Apache Kafka trademark disclaimer + derived-code statement
- [x] `README.md` — 30-second pitch, architecture diagrams (mermaid + SVG), quick start, version matrix, trademark note
- [x] `CONTRIBUTING.md` — aws-samples standard structure + project-specific expectations (patch regeneration, shellcheck, evidence discipline)
- [x] `CODE_OF_CONDUCT.md` — Amazon Open Source Code of Conduct reference
- [x] `SECURITY.md` / security-notification section — AWS vulnerability-reporting page + project scope notes
- [x] `.gitignore` — covers tfstate, build dirs, caches. (No `.env*` / credentials are used anywhere in this repo.)

## 2. Legal / license review items (for the open-source review ticket)

- [ ] **Derived code**: `patches/*/observer.patch` contain context lines and modified fragments of Apache Kafka source (Apache-2.0). NOTICE declares this. Confirm reviewer is comfortable with distributing diffs of ALv2 code under ALv2 — standard, but call it out.
- [ ] **Trademark**: "Apache Kafka" is used only descriptively; repo name (`sample-kafka-observer`) does not lead with "Apache Kafka"; README and NOTICE explicitly instruct users not to label rebuilt binaries "Apache Kafka". FAQ covers redistribution conditions.
- [ ] **Third-party name usage**: Confluent (MRC, Replicator, Cluster Linking), Uber uReplicator, LinkedIn Brooklin are referenced factually for comparison (`docs/industry-comparison.md`). No logos, no claims of affiliation ("not affiliated with … Confluent, Inc." in NOTICE/README).
- [ ] **No binaries distributed**: patches are the canonical artifact; Docker builds from upstream source at build time; Terraform downloads vanilla Kafka. Verify no jars slip into the repo (`find . -name '*.jar'` must be empty).
- [ ] **Chinese-language content**: `docs/zh/` is an intentional Chinese mirror (POC report). Evidence files (`evidence/*.md`), `patches/*/README.md`, in-patch code comments, and `tools/generate-patch.py` docstrings also contain Chinese. Decide: keep as-is (they are raw lab records — translating would break the "evidence = verbatim" principle), add one line to README explaining evidence files are bilingual lab records, or translate patch-dir READMEs (small, user-facing — recommended). See "Open items" below.
- [ ] **Upstream bug discussion**: KAFKA-19522 is described factually with the upstream fix commit — no disclosure concern (already public upstream).

## 3. Content hygiene (verified in this review)

- [x] No credentials, tokens, or key material (`grep -rn "sk-\|ghp_\|AKIA\|ABSK"` clean)
- [x] No real IPs / hostnames / account IDs — all examples use RFC-1918 (`10.0.x.x`) or RFC-5737 (`203.0.113.10`) addresses; the POC host IPs and key names never appear
- [x] No personal paths (`/Users/...`) or usernames in tracked files
- [x] No customer names. Two Chinese evidence/zh files mention "交易所客户" (exchange customer) generically — no identifiable entity; acceptable, but easy to soften if legal prefers
- [x] Internal link integrity: previously-missing `docs/industry-comparison.md` and `docs/testing.md` now exist; `docs/monitoring-alerting.md` added
- [x] Full-repo relative-link check passes (2026-07-20, after `docs/design-story.md`, `docs/scenario-playbook.md`, `docs/zh/README.md` landed). Re-run as the final pre-publish gate: extract `](...)` targets from all `*.md` and test existence.
- [x] Version references in docs updated (stale "v0.4 will…" phrasing removed from docker/terraform READMEs)

## 4. Repo settings after transfer to aws-samples

- [ ] Description: "Observer/Learner replicas for Apache Kafka — sync without joining the ISR; promote in seconds with zero data movement. Reference implementation with real-cluster evidence."
- [ ] Topics: `kafka`, `apache-kafka`, `high-availability`, `multi-az`, `disaster-recovery`, `replication`, `exactly-once`, `aws`
- [ ] Default branch `main`; branch protection: require PR + passing `build-verify` and `lint` checks
- [ ] Enable: Issues, Discussions (optional); disable: Wiki, Projects (docs live in-repo)
- [ ] GitHub Actions: confirm the weekly cron (`build-verify` drift sentinel) is allowed under org policy; set `permissions: contents: read` in workflows if not already
- [ ] Add issue templates (`.github/ISSUE_TEMPLATE/`):
  - **Bug report** — fields: Kafka version + mode (ZK/KRaft), patch dir used, `kafka-topics --describe` output, `observer.ids` content on each node, relevant broker/controller log lines
  - **Version-support request** — new Kafka tag; ask requester to paste `tools/check-anchors.sh` output for that tag
  - **Question** — pointer to FAQ / docs first
- [ ] Suggested first (pinned) issue: "Roadmap to v0.7 — metrics, opt-in auto-promotion, audit log" linking ROADMAP.md, inviting feedback on the proposed JMX metric names in `docs/monitoring-alerting.md`
- [ ] `SUPPORT`/expectations note in README or issue template: community-supported sample, no SLA, not an AWS service

## 5. Open items / residual risks (decide before flipping public)

1. **Bilingual artifacts** (highest-visibility item): evidence files and patch-dir READMEs are partly Chinese. Options: (a) publish as-is with a one-line note, (b) translate the four `patches/*/README.md` (user-facing, ~30 lines each) and keep evidence raw, (c) full translation. Recommendation: **(b)**.
2. **`docs/zh/POC验证报告.md`** contains internal-process phrasing ("会议已定调…不承担生产代码责任" — meeting decided: provide the approach, no production-code liability). Harmless but internal-sounding; consider trimming §六(责任边界) or replacing the file with the English README content it mirrors.
3. **Patched-Kafka operational risk framing**: README states patches are a reference, not a supported product; consider adding an explicit "not an AWS service; test before production" disclaimer box if the review asks for one.
4. **Weekly CI against upstream tags** will eventually fail when a new Kafka release drifts an anchor — that is by design (drift sentinel), but note it in CONTRIBUTING so failing badges don't look like neglect.
