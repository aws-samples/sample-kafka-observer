# Runbook — Scenario B: all primary replicas lost (extreme disaster)

**When**: both primary AZs (or all ISR members) are down simultaneously. Only the observer survives.

## Verified behavior (real-machine test, Tokyo)

Without promotion, the partition correctly refuses to elect the observer even under `unclean.leader.election.enable=true` — result is `Leader: none`. This is by design: an un-promoted observer must never lead, because it can never re-enter ISR and the partition would deadlock.

With promotion, the observer takes over fully:

```
kill broker2 + broker3 (all ISR members)
→ promote broker1 (remove from observer.ids on the surviving broker)
→ Leader: 1, Isr: 1
→ produced test message; consumed it back successfully
```

## Procedure

```bash
# 1. Promote: remove the observer id from /opt/kafka/observer.ids on ALL surviving brokers
echo "" | sudo tee /opt/kafka/observer.ids.tmp >/dev/null && sudo mv /opt/kafka/observer.ids.tmp /opt/kafka/observer.ids

# 2. Wait for it to enter ISR and be elected (≤10 s measured when it was already in Replicas)
kafka-topics.sh --bootstrap-server $OBSERVER_HOST:9092 --describe --topic $TOPIC
#    Expect: Leader: <observer-id>  Isr: <observer-id>

# 3. Writes: with ISR=1 < min.insync.replicas=2, acks=all is still blocked.
#    Decide explicitly (business decision, document who decides):
#      a) accept degraded durability: temporarily set topic min.insync.replicas=1  → acks=all resumes
#      b) keep fail-stop: serve reads only until another replica is restored
```

## Consistency guarantee at takeover

The observer's log is byte-identical to the last leader's log **up to the observer's LEO**. Because acks=all messages are only acknowledged after all ISR members (which excluded the observer) persisted them, the observer may trail by in-flight messages that were **never acknowledged** — so no acknowledged data is lost (RPO=0 for acknowledged writes), which is exactly Kafka's contract.

## After primaries return

```bash
# followers rejoin and catch up automatically → run preferred election to move leader back
kafka-leader-election.sh --bootstrap-server $BS --topic $TOPIC --partition 0 --election-type preferred
# restore min.insync.replicas if it was lowered
# demote the observer (add id back to the file)
```

## Multi-observer layouts

The `observer.ids` file accepts multiple ids (`1,4,5`). Standard layouts:

| Layout | Use |
|---|---|
| Fast-pair 2AZ primaries + 1 observer in 3rd AZ | Lowest latency + AZ-level DR; unambiguous promotion target |
| 3AZ primaries + observer in remote DC | Off-site strongly-consistent backup that never drags the main path |
| Multiple observers at several sites | At promotion time, pick the one with the smallest lag closest to the primary AZ — the choice is explicit, not automatic |
