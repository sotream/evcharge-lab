This run FAILED and is kept as evidence. The failure has two causes, neither of them the slow database:

- The OCPP CALLRESULT p99 was already 4.5-4.7 s in the "before" phase. Calls that wait for a Kafka ack were slow
  because Kafka itself was slow: `kafka-producer-perf-test` with acks=all at 50 msg/s measured p50 493 ms and
  p99 1,230 ms (observed, not saved). There were two reasons:
  - The Docker VM had swap fully used (an unbounded Jaeger in-memory store).
  - A feedback loop in sessions: every accepted SetChargingProfile triggered a new round, and every round resent
    every session's profile at the site. That produced about 140 profiles/s for 20 sites.

  All three were fixed afterwards:
  - accepted results are debounced by 2 s;
  - profiles are sent per session;
  - Jaeger has a bounded store.
- `ingest_lag_p99_s_after_recovery` (118 s) was measured over the minute in which the backlog itself drained, so it
  mostly reports the backlog. The check now runs 90 s after recovery.

The slowdown itself behaved as designed: ingest stalled, the backlog stayed in Kafka (peak consumer lag 665
records), it drained within 7 s once the CPU limit was lifted, and no probe reading was missing.
