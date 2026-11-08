This is the clean slow-Postgres run. It started after a rolling broker restart, a forced PREFERRED leader election, and
more than 5 minutes of waiting (the election and the wait were observed by hand and are not in this run's
events.log), so no automatic leader rebalance fell inside it. The simulator used the default 30 s
meter interval.

It PASSED every check. The OCPP CALLRESULT p99 stayed at 0.076 s while the telemetry database was capped at 0.05 CPU,
and the durable lag 90 s after recovery was 0.49 s.

Limitation: at this meter rate the ingest never built a Kafka backlog (peak consumer lag 0). Backlog growth and drain
are shown by the 2 s meter-interval runs slow-postgres-20261108T150304Z (peak lag 133, failed for an unrelated Kafka
leader move) and slow-postgres-20261108T160959Z (peak lag 469, confounded by an overloaded VM). Both are kept with
notes.
