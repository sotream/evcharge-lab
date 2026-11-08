This run FAILED on one check. The cause was isolated afterwards, and it was not the slow database.

What held:
- Ingest stalled while the telemetry database was capped at 0.05 CPU.
- The backlog stayed in Kafka (peak consumer lag 133 records) and drained within seconds once the cap was lifted.
- 90 s after recovery the durable lag p99 was 1.37 s (target < 5 s).
- 0 of 2,103 probe readings were missing.

What failed: the OCPP CALLRESULT p99 rose to the 10 s histogram cap during the slowdown, then fell back to about
0.7 s (timeline.jsonl).

The p99 was already above the 0.5 s target before the database was capped. In the before phase of timeline.jsonl,
`callresult_p99_s` was 0.24-0.47 s until 15:03:44. From 15:03:45 it was 0.84-0.85 s (maximum 0.849 s), and it stayed
there until 15:04:09, so it was above 0.5 s before the database cap (applied at 15:04:06) and before the 15:04:31
leader move. This pre-fault rise is not isolated: no cause was found for it.

Cause (from the broker and gateway logs for 15:04:25-15:07:30, read after the run):
- At 15:04:31 all three brokers moved partition leadership (epochs bumped; "Removed fetcher" / "Added fetcher";
  group coordinators unloaded and reloaded __consumer_offsets).
- This was Kafka's preferred-leader rebalance (`auto.leader.rebalance.enable`, checked every 300 s). It fired 5 min
  after the rolling broker restart at 14:58-14:59 (kafka-3 started 14:59:28), not in response to the drill.
- During the move, the gateway's in-flight produces hung until its send timeout: 50 x "kafka send timed out after
  30000ms" and 54 x "CALL failed" (log counts observed, not saved as a file). Every CALL waiting for a Kafka ack (Boot, Status, MeterValues) held its CALLRESULT.
- StopTransaction slowed too: 4 x "POST .../stop-transaction timed out after 1000ms" (observed, not saved) while sessions' own outbox
  relay hit the same leader move. Heartbeat, answered locally, stayed at about 8 ms.
- Jaeger has no spans of 300 ms or more for the gateway or sessions in this window; the WebSocket CALL path is not
  traced, which is why the logs were needed.

Fix: the gateway send timeout is now 3 s (KAFKA_SEND_TIMEOUT_MS). A hung produce fails fast with CALLERROR
InternalError, and the station retries with the same uniqueId, which is idempotent downstream. This bounds a station's
wait during a leader move at about 3 s instead of 30 s; it does not meet the 500 ms SLO during the move itself.

The slow-database part of the drill passed.
