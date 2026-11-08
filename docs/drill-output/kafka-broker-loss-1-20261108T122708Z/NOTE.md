This run FAILED and is kept as evidence. The 30 missing probe readings all belong to three stations
(st-0001-001, st-0003-001, st-0009-001) that had been stuck since before the broker was stopped. After a simulator
restart their OCPP uniqueIds were reused, and sessions deduplicated on the uniqueId alone. A new StartTransaction
therefore got a stored response without a transactionId, and the station retried it forever, so its queued
MeterValues never reached Kafka. Fixed by deduplicating on uniqueId, action and payload, and by boot-prefixed
simulator ids. The broker loss itself was not the cause.

Also note: during this run the telemetry Postgres was still capped at 0.05 CPU, left over from a slow-Postgres
drill teardown bug. The three stuck stations explain the missing readings (they stopped at 12:22, before this
drill), but ingest was slower than normal.
