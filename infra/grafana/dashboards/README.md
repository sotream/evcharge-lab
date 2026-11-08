# Grafana dashboards

Grafana loads every `*.json` file in this directory at start (folder `evcharge`). All panels use the
`Prometheus` datasource (uid `prometheus`, set in `provisioning/datasources/ds.yml`). Queries use
labels only (`service`, `job`, `group`, and so on), never instance addresses. Dashboards are
editable in the UI, but changes are not saved back to these files.

| File | Purpose |
|---|---|
| `overview.json` | The SLO view from ADR 0000. Start here. It shows OCPP CALLRESULT p99 (500 ms), connected stations per gateway, messages per second by action, telemetry durable lag p99 (5 s), driver-api read and write p99 (300 ms and 800 ms), outbox backlog and publish lag, consumer lag, site power against the cap (threshold 1.0), and the optimal versus fallback allocation rate. |
| `gateway.json` | Station gateway internals. Use it when stations cannot connect or CALLs fail. It covers upgrades, credential lookups, the produce buffer, produce errors and rejections, raw-log drops, commands, dispatch, safe-share, event-loop lag and RSS. |
| `sessions-allocation.json` | Sessions service and smart-charging allocation. Use it when reallocation is slow or the allocator falls back. It covers OCPP handler latency, rounds, round duration, reallocation latency, backlog, profiles, deferred increases, cap violations, allocator failures and reservations. |
| `billing.json` | Billing sagas. Use it when invoices are late or a payment is stuck. It covers pending and parked sagas, invoice latency against the 5 minute SLO, captures, PSP calls and idempotency mismatches. |
| `telemetry.json` | Telemetry ingest. Use it when readings are late or missing. It covers ingest rate, flush duration, durable lag, buffered rows, backpressure, dedup drops, dead letters and unknown measurands. |
| `kafka-and-infra.json` | Shared infrastructure. It covers consumer lag per group, topic and partition, producer and consumer outcomes, DLQ counts, and HAProxy sessions and backend health. |

Panels for "must stay 0" counters (cap violations, idempotency mismatches) show a red threshold at 1.
Some metrics only appear after the first event, so a panel can say "No data" on a healthy stack.

SLO panels read the recording rules in `infra/prometheus/rules/slo-recording.yml` (`slo:*`, ADR 0014), so they match the alerts. The exporter panels at the bottom of `kafka-and-infra.json` (Kafka consumer lag, Postgres connections and transactions, Redis memory and operations) need the exporters from `docker-compose.yml` running.
