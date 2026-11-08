# 0014. Observability: SLO recording rules and alerts

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000 SLO rows: Remote start, OCPP responses, Telemetry, Driver API, Station gateway,
  Billing, Smart charging (site cap and reallocation). ADR 0006, 0007, 0008 and 0012 supply the
  failure modes the non-SLO alerts watch.

## Context

Every service already exports Prometheus metrics, and six Grafana dashboards chart them. Until now
each SLO panel computed its own quantile from raw buckets, and nothing evaluated an SLO
continuously. A breach was visible only if someone looked at the right dashboard. We need:

- one definition per SLO, shared by dashboards, alerts and the load-test reports;
- alerts that fire on budget burn rather than on single bad samples;
- alerts for the invariants and pipelines whose failure the SLOs do not show directly (the site cap,
  outbox, dead-letter topics, parked sagas).

## Decision

Rules live in `infra/prometheus/rules/` and are loaded through `rule_files`. They are checked and
unit-tested by `make prom-check` (`promtool check rules` and `promtool test rules` in a one-off
`prom/prometheus:v3.16.0` container, part of `make check`).

### Recording rules (`slo-recording.yml`)

Recorded names follow `level:metric:operation`. Latency rules exist at 5m and 1h windows
(`_5m`, `_1h`). The 5m window drives alerts and "now" panels, and the 1h window smooths the load-test
and drill traces.

| Rule | ADR 0000 row | Source metric |
|---|---|---|
| `slo:remote_start_delivery:p99_*` | Remote start, delivered p99 < 2 s | `gateway_command_duration_seconds{action="RemoteStartTransaction"}` (proxy, see below) |
| `slo:ocpp_callresult:p99_*` | OCPP responses p99 < 500 ms | `ocpp_call_duration_seconds` for Authorize, StartTransaction, StopTransaction |
| `slo:driver_api_read:p99_*` | Driver API read p99 < 300 ms | `http_server_requests_seconds{service="driver-api",method="GET"}` minus health, ready, live and metrics routes |
| `slo:driver_api_write:p99_*` | Driver API write p99 < 800 ms | same, method POST, PUT, PATCH or DELETE |
| `slo:reallocation:p99_*` | Smart charging reallocation p99 < 5 s | `sessions_reallocation_latency_seconds` |
| `slo:telemetry_durable:p99_*` | Telemetry durable p99 < 5 s | `telemetry_ingest_lag_seconds{stage="durable"}` |
| `slo:billing_invoice:p99_*` | Billing invoice p99 < 5 min | `billing_invoice_latency_seconds` |
| `service:http_server_requests:p99_*`, `action:ocpp_call_duration:p99_*`, `service:outbox_publish_lag:p99_*` | none directly (per-service views) | the same histograms, grouped by `service` or `action` |
| `slo:driver_api_error_ratio:rate{5m,30m,1h,6h}` | Driver API 99.9% availability | see SLIs |
| `slo:gateway_error_ratio:rate{5m,30m,1h,6h}`, `slo:gateway_upgrade_reject_ratio:rate{5m,1h}` | Station gateway 99.95% availability | see SLIs |
| `slo:site_cap_utilisation:max` | Smart charging, the cap is never exceeded | `max(sim_site_drawn_watts / sim_site_cap_watts)` |
| `group:kafka_consumer_lag:sum` | none directly | `sum by (group) (kafka_consumer_lag)` |

### SLI choices

**Remote start delivery is a proxy.** No metric spans the whole path "driver tap to
RemoteStartTransaction delivered". The closest measurable span is
`gateway_command_duration_seconds`: from the owning gateway receiving the command from Kafka to the
station's response. It is measured at the end that matters (the station), but it excludes the
driver-api request, the sessions transaction, the outbox relay and the Kafka hop, and it includes the
station's own reply time, so it is an approximation, neither strictly an upper nor a lower bound. The
end-to-end figure is measured by the load test (`load/`), not by an alert. The "confirmed charging
p99 < 5 s" half of the Remote start row is not covered by any rule: no metric ties a StartTransaction
back to the remote start that caused it.

**driver-api availability** is the classic request SLI: `5xx / all requests` for `service="driver-api"`,
excluding health and metrics routes. 4xx responses are client errors and are not counted. Gateway-side
timeouts that driver-api reports as 503 or 504 are counted, because the driver saw a failure.

**Gateway availability.** The gateway has no request/response HTTP surface: its job is to keep 50,000
sticky WebSockets and answer CALLs. The candidates were:

1. Accepted upgrades over attempts (`gateway_upgrades_total`). It measures only connection setup.
   It is quiet in steady state (traffic is a few upgrades per minute) and spikes during reconnect
   storms, which is when a ratio over a short window means least. Rejects also include legitimate
   authentication failures from misconfigured stations.
2. CALLERROR frames sent over CALL frames received (`ocpp_messages_total`). It is high volume
   (about 550 CALLs/s in ADR 0000), covers every action, and a CALLERROR is exactly the gateway
   telling a station that it failed (InternalError when the produce buffer is full or a dependency is
   down, see ADR 0012).

We use option 2 as the SLI for burn-rate alerts: `sum(rate(ocpp_messages_total{direction="out",
type="error"})) / sum(rate(ocpp_messages_total{direction="in",type="call"}))`. A gateway that is down
sends no CALLERRORs at all. That case is covered by the 60 s reconnect objective and the
`gateway_connected_stations` signal in the runbook, not by this SLI, and this limit is accepted. The
upgrade reject ratio is recorded as a supplementary series (`slo:gateway_upgrade_reject_ratio`) and
charted, but does not alert.

The simulator's own `sim_*` metrics are used only for the site cap. The simulator is the only
component that knows the power physically drawn per site, which is why the cap alert is driven by
`sim_site_drawn_watts / sim_site_cap_watts` and why the allocation guard counters are separate alerts.

### Alerts (`slo-alerts.yml`)

Every alert has a `severity` label (`critical` or `warning`) and a `runbook` annotation pointing at a
section of `docs/runbook.md`.

| Alert | Condition | Severity |
|---|---|---|
| `DriverApiErrorBudgetBurnFast` / `Slow` | 14.4x budget on 1h and 5m / 6x budget on 6h and 30m, budget 0.1% | critical / warning |
| `GatewayErrorBudgetBurnFast` / `Slow` | same factors, budget 0.05% | critical / warning |
| `SiteCapExceeded` | `slo:site_cap_utilisation:max > 1`, no `for` (any occurrence) | critical |
| `AllocationCapViolationRejected` | `increase(sessions_allocation_cap_violations_rejected_total[5m]) > 0` | critical |
| `InvariantViolation` | `increase(invariant_violations_total[5m]) > 0` | critical |
| `AllocationBacklog` | `sessions_allocation_backlog_seconds > 60` for 2m | warning |
| `OutboxPublishLagHigh` | outbox lag p99 > 5 s for 5m | warning |
| `OutboxPendingGrowing` | `outbox_pending > 100` and rising over 10m | warning |
| `DlqMessages` | `increase(kafka_consumer_messages_total{outcome="dlq"}[10m]) > 0` | warning |
| `TelemetryDeadLettered` | `increase(telemetry_dead_lettered_total[10m]) > 0` | warning |
| `KafkaConsumerLagHigh` | application `kafka_consumer_lag` > 10,000 per group for 5m | warning |
| `KafkaExporterConsumerLagHigh` | `kafka_consumergroup_lag` from kafka-exporter > 10,000 per group for 5m | warning |
| `TelemetryDurableLagSloBreach` | durable p99 > 5 s for 5m | critical |
| `TelemetryIngestPaused` | `telemetry_ingest_paused == 1` for 5m | critical |
| `BillingSagaParked` | `increase(billing_sagas_parked_total[10m]) > 0` | critical |
| `BillingPspIdempotencyMismatch` | `increase(billing_psp_idempotency_mismatch_total[10m]) > 0` | critical |

Burn-rate alerts follow the multi-window, multi-burn-rate method of the Google SRE workbook. The
fast pair (1h and 5m at 14.4x) spends 2% of a 30-day budget in an hour. The slow pair (6h and 30m at
6x) spends 5% in six hours. Both windows must exceed the threshold, so a spike that already ended
(long window hot, short window cold) does not page. Thresholds: driver-api 1.44% and 0.6%, gateway
0.72% and 0.3%.

The consumer lag threshold of 10,000 records is an `[assumption]` for this lab, sized at about four
seconds of peak ingest (2,400 messages/s in ADR 0000). It should be tuned after reading drill output.
Consumer lag exists twice on purpose: the application metric is cheap and per partition, while
kafka-exporter measures committed offsets against the broker log end and keeps working when a service
is dead.

The alert on a counter's `increase` fires only after the counter exists. Counters such as
`billing_sagas_parked_total` appear on first increment, so the first parked saga produces a series that
starts at 1 and `increase` may report 0 for it. This is a known Prometheus limitation, so these alerts are
backed up by the dashboard panels and the runbook sections.

### Exporters

Compose gets four exporters, all pinned and capped at 64 MB (`mem_limit`):
`danielqsj/kafka-exporter:v1.11.0` (host port 29308), `prometheuscommunity/postgres-exporter:v0.20.1`
twice for `postgres` (29187, label `db=oltp`) and `postgres-telemetry` (29188, label `db=telemetry`),
and `oliver006/redis_exporter:v1.97.0` (29121). They feed the kafka-and-infra dashboard and the
exporter lag alert.

### No Alertmanager

Alerts are evaluated by Prometheus and are visible in its UI (`localhost:29090/alerts`) and as
the `ALERTS` series, which Grafana can chart. There is no Alertmanager, no routing tree and no paging
integration. This is a lab running on one laptop with no on-call. A pager target would be an empty
integration, and silences, grouping and inhibition would have nothing to act on. Paging is a non-goal
of this lab (ADR 0000 lists no on-call requirement). Adding Alertmanager later needs only an
`alerting:` block in `prometheus.yml`: the `severity` labels are already the routing key, and the
`runbook` annotations are already what a notification template would link to.

## Alternatives considered

- Alerting on the SLO threshold directly (for example p99 over 500 ms for five minutes). Simple,
  but it pages on every short blip and is blind to a slow sustained burn. It is used only where the SLO
  is a latency or lag target and not an availability budget (telemetry durable lag).
- Recording the quantiles per instance. Gateways are interchangeable and stations move between them
  (ADR 0004), so a per-instance p99 would be noise.
- Prometheus-side alerts for the Remote start end-to-end path through a synthetic probe. Rejected as
  extra moving parts. The load test reports that figure.

## Consequences

- Dashboards, alerts and tests share one definition per SLO. A change to an SLI is a change to
  `slo-recording.yml` plus its test.
- `make check` now needs Docker for `prom-check` (the Prometheus image is already used by compose).
- Drills and load tests can read `ALERTS{alertstate="firing"}` as a pass/fail signal.
- Known limits, accepted: the Remote start proxy, the gateway SLI being blind to a fully down gateway,
  first-increment counters, and no notification path.
