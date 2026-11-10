# Production-readiness brief (review of 2026-11-10)

Source: a read-only review of the repository on 2026-11-10 (commit `70cfcb2`). Nothing was run during it: the
findings come from reading the code, the docs and the raw files in `docs/load-output/` and `docs/drill-output/`.
The plan that implements this brief is
[`docs/superpowers/plans/2026-11-10-prod-readiness.md`](../plans/2026-11-10-prod-readiness.md).

## Verdict

The project is a strong and honest system-design lab: failed runs are kept, the derived numbers in ADR 0000 are
arithmetically correct, and the README figures match the raw files. It is far from production on three axes that
are not about scale: **money** (invoice, VAT, energy plausibility, dunning), **law** (Eichrecht, GDPR, invoice
numbering) and **closed loops with the hardware** (site meter, rejected profiles, stuck sessions).

## Numbers that were checked and hold

- ADR 0000 derived rows: sessions, heartbeats, MeterValues, data volumes and instance counts recomputed, no error.
- README glance table vs raw files: gateway 149,414 / 60 s = 2,490.23 msg/s; telemetry 2,471.23 rows/s; driver-api
  955.59 RPS, p99 255.2 ms; sessions 199.99, smart-charging 999.97, tariff 1,999.97.
- Header counters: 22 ADRs, 12 drill scripts, 19 of 20 drills pass on the latest run.

## Numbers to correct or qualify (N1–N11)

| # | Issue | Why | Action |
|---|---|---|---|
| N1 | "mid-sized German CPO" with 50,000 stations / 100,000 connectors | Germany has roughly 160–190k public charge points in total (reviewer's memory, to verify). 50k stations is larger than any German CPO. | Relabel as "large CPMS platform" or shrink the fleet. |
| N2 | 45 min average session with 80% AC | AC sessions typically last 2–4 h; 45 min is a DC figure. Start rate is 3–4× high. | Keep as headroom, but label it. |
| N3 | 320k sessions/day for 200k daily active drivers | 1.6 sessions per active driver per day is several times reality. | Reconcile one of the two. |
| N4 | Inbound OCPP total 1,600/s, design 2,400/s | Omits ~900/s CALLRESULTs for outbound SetChargingProfile. Real peak ~2,500/s, design ×1.5 ~3,750/s. | Recompute; per instance ~750/s, still under the 2,490 measured. |
| N5 | "10 years (German tax law)" for invoices | Since 2025 (BEG IV) accounting vouchers are probably 8 years (to verify). | Correct after verification. |
| N6 | 10,000 connections "met" | Native process, stub `sessions`, no HAProxy. 10k connections and 2k msg/s never measured together. | One combined run through HAProxy. |
| N7 | driver-api reads 955 RPS | 30 keys and a 3 s cache: ~99% cache hits. 20 sites seeded, model says 5,000. | Measure with 5,000 sites and a cold cache. |
| N8 | Run durations | k6 60 s, telemetry 20 s, allocbench 30 s, one run each. Autovacuum, checkpoints, compression never show. | Soak 30–60 min, 3–5 repeats. |
| N9 | smart-charging p99 19.45 ms vs 20 | One 30 s run, p99.9 = 55.8 ms. "Met" is inside the noise. | Repeat, or label "at the limit". |
| N10 | sessions 200 cmd/s | Each transaction is stopped at once, active-session table near empty, DB size not recorded. | Test with 10–40k active sessions. |
| N11 | No CPU/memory limits in compose | "One instance" took as many VM cores as it wanted; N+1 sizing does not follow. | Set `cpus`/`mem_limit`, re-measure. |

Already disclosed by the project and not repeated: two different machines, load generator on the same machine,
the 99.9% rate rule added after the runs, reconnect storm with 199 stations instead of 833/s.

## Mandatory before production (M1–M14)

| # | Problem | Where |
|---|---|---|
| M1 | No TLS: stations use `ws://` with Basic auth | `infra/haproxy/haproxy.cfg:20` |
| M2 | Invoice has no VAT, net/gross, sequential number, party data; no credit notes or refunds | `services/billing/migrations/0001_init.sql:90`, `services/tariff/internal/pricing/quote.go:69` |
| M3 | Billed energy = `meterStop − meterStart` with no plausibility check; negative silently becomes 0; no signed meter data (Eichrecht) | `services/sessions/src/ocpp/ocpp.service.ts:233` |
| M4 | Time-of-use energy is apportioned by time in each window, not by metered energy | `services/tariff/internal/pricing/quote.go:84` |
| M5 | The hold does not gate charging: a declined card does not stop the session; nobody consumes `billing.events.v1` | `services/billing/src/sagas/saga-engine.ts:69` |
| M6 | Declined capture is terminal: no retry, no dunning, no admin API for parked sagas | `saga-engine.ts:447` |
| M7 | A session without StopTransaction stays open forever: connector occupied, hold never released | `ocpp.service.ts:109,172` |
| M8 | A session in `Stopping` is permanent when the command result is lost; the driver cannot retry | `services/sessions/src/driver/driver.service.ts:235`, `services/station-gateway/src/gateway.ts:421` |
| M9 | Site cap compliance is observed only through a simulator metric; no real site meter | `infra/prometheus/rules/slo-recording.yml:117` |
| M10 | A station that rejects a profile is only retried every 30 s; no escalation | `services/sessions/src/consumers/handlers.ts:167` |
| M11 | Postgres failover is a scripted `pg_promote`, no HA manager, no fencing | `drills/postgres-failover.sh:113` |
| M12 | Alerts go nowhere: no Alertmanager, no `alerting:` block | `infra/prometheus/prometheus.yml` |
| M13 | Readings replayed after more than 2 h offline never reach the 1-minute rollup | `services/telemetry-ingest/migrations/0002_power_1m.sql:15` |
| M14 | GDPR: no account deletion; payment tokens stored in plaintext in three columns | `services/billing/migrations/0003_capture_state.sql:7` |

## Should fix (S15–S28) — backlog, not in the plan

| # | Problem | Where |
|---|---|---|
| S15 | StartTransaction resent with a new uniqueId creates a second session and second hold (not reproduced) | `ocpp.service.ts:91` |
| S16 | Station command: at-most-once if the instance dies, duplicate on redelivery | `services/station-gateway/src/dispatcher.ts:83` |
| S17 | No per-station message rate limit | `services/station-gateway/src/station-connection.ts:108` |
| S18 | `stations.online` stays true after a gateway crash | `handlers.ts:41` |
| S19 | Unlimited handler retries block a partition | `packages/platform/src/kafka/consumer.ts:109` |
| S20 | Watts only; AC assumed 3-phase | `packages/contracts/src/fleet.ts:9` |
| S21 | Station timestamps not range-checked in telemetry | `services/telemetry-ingest/internal/ingest/mapping.go:114` |
| S22 | Telemetry offset committed after `synchronous_commit=off`; telemetry DB has no replica/backup | `internal/ingest/writer.go:20`, `docker-compose.yml:93` |
| S23 | Backup is `pg_basebackup` only, no WAL archive/PITR; no connection pooler | `drills/backup-restore.sh:54` |
| S24 | No Kafka broker, `up`/`absent`, Postgres, Redis alerts | `infra/prometheus/rules/slo-alerts.yml` |
| S25 | Sessions readiness depends on Kafka and Redis | `services/sessions/src/app.ts:165` |
| S26 | Migrations run at start under the app role | `packages/platform/src/postgres/postgres.ts:116` |
| S27 | Idle fee always 0; round-up per line | `quote.go:97,108` |
| S28 | No email verification / password reset; scrypt N=2^14 | `services/driver-api/src/auth.service.ts:53`, `password.ts:4` |

## Optional (O29–O36) — backlog

O29 staggered drain on SIGTERM (`gateway.ts:255`); O30 `lock_timeout` and TCP keepalive on pools; O31 DST tests for
Europe/Berlin; O32 reconcile config of existing Kafka topics; O33 move the 500 ms threshold of
`sessions-ocpp.js` into `targets.json`; O34 shared LSN tracker; O35 image scan and full-stack smoke test in CI;
O36 explicit HAProxy frontend `maxconn`.

## Scope decisions for the plan

- The plan covers N1–N11 (docs only, no re-measurement) and M1–M14.
- M2 is scoped to: gross prices with a VAT split, a gapless yearly invoice number, credit notes with a PSP refund.
  Seller/buyer party data on the document is out of scope (no PDF rendering).
- M3 Eichrecht is scoped to storing and exposing the station's signed meter data (OCMF strings) per session.
  Verifying signatures is out of scope (the driver uses transparency software).
- M9 is scoped to measuring site draw from station-reported power in telemetry and alerting on it. A dedicated
  site-meter integration (grid connection meter) is out of scope.
- M11 is scoped to an ADR decision only (Patroni + etcd recommended); the implementation is its own plan.
- Re-measuring N6–N11 is a separate plan; this plan only states the qualifications in the docs.
