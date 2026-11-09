# Development

## Repo map

```
docs/
  adr/                 ADRs and the index: requirements, architecture, contracts, per-service decisions
  results.md           measurement setup, how to read the verdicts, generated charts block
  metrics.md           generated: the full metrics table
  drills.md            generated: summary and every drill run with its timeline chart
  findings.md          what the drills, load tests and reviews caught (hand-written items, generated table)
  guarantees.md        the distributed-systems guarantees and the drill behind each
  known-limits.md      known limits and production gaps
  runbook.md           start, stop, signals worth alerting on, failure recipes, known gaps
  load-output/         raw k6 summaries (+ .env.json setup records) and tool outputs: the source of every measured number
  drill-output/        raw failure-drill output: timeline.jsonl, events.log, result.json per run
  img/                 architecture.svg (hand-made) and the generated charts
services/
  station-gateway/     NestJS: OCPP 1.6J WebSocket edge, routing, offline replay
  sessions/            NestJS: catalog, reservations, session state machine, allocation rounds
  billing/             NestJS: saga, mock PSP, invoices
  driver-api/          NestJS: driver authentication and the public API
  smart-charging/      Go: water-filling allocator, allocbench tool
  tariff/              Go: deterministic pricing, quotebench tool
  telemetry-ingest/    Go: Kafka to TimescaleDB, batched COPY
simulator/             Node: OCPP station fleet, demand and physics, control API
packages/
  contracts/           TypeScript contracts, event schemas, shared fleet convention
  contracts-go/        Go side of the contracts
  platform/            shared NestJS building blocks: consumers, outbox, HTTP client, metrics
  platform-go/         shared Go building blocks
load/                  k6 scripts, run.sh (records the setup next to each summary) and targets.json
drills/                failure drills: broker loss, gateway loss, reconnect storm, slow Postgres, allocator down, station offline replay
scripts/
  charts/              generator for the charts, the metrics table, the drills page and the generated README blocks (plain Node, no dependencies)
  backfill-telemetry/  Go: history straight into TimescaleDB, compression and query measurements
  check-topics.mjs     checks that the Kafka topic list in code and in infra/kafka/topics.sh agree
infra/                 compose configuration: HAProxy, Kafka topics, Postgres init, Prometheus, Grafana
docker-compose.yml     the whole stack
Makefile               make check, make up, make down
```


## Regenerating the charts and the generated tables

```sh
node scripts/charts/build.mjs
node --test scripts/charts/     # generator tests; also run by make check
```

This rewrites `docs/img/*.svg`, `docs/metrics.md`, `docs/drills.md` and the generated blocks in the README and the pages under `docs/` (stats, targets at a glance, charts, drills table, findings table) from the raw files only. See [`scripts/charts/README.md`](../scripts/charts/README.md).
