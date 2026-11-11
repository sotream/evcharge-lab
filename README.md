<h1 align="center">evcharge-lab</h1>

<p align="center">
  <b>The charge point operator side of an EV charging platform, built as a system-design lab.</b><br>
  Hold thousands of station WebSockets, take reservations without double-booking, split a site power cap, bill exactly once, absorb telemetry bursts.
</p>

<p align="center">
  <a href="https://github.com/sotream/evcharge-lab/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/sotream/evcharge-lab/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/sotream/evcharge-lab/actions/workflows/images.yml"><img alt="images" src="https://github.com/sotream/evcharge-lab/actions/workflows/images.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow"></a>
  <img alt="Node 26" src="https://img.shields.io/badge/Node-26-5FA04E?logo=nodedotjs&logoColor=white">
  <img alt="TypeScript 7" src="https://img.shields.io/badge/TypeScript-7-3178C6?logo=typescript&logoColor=white">
  <img alt="NestJS 12" src="https://img.shields.io/badge/NestJS-12-E0234E?logo=nestjs&logoColor=white">
  <img alt="Go 1.27" src="https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white">
  <img alt="PostgreSQL 18" src="https://img.shields.io/badge/PostgreSQL-18-4169E1?logo=postgresql&logoColor=white">
  <img alt="TimescaleDB 2.31" src="https://img.shields.io/badge/TimescaleDB-2.31-FDB515?logo=timescale&logoColor=black">
  <img alt="Apache Kafka 4.3 (KRaft)" src="https://img.shields.io/badge/Apache%20Kafka%20(KRaft)-4.3-231F20?logo=apachekafka&logoColor=white">
  <img alt="Redis 8" src="https://img.shields.io/badge/Redis-8-FF4438?logo=redis&logoColor=white">
  <img alt="OCPP 1.6J" src="https://img.shields.io/badge/OCPP-1.6J-0A7E8C">
  <img alt="k6" src="https://img.shields.io/badge/load%20tests-k6-7D64FF?logo=k6&logoColor=white">
</p>

<p align="center">
  <a href="#architecture">Architecture</a> &middot;
  <a href="#5-minute-quick-start">Quick start</a> &middot;
  <a href="#the-system-design-question-each-service-answers">Services</a> &middot;
  <a href="#results">Results</a> &middot;
  <a href="#more">More</a> &middot;
  <a href="docs/adr/README.md">ADRs</a> &middot;
  <a href="docs/runbook.md">Runbook</a>
</p>

> **Status:** a learning and design lab, not a production system. Its known gaps are listed in [known limits](docs/known-limits.md).

<!-- stats:start -->
| 7 services | 23 ADRs | 12 drill scripts | 19 of 20 drills pass on the latest run | 23 of 23 findings fixed |
|:---:|:---:|:---:|:---:|:---:|
| [NestJS and Go, plus a station simulator](#the-system-design-question-each-service-answers) | [one decision per file](docs/adr/README.md) | [failure drills](docs/drills.md) | [15 failed runs kept as evidence](docs/drills.md) | [found by drills, k6 and reviews](docs/findings.md) |
<!-- stats:end -->

## Architecture

evcharge-lab is the charge point operator (CPO) side of an EV charging platform, built as a system-design lab. Seven services (NestJS and Go) and an OCPP 1.6J station simulator answer concrete questions: how to hold thousands of long-lived WebSockets across several instances, how to take reservations under contention without double-booking, how to split a hard site power cap fairly and safely, how to get an exactly-once billing effect out of at-least-once delivery, and how to absorb a bursty telemetry stream. Every decision is written down in an [ADR](docs/adr/README.md) against explicit load targets ([ADR 0000](docs/adr/0000-requirements-and-capacity.md)). The numbers in the generated tables come from raw files in `docs/load-output/` and `docs/drill-output/`, and a target that was missed stays visible as missed.

![architecture](docs/img/architecture.svg)

Solid blue lines are synchronous HTTP requests, dashed purple lines are asynchronous Kafka event flows, thick teal lines are the station WebSockets, and dotted grey lines are data access. The decisions behind it are in [ADR 0001](docs/adr/0001-architecture-overview.md); the ports are in the same ADR and in [`docker-compose.yml`](docker-compose.yml).

## 5-minute quick start

Prerequisites: Docker with a VM of about 8 GB (the k6 runs below used 8 CPUs and 8.3 GB, the later drills 8 CPUs and 11.7 GiB; see [`docs/results.md`](docs/results.md)), plus Node 26 and pnpm for `make check`. Go and `golangci-lint` are also required for `make check`, because it runs `go vet`, `golangci-lint run` and `go test -race` on the Go modules.

```sh
docker compose up -d --build      # infra, services, HAProxy, observability and the simulator
docker compose ps                 # infrastructure shows (healthy); the app services show Up (they have no healthcheck, see docs/runbook.md)
curl -s 127.0.0.1:28089/status    # simulator: connected stations, active sessions, queue depth
curl -s localhost:28081/health/ready   # any service: ports are listed below
```

By default the simulator connects 20 sites with 10 stations each (200 stations) and 50 simulated drivers, so sessions, meter values and allocation rounds start flowing immediately.

| What | URL | Notes |
|---|---|---|
| Driver API | http://localhost:28082 | the only public entry point; `POST /v1/auth/register`, then the routes in [its README](services/driver-api/README.md) |
| Station edge (OCPP) | ws://localhost:28080/ocpp/{stationId} | HAProxy `leastconn` in front of two gateway instances |
| HAProxy stats | http://localhost:28099 | backend health of the two gateways |
| Simulator control | http://127.0.0.1:28089/status | bound to localhost, unauthenticated |
| Grafana | http://localhost:23000 | anonymous viewer; Prometheus and Jaeger datasources and six dashboards in the `evcharge` folder are provisioned from [`infra/grafana/dashboards/`](infra/grafana/dashboards/): `overview` (the ADR 0000 SLOs, open it first), `gateway`, `sessions-allocation`, `billing`, `telemetry` and `kafka-and-infra` |
| Prometheus | http://localhost:29090 | queries used by the alerts in the [runbook](docs/runbook.md) |
| Jaeger | http://localhost:26686 | traces, 10% parent-based sampling |
| Services | `localhost:28081` sessions, `28083` billing, `28084` smart-charging, `28085` tariff, `28086` telemetry-ingest, `28087` and `28088` gateways | each serves `/health/live`, `/health/ready` and `/metrics` |

Run the full local check (lint, types, build, unit and integration tests, Go vet, lint and race tests):

```sh
make check
```

`make check` (through `make infra-up`) stops the application containers first, because the integration tests share Redis and Kafka with the stack; the infrastructure keeps running. Run `docker compose up -d` afterwards to bring the application containers back. `make down` stops everything and deletes the data. Operating notes and failure recipes are in the [runbook](docs/runbook.md).

## The system-design question each service answers

| Service | Language | System-design question | ADR |
|---|---|---|---|
| [`station-gateway`](services/station-gateway/README.md) | NestJS | How do you hold thousands of long-lived WebSockets across N instances and route a command to the one instance that owns a station? How do you survive reconnect storms and offline replay? | [0003](docs/adr/0003-ocpp-subset-station-auth-and-offline-replay.md), [0004](docs/adr/0004-websocket-connection-routing.md), [0012](docs/adr/0012-kafka-replication-and-producer-backpressure.md) |
| [`sessions`](services/sessions/README.md) | NestJS | How do you take reservations under contention without double-booking a connector? How do you run session state machines driven by unreliable station messages? | [0005](docs/adr/0005-reservation-contention.md) |
| [`smart-charging`](services/smart-charging/README.md) | Go | How do you split a hard site cap across sessions fairly, fast and safely, and what happens when the allocator is down? | [0006](docs/adr/0006-smart-charging-allocation.md) |
| [`tariff`](services/tariff/README.md) | Go | How do you price a session deterministically (integer money, time-of-use boundaries, rounding)? | [0011](docs/adr/0011-tariff-model.md) |
| [`billing`](services/billing/README.md) | NestJS | How do you get an exactly-once *effect* (one invoice, one capture) out of at-least-once delivery, and how do you compensate across services? | [0007](docs/adr/0007-billing-saga-and-exactly-once-effect.md) |
| [`telemetry-ingest`](services/telemetry-ingest/README.md) | Go | How do you absorb a high, bursty write rate of meter values with dedup and bounded memory? | [0008](docs/adr/0008-telemetry-storage.md), [0012](docs/adr/0012-kafka-replication-and-producer-backpressure.md) |
| [`driver-api`](services/driver-api/README.md) | NestJS | How do you authenticate drivers and compose calls to downstream services with timeouts, retries and idempotency? | [0010](docs/adr/0010-authentication.md) |
| [`simulator`](simulator/README.md) | Node | How do you produce realistic load from thousands of stations, including floods, offline periods and replays? | [0003](docs/adr/0003-ocpp-subset-station-auth-and-offline-replay.md), [0013](docs/adr/0013-traffic-generators-simulator-and-backfill.md) |

Cross-cutting decisions (requirements, architecture, contracts, toolchain) are in the [ADR index](docs/adr/README.md).

## Results

One instance per service on one machine; the targets come from [`load/targets.json`](load/targets.json), transcribed from [ADR 0000](docs/adr/0000-requirements-and-capacity.md). Every verdict compares the measured value with the target with no tolerance, except the rate rule for k6 throughput, so a missed target stays visible as missed. One machine ran both the stack and the load generator; see [known limits](docs/known-limits.md). Each number is one short run (30–60 s) on one machine; the driver-api read figure is a cache-path number and 10,000 connections were never measured together with 2,000 msg/s. Setup, how to read the verdicts and the charts: [`docs/results.md`](docs/results.md). The full table with sources: [`docs/metrics.md`](docs/metrics.md).

<!-- glance:start -->
| Service | Metric | Target | Measured | Verdict | Source |
|---|---|---|---|---|---|
| station-gateway | WebSocket connections | ≥ 10,000 | 10,000 | ✅ met | [`gateway-10k-connections.json`](docs/load-output/gateway-10k-connections.json) |
| station-gateway | Messages per second | ≥ 2,000 /s | 2,490.23 /s | ✅ met | [`k6-gateway-ws-2000st-1.25hz.json`](docs/load-output/k6-gateway-ws-2000st-1.25hz.json) |
| station-gateway | CALLRESULT p99 | ≤ 500 ms | 253 ms | ✅ met | [`k6-gateway-ws-2000st-1.25hz.json`](docs/load-output/k6-gateway-ws-2000st-1.25hz.json) |
| telemetry-ingest | Sustained rows per second (2,500 offered) | ≥ 2,500 /s | 2,471.23 /s | ❌ missed | [`telemetry-ingestbench.json`](docs/load-output/telemetry-ingestbench.json) |
| telemetry-ingest | Durable lag p99 (at the target rate) | ≤ 5,000 ms | 942.95 ms | ✅ met | [`telemetry-ingestbench.json`](docs/load-output/telemetry-ingestbench.json) |
| telemetry-ingest | Telemetry visible in driver-api p99 | ≤ 10,000 ms | no measurement | not tested | no saved run |
| telemetry-ingest | Loss ratio at the target rate | ≤ 0.0001 | 0 | ✅ met | [`telemetry-ingestbench.json`](docs/load-output/telemetry-ingestbench.json) |
| sessions | Commands per second (StartTransaction + StopTransaction) | ≥ 200 /s | 199.99 /s | ✅ met (rate rule) | [`k6-sessions-ocpp-rate200.json`](docs/load-output/k6-sessions-ocpp-rate200.json) |
| smart-charging | Allocations per second (k6, service) | ≥ 1,000 /s | 999.97 /s | ✅ met (rate rule) | [`k6-smart-charging.json`](docs/load-output/k6-smart-charging.json) |
| smart-charging | Allocation p99 (open loop at the target rate) | ≤ 20 ms | 19.45 ms | ✅ met | [`smart-charging-allocbench.json`](docs/load-output/smart-charging-allocbench.json) |
| tariff | Quotes per second (k6, service) | ≥ 2,000 /s | 1,999.97 /s | ✅ met (rate rule) | [`k6-tariff.json`](docs/load-output/k6-tariff.json) |
| tariff | Quote p99 (open loop at the target rate) | ≤ 20 ms | 13.9 ms | ✅ met | [`tariff-quotebench-openloop.json`](docs/load-output/tariff-quotebench-openloop.json) |
| driver-api | Read requests per second (GET /v1/sites) | ≥ 1,000 /s | 955.59 /s | ❌ missed | [`k6-driver-api-reads.json`](docs/load-output/k6-driver-api-reads.json) |
| driver-api | Read p99 | ≤ 300 ms | 255.2 ms | ✅ met | [`k6-driver-api-reads.json`](docs/load-output/k6-driver-api-reads.json) |
| driver-api | Write p99 | ≤ 800 ms | no measurement | not tested | no saved run |
<!-- glance:end -->

## More

| Page | What is in it |
|---|---|
| [`docs/results.md`](docs/results.md) | measurement setup, how to read the verdicts, charts |
| [`docs/metrics.md`](docs/metrics.md) | every metric with its source file, formula and note |
| [`docs/drills.md`](docs/drills.md) | the failure drills, every run with its timeline |
| [`docs/findings.md`](docs/findings.md) | what the drills, load tests and reviews caught |
| [`docs/guarantees.md`](docs/guarantees.md) | the distributed-systems guarantees and the drill behind each |
| [`docs/known-limits.md`](docs/known-limits.md) | known limits and production gaps |
| [`docs/runbook.md`](docs/runbook.md) | operating the stack, alerts, failure recipes |
| [`docs/development.md`](docs/development.md) | repo map, regenerating the charts and tables |
| [`docs/adr/`](docs/adr/README.md) | one decision per file |
| [`docs/de/`](docs/de/) | German translations: development, guarantees, results, ADR 0000 and 0001 |

## CI and license

Pull requests and pushes to `main` run `make check`, a gitleaks scan of the full history and the image builds in GitHub Actions (no registry publishing); drills and load tests stay local because their numbers belong to the measurement laptop ([ADR 0020](docs/adr/0020-ci-on-github-actions.md)). Licensed under the [MIT License](LICENSE).
