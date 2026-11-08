# 0009. Toolchain and infrastructure choices

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: lab targets are measured on one laptop (Docker VM 8 GB, 8 CPU), so every component must be light enough to run the full stack plus load generators at once.

## Context

The stack is fixed: NestJS on Node, Go for computation, Postgres, Redis, a Kafka-compatible broker,
docker compose and pnpm workspaces. This ADR records the concrete choices inside that frame and the
versions current on 2026-11-08.

## Decision

| Concern | Choice | Version |
|---|---|---|
| Node runtime | Node | 26.13 |
| Node framework | NestJS + Fastify adapter | 12.4 |
| TypeScript | `tsc` | 7.0.2 (strict, `noUncheckedIndexedAccess`) |
| Node lint | oxlint | 1.92 |
| Node tests | vitest | 5.1 |
| Validation | zod | 4.6 |
| Kafka (Node) | `@platformatic/kafka` | 2.15 |
| Go | Go + `go.work` | 1.27.3 |
| Go lint | golangci-lint v2 (`.golangci.yml`) | 2.15 |
| Kafka (Go) | franz-go | latest |
| Postgres (Go) | pgx v5 | latest |
| Broker | Apache Kafka, KRaft, 3 combined broker+controller nodes, 512 MB heap each (ADR 0012) | 4.3.1 |
| OLTP database | Postgres | 18.6 |
| Telemetry database | TimescaleDB on Postgres 18, separate instance (ADR 0008) | 2.24.0-pg18 |
| Cache, registry | Redis | 8.10.3 |
| Edge LB | HAProxy | 3.4.7 |
| Tracing | OpenTelemetry → Jaeger v2 (OTLP/HTTP) | 2.23.0 |
| Metrics | Prometheus + Grafana | 3.16.0 / 13.3.0 |
| Load tests | k6 | local binary |

- **oxlint instead of ESLint.** typescript-eslint 8.72 refuses TypeScript 7, because the native
  compiler has no JS API for type-aware rules. Type safety comes from `tsc` strict mode. oxlint
  covers correctness and suspicious-pattern rules in milliseconds. Revisit this when
  typescript-eslint supports TS 7.
- **`@platformatic/kafka` instead of kafkajs.** kafkajs has not been released since 2023.
  `@confluentinc/kafka-javascript` needs native librdkafka builds in a multi-stage Alpine image.
  `@platformatic/kafka` is pure JS, actively maintained and supports idempotent producers.
- **Apache Kafka, not a Kafka-compatible substitute.** Replication, ISR, `min.insync.replicas`, leader election and
  consumer-group rebalancing are core subjects of this lab, and they behave exactly as in production only on
  Kafka itself. Running three KRaft nodes costs about 2 GB of the 8 GB VM. The previous lab (boxoffice-lab) already
  used Redpanda, so this one covers Kafka. ADR 0012 has the replication settings.
- **CommonJS output for Node packages.** NestJS decorator metadata and the OpenTelemetry
  require-hook auto-instrumentation are most reliable under CJS. ESM instrumentation still needs
  loader flags.
- **Images.** Node images use a multi-stage `node:26-alpine` build: `pnpm deploy --prod` of
  one package, then a slim runtime that runs as a non-root user. Go images build in
  `golang:1.27-alpine` with `CGO_ENABLED=0` and run on `gcr.io/distroless/static-debian12:nonroot`.
- **Checks** (superseded in part by ADR 0020: the same `make check` now also runs in GitHub Actions) ran locally only, through `make check`: install, contracts generation and drift check,
  topic sync, lint, typecheck, build, unit tests, then infra up, integration tests against the real
  Postgres (OLTP + telemetry), Redis and the 3-broker Kafka cluster, and `go vet`, golangci-lint and `go test -race` for every `go.work`
  module. At the time of writing there was no CI.

## Rejected alternatives

- **Redpanda (single node).** It starts faster and uses less memory, and it was the first choice in the draft of this
  ADR. It was rejected because a single node cannot show ISR shrink, `NotEnoughReplicas` or leader failover. A
  3-node Redpanda would show them, but this lab specifically targets Kafka semantics, and an earlier lab already
  used Redpanda.
- **TypeScript 5.x/6.x to keep typescript-eslint.** That would mean running an older compiler than
  the latest stable for the sake of a linter.
- **Biome.** It is comparable to oxlint. oxlint was picked for its vitest and import plugins.
- **Python for load or analysis scripts.** No third language is allowed. k6 runs the load, and the
  chart generation is Node.

## Consequences

- Type-aware lint rules (such as no-floating-promises) are not enforced by a linter. Code review
  checks for floating promises.
- All versions are pinned in compose and lockfiles. Upgrading is a deliberate change.
