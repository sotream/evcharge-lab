# smart-charging

Stateless, pure site power allocator (Go). `POST /v1/allocate` takes a site cap, its active
sessions, the number of idle connectors and `startW`, and returns one `limitW` per session.
No database; scales horizontally; the same request always gets the same answer.

Algorithm: start reserve for idle connectors, effective demand from measured power, admission by
(priority desc, startedAt asc), max-min fair water-filling, int64 W rounded down. Every response
asserts `sum(limitW) + reserveW <= siteCapW`; a violation returns 500 and increments
`invariant_violations_total` (must stay 0).

Governing ADRs: [0006](../../docs/adr/0006-smart-charging-allocation.md) (algorithm),
[0000](../../docs/adr/0000-requirements-and-capacity.md) (targets), 0010 (internal token).

## API

`POST /v1/allocate` with `x-internal-token`. Request/response: `AllocateReq` / `AllocateRes` in
`packages/contracts`. Optional `computedAt` fixes the reference time (default: server clock), which
keeps replays deterministic. Errors are `application/problem+json`: 400 `validation_failed`
(also `infeasible_reserve` when idle + active sessions at `startW` exceed the cap), 401, 413 for
bodies over 512 KiB, 500 `invariant_violation`. Also `/health/live`, `/health/ready`, `/metrics`.

Metrics: `allocations_total{outcome}`, `allocation_duration_seconds`, `allocation_sessions`,
`allocation_suspended_sessions_total`, `invariant_violations_total`.

Config: shared base only (`SERVICE_NAME`, `HTTP_PORT`, `INTERNAL_TOKEN`, `LOG_LEVEL`,
`OTEL_EXPORTER_OTLP_ENDPOINT`, `SHUTDOWN_GRACE`).

## Run and test

```sh
cd services/smart-charging
go test -race ./...                      # unit, table, property (10^5 sites), handler
INTEGRATION=1 go test -race ./cmd/...    # real HTTP server
go test -bench . ./internal/alloc        # algorithm benchmarks
docker build -f services/smart-charging/Dockerfile .   # from the repo root
```

## Load tool

`cmd/allocbench` drives the HTTP endpoint with generated sites of 10-80 connectors (saturation and
fixed-rate scenarios, checks the invariant client side). Raw output:
`docs/load-output/smart-charging-allocbench.json`.

```sh
INTERNAL_TOKEN=... go run ./cmd/allocbench -url http://127.0.0.1:28084 \
  -out ../../docs/load-output/smart-charging-allocbench.json
```
