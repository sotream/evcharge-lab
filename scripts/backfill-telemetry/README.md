# backfill-telemetry

Loads synthetic history straight into TimescaleDB with `COPY` and measures storage and query
behaviour. Governed by ADR 0013 (tool), ADR 0008 (schema) and ADR 0000 (fleet sizes).

- Fleet layout follows the shared convention in `packages/contracts/src/fleet.ts` (DC by the
  running-count rule, DC 50/75/100/150 kW by `g%4`, AC 11/22 kW by `g%2`, tariff ids, site caps,
  secrets). `internal/gen` TestFleetFixture rebuilds `packages/contracts/schemas/fleet-fixture.json`
  and compares every field, including `secretSha256`.
- Same physics as the simulator: AC flat power, DC with SoC taper after 80%, daily utilisation
  10% mean / about 39% evening peak (UTC hours), about 45 min AC sessions, one reading per active
  connector every 30 s. Ids: `st-<site#4>-<n#3>`, 10 stations per site.
- Applies the canonical `services/telemetry-ingest/migrations/*.sql` unchanged through the platform
  `pg.Migrate` runner (one transaction per file; a continuous aggregate created `WITH NO DATA` works
  inside a transaction).
- Then refreshes `power_1m`, compresses raw chunks and `power_1m` chunks older than 1 day (the
  `power_1m` policy itself compresses after 7 days; the 7-day backfill is too short for it to fire),
  and writes `docs/load-output/telemetry-backfill-<UTC>.json` with `measured` figures and a
  `derived` full-fleet extrapolation with its formulas.
- Refuses the shared `telemetry` database. The default target is `telemetry_backfill`; to get a
  clean measurement drop and recreate it first, so every migration is applied by the run.

    go run ./scripts/backfill-telemetry -stations 5000 -days 7 -workers 4 \
      -dsn postgres://evcharge:evcharge@localhost:25433/telemetry_backfill

Run it from the repository root (the migrations path and output path are relative to it).

Tests: `go test -race ./...`. Integration (10 stations x 2 h, plus the compression failure test):
`INTEGRATION=1 go test ./...`.
