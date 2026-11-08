# tariff

Deterministic session pricing (Go). Governed by ADR 0011 (model, rounding, TOU split),
ADR 0000 (target at least 2,000 quotes/s, p99 under 20 ms) and ADR 0010 (internal token).

- `POST /v1/quote` (QuoteReq to QuoteRes) and `GET /v1/tariffs/:id`; both need `x-internal-token`.
- Versioned tariffs in Postgres database `tariff` (`migrations/`, seeded std/tou/fleet), cached in
  process for 60 s. During a database outage an expired entry is still served for up to 10 more
  minutes.
- `/health/live`, `/health/ready`, `/metrics`. Readiness is cache-aware: with Postgres down an
  instance stays ready while its cache is warm (it can still price), and is not ready when the
  cache is cold.

## Publishing a new tariff version

Versions are immutable. A new version is an INSERT into `tariff_versions` whose `valid_from` is at
least 15 minutes in the future (migration 0003 trigger, SQLSTATE 23514 otherwise). 15 minutes is
longer than the cache TTL (60 s) plus the stale retry spacing, so every instance has reloaded the
tariff before the version takes effect and all instances price a session the same way. The seed
migration (0002) runs before the trigger exists and is exempt.

## Run and test (infra already up)

    go test -race ./...                                   # unit, golden and property tests
    INTEGRATION=1 TEST_PG_URL=postgres://evcharge:evcharge@localhost:25432 go test ./...

## Load measurements

`cmd/quotebench` writes the raw output to `docs/load-output/`. Which file answers the ADR 0000
target (2,000 quotes/s with p99 under 20 ms):

- `tariff-quotebench-openloop.json` answers it. It offers a fixed 2,000 quotes/s on a schedule and
  measures latency from the scheduled send time, against the server run as recorded in the file's
  `server` block (mode, CPU limit, concurrency).
- All three files were produced against the service run as a container with `--cpus 2` and
  `DB_MAX_CONNS=8`, with Postgres on the host and the load generator on the same machine.
- `tariff-quotebench.json` (32 closed-loop clients) and `tariff-quotebench-c8.json` (8 clients) are
  saturation measurements: they show maximum throughput and the latency under that load, not
  the latency at the target rate. Measured: at concurrency 32 the service reached 14,399.8 quotes/s with p99 7.957 ms,
  and at concurrency 8 it reached 9,095.7 quotes/s with p99 4.014 ms; both p99 values are inside the 20 ms target
  (`results[0].latencyMs.p99` in each file, `pass: true`). The open-loop file shows p99 13.898 ms at the offered
  2,000 quotes/s.
