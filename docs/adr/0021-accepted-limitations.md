# 0021. Accepted limitations

- Status: accepted
- Date: 2026-11-09
- Context: the November 2026 review listed findings that were checked and left as they are. Each one is a deliberate
  trade-off of a lab, not an oversight. None changes a number in the README. If any becomes a requirement, the "Revisit
  when" line says what to build.

## Decision

**Login limiter keyed by email (driver-api).** `AuthService.login` counts every attempt per IP and per email, whether the
password was right or not. Anyone can therefore lock a known email out for the limiter window by sending wrong passwords.
Accepted: the window is short and a lockout does not expose data. Revisit when accounts are real: key the email bucket
by (email, IP), or count only failures.

**Station credential revocation takes up to about 5.5 minutes (station-gateway).** Credentials are cached in an LRU for
30 s on top of a 5 minute Redis TTL, and an open socket is not re-checked. A revoked station keeps working until the cache
expires and, if connected, until it reconnects. Accepted: the cache is what keeps `sessions` off the connect path
(ADR 0003). Revisit when: revocation must be immediate; publish a revoke message on the gateway command channel.

**Read-your-writes is not monotonic across the primary fallback (sessions, driver-api).** The fallback read from the primary does not
return an LSN, so a following read can be served by a replica that is still behind (ADR 0017); the review also noted that
`read-router` waits until its deadline when the replay LSN is NULL, and no test measures that wait. Accepted: the read
after a write is correct; a second read is correct once the replica caught up, which the lag alert watches. Revisit when: return the primary LSN with the fallback answer.

**Billing keeps one currency per invoice and does not release the remainder of a partial capture.** The hold is placed in
`HOLD_CURRENCY`, the capture is made in the tariff currency, and a capture below the hold marks the hold released without
voiding the rest at the PSP. Only EUR tariffs are seeded, and the PSP is a mock. Both points come from reading the code (`saga-engine.ts`); neither
was reproduced. Revisit when: a second
currency or a real PSP is added; void the remainder explicitly and reject a currency mismatch (ADR 0007).

**Outbox order is bounded by the xid hold-back, not by a lock (platform).** The relay publishes rows whose transaction
finished before every running one. A transaction that waits on a row lock is already in that set, so rows can be held back
for as long as it waits, and the hold-back is cluster-wide (ADR 0002 erratum). A `pg_advisory_xact_lock` taken before the
first write would assign no xid and close the window; it was not built because it serialises every writer that emits an
event. Revisit when: ordering across keys during long lock waits matters more than write concurrency.

**Go `kafka.Consumer` has no dead-letter topic and a shared backoff (platform-go).** One failing partition slows the others
because the retry backoff is per poll. No service uses it: telemetry-ingest has its own pipeline with bisecting and a DLQ
(ADR 0008). Revisit when: a Go service adopts it; add a per-record attempt limit like `isDataError` in the TypeScript consumer.

**telemetry-ingest readiness includes Kafka, Redis and Postgres.** The review noted that reads work without Kafka or Redis.
Readiness here means "can do its whole job", which includes ingesting. Kept as it is.

**Open-reservation uniqueness per driver across monthly partitions (sessions).** The review reported that the uniqueness
does not hold between monthly partitions for rows near a month boundary. It was not reproduced, and the invariant drill
(ADR 0019) does not exercise a month boundary. Revisit when: reservations may span months; move the guard to a
non-partitioned table.

**The k6 runs from before the rewrite keep their incomplete env records.** Only the gateway series was rerun. The other
runs were not repeated through the current `run.sh`; the README labels which env records are incomplete. Repeating them
changes no conclusion and costs hours on the measurement laptop.

**Redis has no password and compose secrets have defaults.** Every published port is bound to `127.0.0.1` unless `BIND` is
set, and the runbook says what to change before binding wider. Production would use ACLs and mandatory secrets.

**Dockerfile base images are not pinned by digest, and the load history generator has no seed.** Dependabot proposes base
image updates weekly (ADR 0020). `generate.mjs` uses `Math.random`, so a regenerated history differs from the saved one.
Revisit when: images are published or someone needs a byte-identical history.

## Consequences

- The README "Known limits" section points here, so a reader sees these as decisions rather than finds.
- Each item above is a candidate, in the order a real deployment would need it: revocation, login limiter, billing
  currency and remainder, then the rest.
