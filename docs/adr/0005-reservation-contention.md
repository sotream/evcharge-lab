# 0005. Reservations under contention

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: "zero double bookings of one connector; reservation TTL 15 min"; driver writes 50 RPS peak; sessions ≥ 200 commands/s per instance; sessions retained 10 years (~117 GB/year).

## Context

Popular connectors get several reservation attempts within milliseconds of each other. Exactly one
may win. A reservation also has to be confirmed by the station (`ReserveNow`), it expires, and a
driver who taps "reserve" twice, or whose client retries on a timeout, must not hold two
reservations.

## Decision

- **The database enforces exclusivity.** `reservations` carries a generated `tstzrange(starts_at,
  expires_at)` and an exclusion constraint:
  `EXCLUDE USING gist (connector_pk WITH =, period WITH &&) WHERE (status IN ('Pending','Active'))`
  (btree_gist). A concurrent loser gets `23P01` (exclusion_violation), which maps to HTTP 409
  `connector_unavailable`. The constraint is the guarantee and there is no read-then-write race. An advisory lock per connector is taken before the insert only so concurrent losers queue instead of deadlocking (see "Concurrent inserts deadlock" below); it adds no correctness of its own.
- **One open reservation per driver.** A partial unique index on `(driver_id) WHERE status IN
  ('Pending','Active')` enforces it.
- **TTL.** Every reservation lasts 15 minutes (ADR 0000), so `expires_at = now() + 15 min`, and the request has no duration field.
- **Retries are idempotent.** `POST /v1/reservations` requires `idempotency-key`. The same key
  returns the original reservation.
- **Station confirmation.** The row is inserted as `Pending` together with an outbox
  `ReserveNow` command (`station.commands.v1`, expiry 30 s) in one transaction.
  `station.command-completed` changes the status to `Active` on `Accepted`, or to `Rejected` on
  any other outcome, which releases the range.
  - If the station is offline, the outcome is `StationOffline`, the reservation becomes
    `Rejected`, and the driver can see that.
- **Expiry.** A sweeper runs every 5 s:
  `UPDATE ... SET status='Expired' WHERE status IN ('Pending','Active') AND expires_at < now()
  RETURNING ...`, using `FOR UPDATE SKIP LOCKED` batches so that several `sessions` replicas can
  sweep safely. Each expired row emits `reservation.ended`.
- **Use.** A `StartTransaction` that carries a `reservationId`, or that comes from the reserving
  driver's idTag on that connector, marks the reservation `Used` in the same transaction that
  creates the session.
- A connector that is `Charging` or `Faulted` per its last StatusNotification is rejected up front
  with 409, which is cheaper than a round trip to the station.

## Rejected alternatives

- **Redis `SET NX` lock per connector.** This is fast, but it is a second source of truth. It
  needs fencing against lock expiry during a slow transaction, and it still needs the database
  write. The exclusion constraint is atomic with the write itself.
- **`SELECT ... FOR UPDATE` on the connector row, then insert.** This is correct but serializes
  every reservation on a connector through a row lock, and it relies on every code path taking
  the lock. A constraint cannot be forgotten.
- **SERIALIZABLE isolation.** This is correct, but it produces retry storms under contention. The
  constraint fails only the true losers.

- **Sizing.** At 50 driver writes/s at peak, against a target of at least 200 commands/s per `sessions` instance, the GiST exclusion check is an index probe on one connector's handful of rows. Contention is per connector, so it does not grow with fleet size.
- **Concurrent inserts deadlock (found in November 2026).** Two transactions inserting into the exclusion-constrained table for the same connector at the same moment each find the other's uncommitted row and wait for it, so PostgreSQL aborts one with `40P01` after `deadlock_timeout` (1 s). In the 50-concurrent-attempts integration test this showed up in 2 of 40 runs as a stall and 49 HTTP 500s, because the waiters held pool connections (observed while fixing PR #10; that raw output was not saved, and the stalls seen later with k6 were 6 to 10 s, not 3 to 6 s). The saved evidence is the two k6 series in `docs/load-output/` (`contention-n50-unfixed-build-*`: 28 of 30 runs valid, 12 deadlocks in the Postgres log, stalls of 10,021 ms and 6,018 ms; `contention-n50-fixed-build-*`: 30 of 30 valid, 0 deadlocks). `createReservation` now takes `pg_advisory_xact_lock` on the connector first, so inserts for one connector are serialized and the losers get `23P01` straight away (100 of 100 integration-test runs, slowest request 83 ms, observed and not saved; the saved k6 series above has slowest requests of 26 to 60 ms). The lock is not what makes the guarantee: the constraint still decides, and a code path that skips the lock gets a deadlock or a 409, never a double booking. This is not the rejected row-lock design above, which would have made the lock the only protection.
- **A late `Accepted` is cancelled on the station (found in the November 2026 review).** If the station answers `ReserveNow` after the sweeper expired the command and rejected the reservation (Kafka or the gateway stalled for more than 40 s), the station holds the connector for 15 minutes for a reservation sessions no longer knows. `commandResult` now sends a `CancelReservation` for that case (test: "a late Accepted does not revive it but cancels it on the station").
- **Retention.** Reservations and sessions are kept for 10 years (ADR 0000) in monthly range partitions. The exclusion constraint is defined per partition on `starts_at`. A reservation never spans more than 15 minutes, so a range crossing a month boundary is checked against both partitions by a constraint trigger on the boundary rows.

## Consequences

- The load test hits one connector with N concurrent virtual users and must show exactly one 201
  and N-1 409s, plus the p99 of that path. Results are in `docs/load-output/`.
- The reservation lifecycle depends on command results arriving. If the gateway or broker is down,
  reservations stay `Pending` until the 30 s command expiry produces `Expired` or
  `StationOffline`, which rejects them.
