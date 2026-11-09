# 0019. Checking the reservation invariant from an operation history, under faults

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000 ("Reservations: zero double bookings of one connector") and ADR 0005 (the database decides: an exclusion constraint on `(connector_pk, period)` for Pending and Active rows). ADR 0017 lists the guarantee as "Serializable outcome for one connector's time range". A guarantee that is only argued in a document has not been tested. This ADR tests it from the outside, while faults are injected, and states what the test does not prove.

## Context

The reservation path crosses a driver, driver-api (with a circuit breaker), sessions and Postgres, and each hop can time out, restart or fail over. After a fault a client may not know whether its request took effect. Two properties must survive that:

1. **No double booking.** No connector has two reservations that both held it at the same time.
2. **No lost acknowledgement.** Every reservation that was acknowledged to a client still exists after recovery.

## Decision

### The history

`load/history/generate.mjs` runs N virtual drivers (default 12) against a small pool of idle connectors taken from `GET /v1/sites` (default 6), so that they contend on purpose. Each driver reserves a random connector, holds it 0.5 to 3 s and cancels it; a cancel that fails is retried every 500 ms, otherwise the driver would keep its reservation for the full 15 minutes. After a refused reservation a driver waits 0.4 to 1.2 s; a version without that think time hammered six connectors from 20 drivers, produced lock waits, deadlocks and an open circuit breaker before any fault, and its runs were discarded.

Every operation is one JSON line in `history.jsonl`: client op id, driver, type (`reserve` or `cancel`), station and connector, `invokeMs` and `completeMs` taken on the client around the request, and an outcome:

- `ok`: definitely accepted (2xx).
- `rejected`: a definite business refusal (4xx other than 408 and 429, for example 409 `connector_unavailable`).
- `error`: timeout, 5xx, 429 or network failure. The outcome is unknown, and it is never treated as a refusal.

### The checker

`load/history/check.mjs` takes the history and a dump of the `reservations` table (joined with `connectors`, including `ended_at`) taken from the database that is live after recovery (the dump of `reservation-invariant.sh` and, since the rerun, of `postgres-failover.sh` includes `ended_at`; the first failover runs did not and used the fallback below). It exits 0 for a pass, 1 for a violation, 2 for bad input. It has unit tests (`load/history/check.test.mjs`, 19 tests, run by `make check` through `make history-test`).

1. **No overlap.** For each acknowledged reservation the interval during which it held the connector comes from the database row: `[starts_at, min(expires_at, ended_at))`. Two acknowledged reservations on one connector whose intervals overlap are a violation. The row decides because a 201 only means Pending: the station can reject the reservation moments later (status `Rejected`, `ended_at` set, tens of milliseconds later in the data I looked at), and a cancel sets `ended_at` too. A first version of the checker took the end from the client's view (expiry or the client's cancel) and reported 33 "violations" in a development run with no fault (that run was not kept), all of the kind "acknowledged Pending, then rejected by the station, connector free again". The rows showed it (`Rejected`, `ended_at` 50 ms after `starts_at`). The checker was wrong, not the system. Without `ended_at` in the dump (older input) it falls back to the client's cancel as the end.
2. **No lost acknowledgement.** Every `ok` reserve has a row with the same reservation id, the same connector and the same `starts_at` (within 1 ms). Missing, on another connector, or with a different start (the promoted node issued the id again to another reservation) means "acknowledged reservation lost". Matching by id and connector alone, as the first version did, hides a lost reservation whose id is reissued on the same connector.
3. **Unknown outcomes.** An `error` reserve that has a row on its connector whose `starts_at` lies within one second of `[invokeMs, completeMs]` is matched to that row, so that a request that took effect after the client gave up still takes part in the overlap check through its row. Unmatched unknowns are counted and never produce a violation by themselves.
4. **Overlaps with a lost reservation.** A lost acknowledged reservation has no row, so its end is the client's view (its expiry or its acknowledged cancel). An acknowledged reservation that overlaps it is reported, one pair at a time: one lost reservation can produce many pairs (8 lost reservations gave 32 pairs in the async failover run).

The report always ends with: "This is not a full linearizability check: it proves non-overlap of acknowledged intervals and durability of acknowledged reservations only."

### The mutation test

A checker that never fails proves nothing. `drills/reservation-invariant.sh` with `MUTATE=1` drops the exclusion constraint on every reservation partition before the run, so the protection is gone, runs the same load, and passes only if the checker goes red. Afterwards it ends every open reservation and puts the constraints back (and checks that the count is restored).

### The faults

- `FAULT=pause`: `docker pause` of the sessions container for 15 s (requests hang, clients time out, then everything resumes).
- `FAULT=restart`: `docker restart` of sessions (in-flight requests are cut).
- Kill of the primary plus promotion: the same history and checker run inside every `drills/postgres-failover.sh` run (ADR 0018).
- `FAULT=none`: a control run.

### Result

All runs under `docs/drill-output/`, machine in each `env.json`. "Ack" is acknowledged reserves, "indet." is reserves with an unknown outcome and in brackets how many of those were found in the database.

| Run | Fault | Ops | Ack | Rejected | Indet. | Checker |
|---|---|---|---|---|---|---|
| `reservation-invariant-none-20261108T205643Z` | none | 954 | 183 | 588 | 0 | PASS |
| `reservation-invariant-pause-20261108T205154Z` | sessions paused 15 s | 1,123 | 121 | 647 | 116 (3) | PASS |
| `reservation-invariant-restart-20261108T205335Z` | sessions restarted | 1,065 | 153 | 581 | 100 (0) | PASS |
| `reservation-invariant-none-mutated-20261108T205511Z` | **protection disabled** | 883 | 433 | 17 | 0 | **FAIL, 292 violations** (5 constraints dropped, 5 restored) |
| `reservation-invariant-pause-20261109T090014Z` | sessions paused 15 s, build with the advisory lock (ADR 0005) | 1,125 | 135 | 634 | 109 (2) | PASS |
| `reservation-invariant-restart-20261109T090135Z` | sessions restarted, same build | 1,123 | 183 | 570 | 93 (0) | PASS |
| `reservation-invariant-none-mutated-20261109T090303Z` | **protection disabled**, same build | 920 | 460 | 0 | 0 | **FAIL, 315 violations** (5 constraints dropped, 5 restored) |
| `postgres-failover-auto-async-stall0-20261108T203614Z` | primary killed, replica promoted | 1,535 | 273 | 803 | 90 (0) | PASS |
| `postgres-failover-auto-async-stall3-20261108T220826Z` | same, replica unreachable 3 s before | 1,528 | 191 | 968 | 88 (0) | **FAIL, 42 violations: 10 lost acknowledged reservations, 32 overlapping pairs (each with a lost reservation on one side)** |
| `postgres-failover-auto-sync-stall0-20261108T204337Z` | same, synchronous standby | 1,316 | 264 | 776 | 8 (0) | PASS |
| `postgres-failover-auto-sync-stall3-20261108T221218Z` | same, sync, 3 s partition | 1,552 | 277 | 801 | 96 (0) | PASS |
| `postgres-failover-manual-async-stall0-20261108T215357Z` | manual promotion | 1,495 | 89 | 1,143 | 134 (0) | PASS |

What the results say:

- Under a sessions pause and a sessions restart the invariant held: no overlap and no lost acknowledgement, with 116 and 100 requests of unknown outcome. First accepted reservation after the fault: 1 s after the pause ended, 11 s after the restart command returned (`recovered_within_s` in `result.json`).
- With the protection removed the checker went red with 292 violations in 883 operations, so it can see a missing exclusion constraint.
- The one failure under a real fault is the asynchronous failover with an unreachable replica: 10 acknowledged reservations were lost, and because the promoted node did not know them, it accepted new reservations for the same connectors, so 32 pairs of acknowledgements overlap, each pair with a lost reservation on one side (the database itself held no overlapping rows). That is the double booking of ADR 0000, and it is caused by replication, not by the constraint (ADR 0018). An earlier run of the same scenario was judged by a checker with two defects (see Decision) and is superseded.
- The synchronous runs with the same fault passed (two runs).

## What this does not prove

This is a safety check on two properties of write operations, not a linearizability check.

- It does not search for a total order of all operations that respects real time and the sequential specification. Reads (`GET /v1/sites`, session lists), the cancel result and the session start are not checked. A history can pass here and still contain a stale read.
- The overlap check trusts the database's own `starts_at`, `ended_at` and `expires_at`. The exclusion constraint already guarantees no overlap among Pending and Active rows, so passing mostly confirms that the guarantee survived the fault and that nothing bypassed it. A wrong `ended_at` (a station rejecting a reservation it should have accepted, or a wrong status transition) would shorten a hold and hide an overlap.
- Unknown outcomes are matched by connector and start time within 1 s, which assumes the client and the database clocks agree (they run on one machine here). With clock skew the matching degrades.
- The history is short (883 to 1,552 operations per run) on 6 connectors and 12 drivers: it proves nothing about rare interleavings, and most rows are a single run, so they give no spread (the pause, restart and mutation rows were repeated once on the advisory-lock build and agree with the first run).
- One fault per run. No network partition between services, no clock skew, no fault inside a transaction, no disk fault.
- It checks that an acknowledged row exists after recovery, not that its status is the right one.

## Rejected alternatives

- **A full linearizability checker (Knossos or Porcupine style).** The operations are not a register: a reservation is an interval, and the model of "this connector is free" needs a purpose-built specification. A generic checker also needs complete histories, and a lab with up to 13 unknown-outcome reserves in a hundred (116 of 884 in the pause run) would need a model for them. Not done; this ADR says so instead of implying it.
- **Checking only the database.** `SELECT` for overlapping rows would pass after a failover that lost acknowledged reservations and left a clean-looking table. The history is what exposes the loss.
- **Counting 409 responses as proof the constraint works.** A refusal says nothing about the cases where the constraint was bypassed or a write was lost.

## Consequences

- The checker and the unit tests live in `load/history/` and run in `make check`. The drills (`reservation-invariant.sh`, `postgres-failover.sh`) are local, like every drill (ADR 0020).
- Any change that touches the reservation path should rerun `FAULT=pause`, `FAULT=restart` and the mutation run, and the mutation run must stay red.
- The generator's think time and pool size decide how much contention a run produces. Raising the number of drivers or shrinking the pool increases contention and the lock waits the first generator showed.
