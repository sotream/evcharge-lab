# 0018. Postgres failover, backup and restore: measured RTO and RPO

- Status: accepted
- Date: 2026-11-08
- Superseded in part by [ADR 0022](0022-postgres-ha-manager.md) (proposed): the HA manager and fencing it says are missing.
- Driven by ADR 0000: "Durability: RPO 0 for sessions and billing (synchronous commit)" and the "Reservations: zero double bookings" SLO. ADR 0001 said that RPO 0 only holds on a single node and that a synchronous standby is a documented gap. This ADR measures what a node loss does, with and without that standby, and what a restore from backup brings back.

## Context

The OLTP Postgres instance holds the databases `sessions`, `billing`, `tariff` and `drivers` (ADR 0001). ADR 0015 added a streaming hot standby (`postgres-replica`, slot `replica_1`) for reads and said that failover was out of scope. Two questions follow, and the claim in ADR 0000 depends on the answer:

1. If the primary dies, how long until writes work again (RTO), and are acknowledged writes lost (RPO)?
2. If the data is gone (or corrupted), what does a physical backup bring back, how fast, and how old is it?

All numbers below are `[measured]` from `docs/drill-output/` on one machine (Apple M4 Pro, Docker VM 8 CPU / 11.7 GiB, recorded in each `env.json`), one run per row unless stated. Nothing here is a production figure: the replica and the primary are on one Docker host, so network lag is near zero, and the fault that creates data loss is injected on purpose.

## Decision

### What a failover consists of

1. The primary stops (the drill runs `docker kill`).
2. The replica is promoted: `pg_promote()`. It replays the WAL it already received and starts accepting writes.
3. Every service connects to the host name `postgres`. After the promotion that name must point at the promoted node. In the lab this is a Docker DNS alias: `docker network disconnect` and `connect --alias postgres --alias postgres-replica` on the replica container. No service is restarted or reconfigured. The services' pools reconnect on their own: `createPgPool` sets a 3 s connect timeout (`packages/platform/src/postgres/postgres.ts`) and node-postgres resolves the host name on every new connection (library behaviour, not visible in that file).

There is no automatic detection, no fencing of the old primary and no re-attachment of the old primary as a new standby. Those are the production gaps (see Consequences). The drill therefore ends by rebuilding the stack from empty volumes (`drills/lib/reset-stack.sh`).

`drills/postgres-failover.sh` has two modes. `MODE=auto` does steps 2 and 3 itself. `MODE=manual` kills the primary, prints the four commands, and waits for the operator; the run below was made by this session typing those commands.

### The fault that exposes RPO

A replica that has received all the WAL loses nothing when promoted, so a plain kill proves little. The drill adds an optional partition (`STALL_S`, default 3 s): before the kill the replica is cut off from the Docker network (`docker network disconnect`), so no WAL reaches it, and the primary is killed after the partition. That is "the standby was unreachable and then the primary died". Two replication modes are run: asynchronous (`synchronous_standby_names=''`, the lab default) and synchronous (`FIRST 1 (replica_1)`, with `synchronous_commit=on` as before).

### Measuring RPO

Two independent measurements run during every failover:

- A **durability probe** (`load/pg-ack-probe.mjs`) runs on the compose network and connects to the name `postgres`, like a service. It commits one row at a time (`drill_acks`) and writes every acknowledged sequence number to a file before the next attempt. After the failover, every acknowledged sequence must exist on the new primary. A missing one is an acknowledged write that was lost.
- A **reservation history** (`load/history/generate.mjs`, checker `load/history/check.mjs`, ADR 0019) records every reserve and cancel the drivers make. After the failover the checker proves that every acknowledged reservation exists and that no two acknowledged reservations overlap on one connector.

### Measuring RTO

RTO is counted from the moment `docker kill` has returned (the primary is certainly dead), and only writes that started after that moment count. A first version took the timestamp before the command and counted acknowledgements from the last 100 ms of the old primary as recovery (RTO 0.002 s); that run was discarded.

- **DB RTO**: first acknowledged probe commit that started after the kill.
- **Driver RTO**: first accepted reservation that was invoked after the kill.
- **Longest write gap**: the longest silence between two acknowledged probe commits, which is the write outage a database client saw.

### Result: failover

| Run (directory under `docs/drill-output/`) | Replication | Partition before the kill | DB RTO | Longest write gap | Driver RTO | Acknowledged probe commits | Lost |
|---|---|---|---|---|---|---|---|
| `postgres-failover-auto-async-stall0-20261108T203614Z` | async | none | 0.46 s | 0.65 s | 10.27 s | 9,117 | 0 |
| `postgres-failover-auto-async-stall3-20261108T204000Z` (first run) | async | 3 s | 0.48 s | 0.64 s | 1.19 s | 8,566 | **282** (seq 2724 to 3005, spanning 3,117 ms) |
| `postgres-failover-auto-async-stall3-20261108T220826Z` (rerun) | async | 3 s | 0.37 s | 0.54 s | 10.09 s | 9,426 | **281** (seq 2724 to 3004, spanning 3,067 ms) |
| `postgres-failover-auto-sync-stall0-20261108T204337Z` | sync | none | 0.37 s | 0.54 s | 1.10 s | 8,244 | 0 |
| `postgres-failover-auto-sync-stall3-20261108T204731Z` (first run) | sync | 3 s | 0.38 s | 3.64 s | 9.93 s | 9,092 | 0 |
| `postgres-failover-auto-sync-stall3-20261108T221218Z` (rerun) | sync | 3 s | 0.40 s | 3.65 s | 9.93 s | 9,103 | 0 |
| `postgres-failover-manual-async-stall0-20261108T215357Z` | async | none | 9.96 s | 10.15 s | 10.18 s | 8,275 | 0 |

What each row shows:

- **Async with a partition loses acknowledged writes, reproducibly.** In two runs 282 and 281 acknowledged probe commits were gone on the new primary, spanning 3,117 and 3,067 ms, which is the length of the partition. The reservation history shows the same fault as a business failure. The rerun (`...T220826Z`, with `ended_at` in the dump and a checker that also matches the start time, see ADR 0019) reports 42 violations: **10 acknowledged reservations missing from the database** (ids 431, 432, 433, 434, 447, 448, 452, 459, 461, 465) and 32 overlapping pairs. Every one of the 32 pairs has a lost reservation on one side (8 distinct lost reservations): the promoted node did not know the reservation, handed the connector to a later driver, and the first driver still holds an acknowledgement for it. The database itself never held two overlapping rows (0 `db-row` overlaps). That is the double booking that ADR 0005 and ADR 0000 say there must not be, caused by replication, not by the exclusion constraint. The first run was judged by an older checker (id and connector only, no `ended_at`): the promoted node had issued two of the lost ids again, which hid one loss, and the report said 6 lost and 3 overlaps; re-checking its saved files with the current checker gives 10 violations, and its NOTE.md says what is superseded.
- **Sync loses nothing, and pays with availability.** In the three sync runs (two with the partition, one without), 0 acknowledged writes were lost. With the partition, the primary could not commit while its only synchronous standby was unreachable: the probe's longest write gap was 3.64 and 3.65 s (async: 0.64 and 0.54 s), and writes resumed only after the primary died and the replica was promoted. That is the trade: the same 3 s partition costs 3.6 s of write unavailability under sync and costs 281 to 282 acknowledged writes under async.
- **A clean kill with a healthy replica loses nothing in either mode** (rows 1 and 3): the replica had every acknowledged commit. RPO is a property of the replica's lag at the moment of the kill, not of the mode alone. The lab replica is on the same host, so its normal lag is near zero (ADR 0015: 0 s in steady state); a real network and load make the async window larger.
- **The database recovers in under a second; drivers see it ten times slower in some rows.** DB RTO is 0.37 to 0.48 s in every automatic run: `pg_promote` returned 0.2 to 0.3 s after the kill and the alias was in place 0.4 s after. The driver RTO is 1.1 to 1.2 s in two rows and 9.9 to 10.3 s in the other five (the two async runs with the same setting gave 1.19 s and 10.09 s). The cause is the circuit breaker in driver-api: after 5 consecutive failures it opens for `openMs` = 10 s (`packages/platform/src/http-client/circuit-breaker.ts`) and rejects requests with `503 downstream_unavailable` without calling sessions. The rows with about 1 s have few driver-API 5xx (3 and 15, counted from the last pre-fault sample to the end of the run) and the rows with about 10 s have many (205 to 228), which is consistent with the breaker opening in the latter; the breaker state itself was not recorded. So the driver RTO is the DB RTO plus up to `openMs`, depending on whether five failures happen in a row.
- **Manual failover is operator time.** The 9.96 s DB RTO of the manual run is the time this session took to type the four printed commands [unverified: the manual steps are not timestamped] (the drill saw the first accepted reservation 10.2 s after the kill (`rtoAppS` 10.176)). It says nothing about a human on a pager. What it shows is that nothing else was needed: no service restart, and the services reconnected by themselves once the name resolved.

Errors seen, from counter growth in Prometheus between the last pre-fault sample and the end of the run (so they include the 60 s after recovery): stations received 0 to 8 OCPP CALLERRORs (they retry transaction messages), `sessions` returned 3 to 33 HTTP 5xx, and driver-api 3 to 228 (a large number comes from the open breaker answering quickly). Per run: `summary.json` under `stationsAndServices` and `driversInWindow`.

### Result: backup and restore

`drills/backup-restore.sh` takes a physical backup of the whole instance (all databases, so `sessions` and `billing`) with `pg_basebackup -X stream -c fast` into a volume, keeps the load running for 60 s, then restores the backup into a separate container (copy the volume, start Postgres on it, crash recovery to the end of the backup) next to the live primary, which is not touched. Run `backup-restore-20261108T215232Z`:

| What | Value |
|---|---|
| Backup size | 163,044 KiB (about 159 MiB) |
| Backup time | 1.07 s |
| Restore: copy into a fresh volume | 0.73 s |
| Restore: start to first successful query | 1.36 s |
| Restore total | 2.10 s |
| Time between the end of the backup and the "disaster" | 60.9 s |
| Newest reservation on the source / in the restored copy | 21:53:22.925Z / 21:52:30.216Z |
| **Data age (RPO of a backup-only recovery)** | **52.7 s** |

Rows that were on the source at the disaster and not in the restored copy: 33 sessions, 5 reservations, 27 invoices, 108 invoice lines, 21 payments, 21 PSP captures, 422 and 60 processed-event rows in `sessions` and `billing`.

All 10 checks of the drill passed (readiness of the restored instance plus 9 integrity checks): every table's row count lies between the source counts taken just before and just after the backup (the tables are append-mostly), no overlapping Pending/Active reservations on a connector, every invoice total equals the sum of its lines, no session has two invoices, `invoices` equals `invoice_registry` in rows, every payment equals its invoice total and has an invoice, the sum of payments equals the sum of PSP captures, and no session stopped before it started.

The backup is a base backup with the WAL it needs streamed into it (`-X stream`), so it is consistent by itself. It is not point-in-time recovery: nothing after the backup is brought back. Continuous WAL archiving (`archive_command`, `archive_timeout`) would reduce the data age from "time since the last backup" to roughly the archive lag, and it is the production step. It is not built here, because the lab has no object store to archive to and the drill's point is to measure what the base backup alone gives. The 159 MiB and 1 s do not extrapolate: the lab database is small and the disk is local.

### Consequences for ADR 0000

ADR 0000 says "RPO 0 for sessions and billing (synchronous commit)". Measured, that holds as follows, and ADR 0000 is amended accordingly:

- On one node, a crash loses no committed write (`synchronous_commit=on`, WAL flushed before the acknowledgement). Not re-measured here (no crash-recovery drill of the primary alone).
- Across loss of the node, RPO 0 holds only with a synchronous standby (0 lost in two runs). With the lab's default asynchronous replica the target is **missed**: a replica that was unreachable for 3 s before the primary died loses 3.1 s of acknowledged writes, including acknowledged reservations and, through them, double bookings.
- The lab default stays asynchronous. A synchronous standby ties the availability of every write to the standby (3.6 s write gap during a 3 s partition) and ADR 0015 chose not to couple them for read scale-out. That choice is now a measured trade-off, not an oversight.
- A restore from a base backup alone has a data age equal to the time since the backup (52.7 s here, 60.9 s after the backup).

## Rejected alternatives

- **Promote by restarting services with the new connection string.** Slower and touches every service. The DNS alias moves the name once and the pools reconnect.
- **A failover manager (Patroni, repmgr, Stolon) in the lab.** It would give automatic detection and fencing, which are the real production gaps, but it adds a consensus store and a second subject to a lab that already measures consensus on Kafka (ADR 0016). Not done; listed as the gap.
- **Measuring RPO only with the reservation history.** It shows the business consequence but at about 15 operations per second for all drivers together it would rarely hit the lost window. The probe commits about 90 times per second and gives an exact count; the history shows what the loss means.
- **Pausing the replica container (`docker pause`) as the partition.** A paused container still has its receive buffer: WAL already sent to the socket is read after the unpause, so little is lost. A real network disconnect delivers nothing. A first design used the pause and was dropped before any run.

## Consequences

- No automatic failover: detection, promotion, fencing of the old primary (nothing stops it from coming back and accepting writes: split brain) and re-seeding it as a standby are all manual or missing. After a failover the lab has no replica; the drill rebuilds everything.
- The DNS alias trick works because one Docker network owns the name. Production needs a virtual IP, a proxy such as HAProxy or PgBouncer in front, or a DNS TTL discipline.
- The driver-visible recovery time is bounded by the circuit breaker's `openMs` (10 s), not by the database (0.4 s). Lowering it speeds recovery and raises the risk of hammering a sick service; the knob is in `createHttpClient`'s `circuitBreaker.openMs` (and `failureThreshold`, default 5).
- Most rows are one run. The spread between runs (1.19 s against 10.09 s driver RTO under the same async setting with a partition, depending on whether the breaker opened) shows that one run does not give a stable driver RTO; the loss (282 and 281) repeated closely. Repeat runs before quoting a number outside this lab.
- Every run records the machine and the Docker VM in its `env.json`.
