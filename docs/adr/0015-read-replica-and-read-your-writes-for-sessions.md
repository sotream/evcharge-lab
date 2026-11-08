# 0015. Read replica and read-your-writes for sessions

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: "Driver API reads (map, availability, session status)" at 500 RPS peak and the 1,000 RPS per instance target; Driver API "99.9% monthly availability; read p99 < 300 ms, write p99 < 800 ms"; Durability "RPO 0 for sessions and billing (synchronous commit)". ADR 0001 (the OLTP instance holds sessions), ADR 0005 (reservations), ADR 0010 (driver-api is the only caller of the driver routes of sessions).

## Context

Driver reads of the map and of availability are the largest read traffic in the system, and they compete for the same Postgres primary that serves the OCPP hot path (CALLRESULT p99 < 500 ms) and the reservation writes. A read replica takes the map reads off the primary.

A replica is asynchronous, so it can be behind. Two kinds of read have different needs:

- The site map and availability are stale-tolerant. A connector that turned Charging two seconds ago may still show Available.
- A driver who has just reserved, started or stopped must see that write on the next read. A stop that returns 202 followed by a status read that shows `Active` is a bug a driver notices.

The primary keeps `synchronous_commit=on` for RPO 0 (ADR 0000). Making the replica a synchronous standby would couple the write latency (p99 < 800 ms) and the availability of every write to the replica, so it is not one.

## Decision

### Topology

- The existing `postgres` service is the primary: `wal_level=replica`, `max_wal_senders=10`, a `replicator` role created by `infra/postgres/init-replication.sql` (and, for a data directory that predates it, idempotently by the replica entrypoint), and a `pg_hba.conf` that allows replication from the compose network ranges only. A physical replication slot `replica_1` keeps the WAL the replica still needs, and `max_slot_wal_keep_size=2GB` stops a dead replica from filling the disk. If the replica loses its slot because it was away for more than 2 GB of WAL, it must be re-bootstrapped (delete the `pgreplica` volume).
- `postgres-replica` uses the same pinned image, host port 25434, its own volume `pgreplica`, and is created by `pg_basebackup -R` in `infra/postgres/replica-entrypoint.sh`. It runs with `hot_standby=on` and `hot_standby_feedback=on` (no query cancellations on the standby, at the price of some bloat on the primary during a long query). Its healthcheck requires `pg_is_in_recovery()`.
- `postgres-replica-exporter` (host port 29189) exposes `pg_replication_lag_seconds` for the replica, scraped with the label `db="oltp-replica"`. Lag in bytes (`pg_current_wal_lsn()` minus `replay_lsn` in `pg_stat_replication`) is measured on the primary by sessions as `sessions_replica_lag_bytes`, because postgres-exporter 0.20 has no custom query file.
- There is no automatic promotion. The replica is a read scale-out, not a failover mechanism; failover is out of scope for this ADR. ADR 0018 promotes it by hand or by script and measures what that loses: the replica is asynchronous, so a promotion can lose the last seconds of acknowledged writes (RPO is not 0), and only a synchronous standby avoids that.

### Routing in sessions

`REPLICA_DATABASE_URL` is optional. When it is unset or empty, every read uses the primary. Otherwise sessions has a second pool and a `ReadRouter`:

| Read | Target |
|---|---|
| `GET /v1/sites` (map and availability, the catalog) | replica, always |
| Session or reservation read **with** `x-min-lsn` | replica once `pg_last_wal_replay_lsn() >= x-min-lsn`, else primary |
| Session or reservation read **without** `x-min-lsn` | primary |
| Everything the stations and workers do | primary |

Read-your-writes works like this:

1. Every write response of sessions (reserve, cancel, remote start, stop) carries the header `x-commit-lsn`, the value of `pg_current_wal_lsn()` read right after the commit. That is at or after the commit record, so waiting for it is never too early. Under heavy write load it can be slightly later than the commit and cause a slightly longer wait, which is the safe direction. Replayed idempotent responses carry the current LSN, which is also safe.
2. driver-api keeps the last commit LSN per driver and, for session and reservation resources, per resource, for 30 s (`MIN_LSN_TTL_MS`). It sends the later of the two as `x-min-lsn` on the driver's reads of their sessions. It never sends `x-commit-lsn` to the driver.
3. sessions polls `pg_last_wal_replay_lsn()` on the replica every 20 ms for at most 200 ms (`REPLICA_RYW_TIMEOUT_MS`, `REPLICA_RYW_POLL_MS`). If the replica catches up, the read runs there (`replica_caught_up`). If not, or if the replica cannot be asked, the read runs on the primary (`fallback_primary`).
4. A malformed `x-min-lsn` is a 400. A failed replica query is retried once on the primary.

The 200 ms wait was chosen so that the slowest read-your-writes path, which is the wait plus a primary query plus the hop through driver-api, stays below the 300 ms read p99 of ADR 0000. It applies only to reads within 30 s after the driver's own write, not to the map.

### Documented staleness bound

- **Stale-tolerant reads (site map, availability)** are at most *replica replay lag + 3 s* old. The 3 s is the driver-api sites cache (`SITES_CACHE_TTL_MS`). The lag alert `SessionsReplicaLagHigh` fires when the lag stays above **5 s for 2 minutes** (warning), and `SessionsReplicaLagCritical` above **30 s for 1 minute** (critical). So while no alert is firing, a map read is at most about 5 s + 3 s = 8 s old; after the warning it can be older, up to the lag, and the alert says so.
- **Reads of a driver's own recent writes** are never stale: they are served by a replica that has replayed the write, or by the primary. The cost of a lagging replica is latency (up to 200 ms of waiting) and load on the primary, not wrong data.
- **Reads without `x-min-lsn`** of sessions and reservations are never stale, because they use the primary.

The lag in seconds is `0` when everything the replica received has been replayed (an idle primary is not lag); otherwise it is the age of the last replayed transaction.

### Observability

Metrics of sessions: `sessions_reads_total{target}`, `sessions_ryw_waits_total{outcome}`, `sessions_replica_lag_seconds`, `sessions_replica_lag_bytes`. Alerts are in `infra/prometheus/rules/slo-alerts.yml` and unit-tested in `infra/prometheus/tests/slo-alerts_test.yml` by `make prom-check`. The drill `drills/replica-lag.sh` pauses replay for 240 s (the lag rule needs lag above 5 s for 2 m and Prometheus evaluates rules every 1 m; a first run with 150 s never reached `firing`). Measured in `docs/drill-output/replica-lag-20261108T183638Z` (Apple M4 Pro, Docker VM 8 CPU / 11.7 GiB, 11 of 11 checks): steady-state lag 0 s; peak lag 239.4 s; drained 9 s after replay resumed; 624 read-your-writes checks with 0 violations; 338 `fallback_primary` waits per the Prometheus counter increase over the pause; 208 stale-tolerant probes, all resolved, the largest staleness 239.6 s, 0.3 s above the peak lag of 239.4 s (the bound `stale_excess_over_lag_s` was -12.7 s, i.e. inside the lag plus a 13 s margin); `SessionsReplicaLagHigh` fired; driver API read p99 0.173 s during the pause (target < 0.3 s). Two earlier runs failed on drill defects and are kept with notes; one of them showed that the stale probe must accept `SuspendedEVSE` as a started-session status, because the allocator can hold a session at the site cap.

## Rejected alternatives

- **Synchronous standby (`remote_apply`).** It gives read-your-writes for free, but every write waits for the replica and fails or stalls when the replica is down. It trades the 99.9% availability and the 800 ms write p99 for a read optimization.
- **Read the primary for N seconds after any write (time-based pinning only).** It is simple, and driver-api's LSN map has the same TTL, but it guesses the replication delay. LSN comparison is exact and shorter in the common case, and it reads the replica again as soon as it is caught up.
- **LSN in the JWT or a cookie.** The token is not renewed per write, and the LSN must stay an internal detail between driver-api and sessions.
- **Send all driver session reads to the replica.** Session state is also changed by stations and workers, not only by the driver, so a driver write LSN does not cover it. Those reads use the primary unless the driver has just written.

## Consequences

- The primary serves less map traffic; how much was not measured here. The measured effect belongs in a load run, not in this ADR.
- Session and reservation reads move to the replica only inside the 30 s after the driver's own write. With no header they stay on the primary, so the offload for them is small. That is the conservative choice, and it can be widened later for history lists.
- Shortcut: driver-api keeps the LSN map in process. With several driver-api instances, a read that lands on an instance that did not see the write carries no header and is served by the primary, which is correct but not offloaded. The upgrade is to keep the LSN in Redis under the driver and resource key with the same TTL.
- A replica that is down or far behind makes read-your-writes reads slower by up to 200 ms and moves their load to the primary. The map keeps working from the replica as long as the replica is up, and falls back to the primary on a replica error.
- Recreating the primary container is needed once to apply `wal_level`, the slot cap and the `hba_file` setting; the data directory is kept.
