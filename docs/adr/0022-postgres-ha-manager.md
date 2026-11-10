# 0022. Postgres HA manager: Patroni with a synchronous standby and fencing

- Status: proposed
- Date: 2026-11-10
- Driven by: ADR 0000 "Durability" (RPO 0 for sessions and billing, amended by ADR 0018) and the production gaps ADR 0018
  lists: no failure detection, no fencing of the old primary, no re-attachment of the old primary as a standby.

## Context

ADR 0018 measured a failover with a scripted `pg_promote()` (`drills/postgres-failover.sh`). It works for a drill and is
not production: nothing detects the failure, nothing stops the old primary from accepting writes again if it comes
back (split brain), and with the lab's asynchronous replica acknowledged commits were lost (281 and 282 writes in two
runs). Production needs an owner for "who is the primary" that decides on a quorum, not on one script's view.

## Decision

Run Postgres under **Patroni** with a **3-node etcd** as the distributed configuration store (DCS).

- **Durability:** `synchronous_mode: true`. A commit is acknowledged only after one standby has it, which gives RPO 0
  across the loss of the primary (the ADR 0000 amendment). With `synchronous_mode_strict` left off, Patroni degrades to
  asynchronous when no standby is available; that trades RPO for availability and must raise an alert.
- **Fencing:** the leader key in etcd has a TTL. A primary that cannot renew it demotes itself, and the Patroni
  **watchdog** resets a node whose Patroni process hangs. A new leader is only promoted after the old key expired, so
  two writable primaries cannot exist for longer than the TTL.
- **Routing:** HAProxy health-checks Patroni's REST API `GET /primary` (200 only on the leader) and sends writes there;
  `/replica` backs the read pool (ADR 0015). Services keep one host name and need no repointing.
- **Rejoin:** a restarted old primary comes back as a replica (`pg_rewind`, enabled in Patroni), never as a primary.

## Rejected

- **Scripted `pg_promote`** (the current drill, `drills/postgres-failover.sh:113`): no failure detection and no fencing;
  the old primary can accept writes after it returns.
- **repmgr:** detects failures and promotes, but fencing is left to scripts the operator writes, which is the gap this
  ADR closes.
- **Managed service (RDS, Cloud SQL):** a sound production choice, out of scope for a local lab that has to show the
  mechanism.

## Consequences

- Cost: three etcd nodes plus at least two Postgres nodes; commit latency includes one standby round trip.
- The failover drill must be rewritten: kill the Patroni leader, measure RTO until `/primary` answers on the new
  leader, check that no acknowledged write is lost, then check that the restarted old primary rejoins as a replica.
- That implementation (compose services, Patroni config, drill, ADR 0018 numbers re-measured) is a separate plan. Until
  it lands, the limits in ADR 0018 and `docs/known-limits.md` stand.
