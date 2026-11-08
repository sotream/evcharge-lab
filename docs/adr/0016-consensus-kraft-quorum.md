# 0016. Consensus: the KRaft controller quorum and what its loss does

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: billing "0 lost, 0 double charges"; "charging never stops because the backend is down".
  Builds on ADR 0012 (replication, backpressure) and ADR 0002 (delivery semantics).

## Context

ADR 0012 shows what happens when brokers fail one or two at a time, but it treats the cluster as a black box. Kafka 4.3 in
KRaft mode runs its own Raft consensus for cluster metadata. Two different majorities decide what this system can do, and they
fail independently of the application. This ADR names both, shows the maths, and states what the platform does when the
control-plane majority is lost.

## Decision

### The control-plane quorum (KRaft)

- **Voters.** The 3 nodes are combined broker and controller. `KAFKA_CONTROLLER_QUORUM_VOTERS` is
  `1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093` (`docker-compose.yml`). All 3 are voters.
- **Metadata log.** Topics, partitions, partition leaders, ISR sets, configs and broker registrations are records in a single
  replicated log (`__cluster_metadata`). Brokers fetch it and keep an in-memory image. Voters and observers pull the log with
  fetch requests, the leader does not push it.
- **Leader election.** One voter is the active controller (the Raft leader). It holds a leader epoch (a term). A voter that
  stops hearing from the leader becomes a candidate, increments the epoch, and asks for votes. A voter grants at most one vote
  per epoch, and only to a candidate whose log is at least as up to date as its own. A candidate that gets a majority becomes
  leader for that epoch.
- **Commit.** The leader appends a record and counts it committed when a majority of voters have it (the high watermark moves).
  Only committed metadata is acted on. Partition-leader changes, ISR changes and topic creation are all such records.
- **Majority quorum.** With `n` voters the quorum is `floor(n/2) + 1`.

### Quorum maths: 3 versus 5 voters

| Voters `n` | Quorum | Tolerated failures `floor((n-1)/2)` | Followers the leader waits for per commit |
|---|---|---|---|
| 3 | 2 | 1 | 1 (the faster of 2) |
| 5 | 3 | 2 | 2 (the second fastest of 4) |
| 4 | 3 | 1 | 2 (no gain over 3, so even sizes are not used) |

- **Write latency.** A commit costs one round trip to the fastest `quorum - 1` followers. With 5 voters the leader waits for the
  second-fastest follower, so the tail of a metadata write is a little worse than with 3, and every record is sent to 4
  followers instead of 2. Metadata writes are rare (topic, leadership and ISR changes), not per message, so this is a small cost
  here. The data path (produce) does not go through the controller quorum.
- **Cost of 5.** Two more nodes to run, patch and monitor, more fetch traffic to the leader, and, in a combined-role lab, two more
  JVMs on a memory-bound Docker VM. The benefit is surviving a second failure, for example one node down for maintenance while a
  second fails.
- **Decision.** 3 voters in the lab. Production would use 3 dedicated controllers for a small cluster, and 5 when the availability
  target or maintenance practice requires tolerating two simultaneous losses. The metadata log is small, so controllers are cheap
  when they are not also brokers.

### Why there is no split-brain

- **Majority intersection.** Any two majorities of the same voter set share at least one voter. Two leaders in the same epoch would
  both need a majority of votes, and a voter votes once per epoch, so at most one leader exists per epoch. A minority partition
  can never reach a majority, so it can never commit.
- **Fencing by epoch.** Every fetch and every append carries the leader epoch. A voter that sees a higher epoch steps down and
  truncates any uncommitted suffix that disagrees with the new leader. A deposed leader that is still running cannot commit,
  because it cannot get a majority, and its requests are rejected by anyone who knows the newer epoch.
- **Brokers are fenced too.** Partition leaders carry a partition leader epoch, and brokers hold a broker epoch from their
  registration. Requests from stale leaders are rejected (`FENCED_LEADER_EPOCH`, `NOT_LEADER_OR_FOLLOWER`).
- **Rule that follows.** For metadata the system chooses consistency over availability (CP in the CAP sense). Without a majority
  it stops changing metadata instead of risking two histories.

### The data-plane quorum versus the control-plane quorum

| | Control plane | Data plane |
|---|---|---|
| What | KRaft voters (`__cluster_metadata`) | ISR of each partition |
| Decides | who leads each partition, topics, ISR membership | whether a produce is acknowledged |
| Size here | 3 voters, quorum 2 | RF 3, `min.insync.replicas=2` |
| Rule | majority of voters | `acks=all` waits for all current ISR members, and at least `min.insync.replicas` must be in the ISR |
| Fails when | 2 of 3 voters are down | fewer than 2 replicas of a partition are in sync |

They are different mechanisms with similar numbers. In this lab they fail together, because the same 3 nodes are voters and
replicas, so losing 2 nodes loses both majorities at once. The ISR is not a Raft quorum: the partition leader waits for every
ISR member, and the controller (which needs its quorum) is what shrinks or grows the ISR. A lost controller quorum therefore also
blocks recovery of the data plane: the ISR cannot be changed and a new partition leader cannot be elected.

### What quorum loss does to this system

Stopping `kafka-2` and `kafka-3` leaves `kafka-1` alone.

| Area | Effect | Why |
|---|---|---|
| Metadata changes (create topic, change config) | fail or time out | no majority to commit the record |
| Partition leader election | does not happen | elections are metadata records. Partitions led by the surviving node keep their leader, partitions led by a lost node have none |
| Produce, `acks=all`, `min.insync.replicas=2` | fails or times out | one replica cannot meet min ISR (ADR 0012) |
| Consume | reads from surviving leaders may continue, but consumer-group coordination can stall | the group coordinator for a group may be on a lost node |
| Gateway | stays live and ready | produce errors are absorbed by the bounded buffer and CALLERROR backpressure (ADR 0012); heartbeats never touch Kafka |
| Stations | stay connected and keep charging | the profiles in force stay in force, the gateway sends no RemoteStopTransaction on a Kafka failure, and the safe-share fallback is local (ADR 0006) |
| `sessions`, `billing` | keep writing state and the outbox | the outbox table is the buffer: rows accumulate (`outbox_pending`) and drain after recovery (ADR 0002) |
| Telemetry | stations keep unacknowledged MeterValues queued and replay them later, and `telemetry-ingest` dedups (ADR 0003, ADR 0008) | the CALLRESULT is sent only after the broker acknowledges |

Recovery needs no operator action beyond starting the nodes: a quorum leader is elected, partition leaders are elected from the
ISR, the producers' retries succeed, the gateway buffer drains, outbox rows are published, and replay plus dedup closes any gap.
Nothing acknowledged is lost, because nothing was acknowledged without `min.insync.replicas` copies.

### Drill

`drills/kraft-quorum-loss.sh` stops `kafka-2` and `kafka-3` under simulator load and records `kafka-metadata-quorum describe
--status` and `--replication` before, during and after. Checks: gateway live and ready during, all stations connected during,
no RemoteStopTransaction sent, topic creation fails during, `time_to_quorum_s <= 60`, `time_to_first_produce_s <= 90`, and
0 probe readings missing after recovery.

Result: see `docs/drill-output/kraft-quorum-loss-*/result.json`.

#### Measured

Run `docs/drill-output/kraft-quorum-loss-20261108T185039Z` (Apple M4 Pro, Docker VM 8 CPU / 11.7 GiB, see `env.json`; 200 simulated
stations, 60 s before, then `kafka-2` and `kafka-3` stopped for about 165 s from 18:51:44Z to 18:54:29Z, then recovery; `DOWN_S` = 60 s is only the last sleep, after the 10 s settle and the three bounded probes). 12 of 12 checks passed. Earlier run
`...T184440Z` failed one check because of the measurement window and is kept with a note.

| What | Value | Source |
|---|---|---|
| `time_to_quorum_s` (nodes started to leader elected, all 3 voters lag ≤ 5) | 4 s | `result.json` |
| `time_to_first_produce_s` (acks=all, one record) | 8 s | `result.json` |
| `time_to_zero_produce_errors_s` | 8 s | `result.json` |
| Metadata quorum before / after | leader 1, epoch 4 / epoch 6, all lags 0 | `quorum-before.txt`, `quorum-after.txt` |
| `kafka-metadata-quorum describe` during the outage | both calls hit the 20 s `timeout` (exit 124) | `quorum-during.txt` |
| Topic create during the outage | failed (25 s `timeout`, no topic) | `create-topic-during.txt` |
| Produce with `acks=all` during the outage | not acknowledged | `produce-during.txt`, `events.log` |
| Stations connected during the outage | 200 of 200 | `timeline.jsonl` |
| Active sessions, min during the outage | 186 (192 to 201 before) | `timeline.jsonl` |
| RemoteStopTransaction sent during the outage | 0 after the quorum loss was confirmed (from about 18:52:38Z); 2 at 18:51:48Z while the nodes were shutting down | `result.json`, `timeline.jsonl` |
| Max gateway produce error rate | 66.2 /s during, 65.6 /s after the nodes returned | `timeline.jsonl` |
| Max gateway produce buffer depth | 59 during, 47 after | `timeline.jsonl` |
| Peak `outbox_pending` | 0 during, 523 after the nodes returned | `timeline.jsonl` |
| OCPP CALLRESULT p99, max of 30 s windows | 0.023 s before, 2.48 s during, 2.36 s after | `timeline.jsonl` |
| Telemetry ingest lag p99, max | 0.50 s before, 29.9 s during, 59.2 s after | `timeline.jsonl` |
| Probe readings missing in Timescale | 0 of 393 (178 generated during this run, 117 while the nodes were down; 215 are left over from run `T184440Z`) | `events.log` |

Two targets of ADR 0000 are missed while the quorum is gone: the CALLRESULT p99 (< 500 ms) reached 2.48 s, and the telemetry
durability lag (p99 < 5 s) reached 29.9 s during and 59.2 s right after, until the buffered readings drained. This is the cost of
having no metadata quorum: the gateway buffers (ADR 0012), nothing is lost, but it is slower. The target stays missed here.

**Thresholds.** `QUORUM_MAX_S` 60 s, `PRODUCE_MAX_S` 90 s and `ERRORS_MAX_S` 120 s were chosen before the first run as upper
bounds, not derived from a formula: they allow the KRaft election timeouts, a producer metadata refresh and the known 1.3 to 1.9 s
client stall after a leader move (ADR 0012) with a wide margin. The run measured 4 s, 8 s and 8 s, so the bounds are loose
by a factor of 11 to 15. They are kept unchanged (one run on one machine is not enough to tighten them), and the measured values are
the result.

**Not shown by this drill.** The probe produce left no error text (`produce-during.txt` holds only a deprecation warning) and did not
return success, so the error code is unknown; `NotEnoughReplicas` does not appear in the saved output. One run was made, so the spread between runs is unknown.

## Rejected alternatives

- **ZooKeeper.** A second consensus system to operate. KRaft is the supported path in Kafka 4.x and keeps metadata in one log.
- **5 voters in the lab.** It tolerates two failures, but costs two more JVMs on a memory-bound host for a failure mode the drill
  already demonstrates with 3.
- **`unclean.leader.election.enable=true` to stay available.** It trades acknowledged data for availability (ADR 0012).
- **Lowering `min.insync.replicas` to 1 so produce survives.** One replica could then hold the only copy of an acknowledged record.

## Consequences

- Losing 2 of 3 nodes stops Kafka writes and metadata changes. It does not stop charging, and it does not lose acknowledged data.
- The lab cannot tolerate a second failure during a one-node maintenance window. Production needs 5 voters if that matters.
- Combined roles couple the two quorums. Dedicated controllers would let a broker fail without touching the metadata quorum.
