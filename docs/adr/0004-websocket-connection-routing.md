# 0004. WebSocket connection routing across gateway instances

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: 50,000 persistent connections; ≥ 10,000 connections and ≥ 2,000 msg/s per instance,
  so 6 instances (N+1); SetChargingProfile outbound ~900/s; remote start delivered p99 < 2 s; a lost
  instance's stations are routable again within 60 s; gateway availability 99.95%.

## Context

Each of 50,000 stations holds one long-lived WebSocket to one of 6 gateway instances, about 8,333
each in steady state (ADR 0000). Any service may need to send that station a command, for
example `sessions` sending `SetChargingProfile`, with a peak of about 900/s. Something has to find
the instance that holds the socket, deliver the command, and return the result, with p99 under 2 s
from the driver's tap. Connections move when a gateway restarts or the load balancer rebalances.

## Decision

1. **Edge.** HAProxy uses `balance leastconn` with no stickiness. A long-lived connection makes
   leastconn the right balancing signal. The health check is `/health/ready`, so a draining
   gateway stops receiving new connections. One HAProxy holds 50,000 tunnels, which costs mostly
   memory (about 2 × 16 KB buffers per connection, around 1.6 GB [derived]). In production this is
   an L4 load balancer pair. In compose, 2 gateway instances stand in for 6.
2. **Registry.** On a successful upgrade the owning gateway writes
   `route:station:{id} = {instance, connectedAt, epoch}` with a 90 s TTL and refreshes it every
   30 s in pipelined batches for all of its connections. `epoch` is an `INCR` counter. On
   disconnect, a gateway deletes the key only if it still holds the epoch it wrote
   (compare-and-delete in a Lua script). A late close from a replaced connection therefore cannot
   remove the newer route.
   - Cost: 50,000 / 30 s is about 1,667 pipelined writes/s fleet-wide [derived], under 1% of what a
     single Redis can do.
3. **Command path.**
   `producer → Kafka station.commands.v1 (key stationId, 12 partitions) → dispatcher consumer group (all gateways, ≤ 12 members) → Redis GET route → PUBLISH gw:cmd:{instance} → owning gateway → station`.
   The owning gateway sends the CALL, waits up to 30 s for the response, and publishes
   `station.command-completed` to `station.command-results.v1`.
   - If there is no route, or the owning instance has no live heartbeat in `gw:instances`, the
     outcome is `StationOffline`.
   - If `expiresAt` has passed before delivery, the outcome is `Expired`.
   - If the PUBLISH reaches no subscriber, the dispatcher retries 3 times over 1.5 s in case the
     station is reconnecting, then reports `StationOffline`.
   - Cost at 900 commands/s: 900 GETs + 900 PUBLISH/s on Redis and about 75/s per dispatcher
     partition [derived]. The budget against the 2 s remote-start SLO is roughly 50 ms outbox +
     10 ms Kafka + 5 ms dispatch + the station round trip. It is measured end to end in `load/`.
4. **Gateway liveness.** Each instance keeps a score in `ZADD gw:instances <now> <id>` refreshed
   every 5 s. A route whose instance is older than 15 s counts as dead.
   - Graceful shutdown closes sockets with code 1012 (service restart), and stations reconnect
     with a random 0–10 s first delay (ADR 0003).
   - After a crash, HAProxy detects the dead backend in at most 4 s (`inter 2s fall 2`). Stations
     see the TCP reset and reconnect within 10 s, and their routes are rewritten on upgrade. The
     target is at most 60 s to routable. The "gateway instance lost" drill measures it.
5. **Per-instance resource plan** for 10,000 connections:
   - `ws` with `perMessageDeflate: false` (frames are small, and compression state costs about
     300 KB per connection).
   - Payloads under 4 KB.
   - Target RSS under 1.5 GB, about 100–150 KB per connection including Node overhead [derived;
     measured in `load/`].
   - Heap under `--max-old-space-size=2048`.

## Rejected alternatives

- **Every gateway consumes all commands and drops those it doesn't own.** With 6 instances, read
  amplification is 6× (about 5,400 records/s for 900 useful ones) and needs 6 consumer groups.
- **Consistent hashing at the LB on stationId.** Every scale event remaps 1/N of stations (about
  8,000 reconnects), it makes leastconn balancing impossible, and the system still needs to know
  whether the station is actually connected right now.
- **Sticky sessions with direct HTTP to the instance.** Callers would have to discover instance
  addresses. A pub/sub channel per instance decouples callers from the topology.
- **Redis Streams per instance instead of pub/sub.** A command for a station that just moved should
  fail fast and be retried by the producer against the new owner. Kafka already gives durability
  for the request.

## Consequences

- If Redis is unavailable, new commands fail as `StationOffline`. Existing sockets, OCPP answers
  and telemetry keep working, and charging continues on the profiles the stations already have
  (ADR 0006).
- The 10,000 connections and 2,000 msg/s per instance in ADR 0000 are pass/fail thresholds in
  `load/` (k6 WebSocket test against one instance).
