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
   an L4 load balancer pair. In compose, 2 gateway instances stand in for 6. In compose, HAProxy
   resolves the gateways through Docker DNS (`resolvers docker` at 127.0.0.11, with
   `init-addr libc,none` on each server line). With startup-only resolution, a recreated gateway
   container got a new IP that HAProxy never learned, and every upgrade got 503. That was found when
   three drill reruns ran with 0 of 200 stations connected.
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
     10 ms Kafka + 5 ms dispatch + the station round trip. It is not measured: no end-to-end remote-start measurement exists in `load/`.
4. **Gateway liveness.** Each instance keeps a score in `ZADD gw:instances <now> <id>` refreshed
   every 5 s. A route whose instance is older than 15 s counts as dead.
   - Graceful shutdown closes sockets with code 1012 (service restart), and stations reconnect
     with a random 0–10 s first delay (ADR 0003).
   - After a crash, HAProxy detects the dead backend in at most 4 s (`inter 2s fall 2`). Stations
     see the TCP reset and reconnect, and their routes are rewritten on upgrade. The 10 s figure
     is a design estimate, not a measurement: the gateway-instance-loss rerun
     (`docs/drill-output/gateway-instance-loss-20261108T122353Z/result.json`) measured 31 s for all
     stations to reconnect (`stations_reconnected_within_s`). The target is at most 60 s to
     routable. The "gateway instance lost" drill measures it.
5. **Per-instance resource plan** for 10,000 connections:
   - `ws` with `perMessageDeflate: false` (frames are small, and compression state costs about
     300 KB per connection).
   - Payloads under 4 KB.
   - Target RSS under 1.5 GB. The original estimate was 100–150 KB per connection including Node
     overhead [derived]. Measured: peak RSS 525 MB with 10,000 connections
     (`docs/load-output/gateway-10k-connections.json`), which is about 52.5 KB per connection
     [derived: 525 MB / 10,000]. The first sample of the same run (t = 2 s, 200 connections) shows 201 MB, so the
     incremental cost is about 33 KB per connection [derived: (525 − 201) MB / 9,800, with 1 MB taken as
     10^6 bytes]. The estimate was therefore a safe upper bound.
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

## Measured against the lab target (amendment, 2026-11-08)

ADR 0000 sets a per-instance target of at least 10,000 connections and at least 2,000 msg/s with
CALLRESULT p99 under 500 ms. The measurements (raw files in `docs/load-output/`):

| Run | Setup | Result | Target met |
|---|---|---|---|
| `gateway-10k-connections.json` | native gateway process, simulator with 10,000 stations, real Redis/Kafka | 10,000 connections, peak RSS 525 MB | connections: yes; memory (< 1.5 GB): yes |
| `k6-gateway-ws-1000st-1hz.json` | gateway container, k6 in the compose network, 1,000 stations at 1 msg/s, one outstanding CALL each | 910.28 msg/s, CALLRESULT p50 9 ms, p99 1,446 ms, 0 CALLERROR | rate: not tested (offered load is at most 1,000 msg/s: 1,000 stations × 1 msg/s [derived]); p99: **missed** |
| `k6-gateway-ws-2000st-1hz.json` | same, 2,000 stations | 1,139.03 msg/s, p50 137 ms, p99 3,228 ms, 0 CALLERROR | rate: **missed** (offered at most 2,000 msg/s, so this run tests the rate target); p99: **missed** |
| `k6-gateway-ws-2000st-1.25hz.json` (+ 2 repeats in `gateway-ws-2000st-1.25hz-repeats-*`) | rewritten script (steady window, 2,500 msg/s offered, unanswered CALLs counted), M4 Pro, Docker VM 8 CPUs / 11.7 GiB, build with the per-socket limits | 2,490.2 / 2,498.1 / 2,491.3 msg/s in the steady window, p99 253 / 237 / 223 ms, 0 CALLERROR, 0 unanswered | rate: **met**; p99: **met** (measured; method, VM and build differ from the two runs above) |

Correction (November 2026 review): the 2,000-station run above offered exactly 2,000 msg/s and k6 divided by the whole run including the connection ramp, so its rate could not reach the target by construction (ceiling about 1,545 msg/s); only its p99 miss is a real finding. The 1.25 Hz series replaces it as the test of the rate target; the old rows stay as recorded.

- **Bottleneck:** one Node.js core per instance. A single gateway process was observed at about
  108% CPU during the first two k6 runs (not re-measured in the 1.25 Hz series) (observed, not saved as a file). The saved 10,000-connection run is a different load
  (stub sessions, no database): its `samples[].cpuPct` peaks at 110 and the last-30-second average is 52
  (`gateway-10k-connections.json`). Per message the work is JSON parse, zod validation, a Kafka produce with
  `acks=all` through a pure-JS client, and a second produce for the raw log.
- **Raw-log experiment (unreproducible, not used as evidence):** an earlier experiment reported that turning the raw
  log off raised the 2,000-station run from 802 to 1,068 msg/s. No file was saved, and the figures are inconsistent
  with the kept 2,000-station run, which reached 1,139.03 msg/s with the raw log on. Removing the second produce is
  still a reasonable scaling step, but its effect is not measured.
- **The fleet requirement is not demonstrated.** 2,400 msg/s design peak across 5 surviving instances is
  480 msg/s per instance [derived: 2,400 / 5]. No run measured the CALLRESULT latency at 480 msg/s. The one run
  nearest to that rate, 910.28 msg/s, missed the p99 target (1,446 ms against 500 ms), although it had zero
  CALLERRORs. Latency at 480 msg/s per instance is not measured.
- **Not met:** the per-instance lab target of 2,000 msg/s at p99 < 500 ms, recorded here as
  measured rather than lowered.
- **Scaling path:**
  - Run one gateway process per core (Node cluster, or more replicas behind HAProxy). The registry
    already supports any number of instances.
  - Move the raw log to sampling or batching.
  - Replace the per-frame zod parse with a precompiled validator.
