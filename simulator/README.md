# simulator

OCPP 1.6J station fleet in one Node process (`@evcharge/simulator`). It produces the live load for compose, the drills and the
README charts. Governing ADRs: [0013](../docs/adr/0013-traffic-generators-simulator-and-backfill.md),
[0003](../docs/adr/0003-ocpp-subset-station-auth-and-offline-replay.md), [0006](../docs/adr/0006-smart-charging-allocation.md),
[0012](../docs/adr/0012-kafka-replication-and-producer-backpressure.md) (and 0000, 0001, 0004).

## What it simulates

- **Fleet** (`SIM_SITES` x `SIM_STATIONS_PER_SITE`, `SIM_DC_SHARE`): ids, power ratings, site caps and secrets all come from
  `@evcharge/contracts` `fleet` (`stationPower`, `siteCap`, `stationSecret`, `siteIdFor`, `stationIdFor`), the same functions `sessions` seeds
  with. The simulator keeps no copy of the convention. HTTP Basic on the upgrade, 2 connectors per station.
- **Physics** (`src/physics.ts`): AC flat at `min(connector, vehicle, profile limit)`, DC full power to 80% SoC then a linear taper to
  10% at 100%; EVs cannot charge below their minimum (6 A per phase), so a limit under it means 0 W. The energy register is the
  integral of power.
- **Profiles** (`src/profiles.ts`): the OCPP stack. TxProfile (stack 1, `validTo`) beats TxDefaultProfile (stack 0); an expired
  allocation falls back to the default until the gateway sends the safe-share profile. Each connector has its own profile id (`ocpp.txProfileId`),
  so a profile for connector 2 never replaces connector 1's, and the safe share replaces exactly its own connector's allocation.
- **Demand** (`src/demand.ts`): arrivals follow a daily profile with 10% mean and 40% evening-peak utilisation; sessions last ~45 min
  (DC ~25 min). `SIM_TIME_SCALE=60` runs one simulated hour per real minute (station timestamps are simulated time).
- **Station protocol**: Boot, Heartbeat (300 s real), Status, Authorize, Start/Stop, MeterValues (Energy, Power, Current, Voltage, SoC for
  DC) every 30 simulated seconds (`SIM_METER_INTERVAL_S`). Handles RemoteStart/RemoteStop/ReserveNow/CancelReservation/SetChargingProfile/
  TriggerMessage. Start/Stop/MeterValues go through a FIFO queue that survives disconnects: the head is resent with its ORIGINAL uniqueId
  until a CALLRESULT arrives, so offline replay keeps order and may duplicate (the stores are idempotent).
- **Reconnect policy**: first attempt uniform 0-10 s after a drop, then exponential full jitter capped at 60 s; `Retry-After` is a floor.
- **Scale**: one hashed timing wheel (`src/wheel.ts`) drives every timer, so there are no per-station intervals. Measured with the OCPP stub in a
  separate process (`node simulator/scripts/scale-smoke.cjs 10000 60 60`, one JSON line per second in `docs/load-output/simulator-scale-10k-*.jsonl`):
  10,000 stations connected within 5-6 s; at time scale 60 the stub received a peak of about 15,000 msg/s with the simulator's event-loop lag p99 around
  100 ms (see the files for the exact values). The gateway side of the same fleet is in `docs/load-output/gateway-10k-connections.json`.

## Run

```sh
CSMS_URL=ws://localhost:28080/ocpp STATION_SECRET_SEED=dev-station-seed-change-me SIM_SITES=20 SIM_STATIONS_PER_SITE=10 \
  pnpm --filter @evcharge/simulator build && node simulator/dist/main.js     # or: docker compose up simulator
```

Env: `CSMS_URL` and `STATION_SECRET_SEED` are required; `SIM_AUTOSTART` (default true) connects the fleet at `SIM_CONNECT_RATE` per second;
`DRIVER_API_URL` + `SIM_DRIVERS` enable driver traffic (tolerates a missing driver-api); `SIM_PROBE_STATIONS` + `SIM_PROBE_DIR` enable probes;
`SESSIONS_URL` + `INTERNAL_TOKEN` (optional) read the real site caps for the cap gauge, otherwise the cap comes from the shared `fleet` convention in `@evcharge/contracts` (the value `sessions` seeds). `sim_site_cap_from_sessions{site}` is 1 when the cap was read from sessions and 0 when it is the convention value.

## Control API (`SIM_HTTP_PORT`, host 127.0.0.1:28089)

**The control API is lab-only and unauthenticated.** It can disconnect or flood the whole fleet, so compose binds it to `127.0.0.1` only. Never expose it on a shared network.

| Call | Effect |
| --- | --- |
| `GET /status`, `/metrics`, `/health/live`, `/health/ready` | state, Prometheus, probes |
| `POST /fleet/start {stations?, ratePerS?}`, `POST /fleet/stop {stations?}` | ramp the fleet up, or stop stations for good |
| `POST /offline {stations\|ids, minutes}` | network loss for N stations for M simulated minutes (max 1440); sessions continue, messages queue, replay in order |
| `POST /storm {stations?}` | disconnect all at once; each reconnects per the policy |
| `POST /flood {stations, concurrency?, wait?}` | connect M stations as fast as possible |
| `POST /drivers/start {count?, ratePerS?}`, `POST /drivers/stop` | driver-api traffic: register, login, find, reserve, remote start, remote stop |
| `POST /probes {count}`, `GET /probes` | tag evenly spread stations as probes |

**Probe stations** charge continuously (huge battery, 24 h sessions) and write every generated reading to
`$SIM_PROBE_DIR/probe-<run>.jsonl` as `{stationId, connectorId, seq, ts, transactionId?, energyWh, powerW}` with a gap-free per-connector `seq`.
Drills compare this file with what reached Kafka and Timescale. In compose the file stays in the container (`docker compose cp simulator:/tmp/sim-probes .`).

Metrics: `sim_connected_stations`, `sim_ocpp_messages_total{direction,action}` (rate = msgs/s), `sim_site_drawn_watts{site}` vs
`sim_site_cap_watts{site}`, `sim_replay_queue_depth`, `sim_reconnect_attempts_total{kind}`, `sim_connect_failures_total{reason}`,
`sim_call_rtt_seconds`, `sim_active_sessions`, `sim_probe_readings_total`, `sim_driver_requests_total{op,outcome}`.

## Test

```sh
pnpm --filter @evcharge/simulator test:unit   # physics, profile stack, replay queue/ordering, backoff, wheel, protocol against a scripted CSMS
make -o infra-up check                        # int: the simulator against an in-process station-gateway (Redis + Kafka, stub sessions)
```
