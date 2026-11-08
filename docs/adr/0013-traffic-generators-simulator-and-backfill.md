# 0013. Traffic generators: station simulator and telemetry backfill

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: 50,000 stations / 5,000 sites; power mix 80% AC 11/22 kW, 20% DC 50–150 kW;
  utilisation 10% average, 40% evening peak; 45 min sessions; offline window up to 24 h; reconnect storm
  833 connects/s; meter values 28.8 M rows/day, raw 90 days.

## Context

Every measured number in this repo comes from traffic that has to look like the load model. Two
different questions need two tools:

1. Live behaviour: protocol, allocation, storms and replays. This needs real WebSocket stations.
2. Storage at volume: compression ratio, chunk size and query latency on days of history. Producing
   that through the live path would take days of wall-clock time.

## Decision

### `simulator/` (Node): live virtual stations over OCPP

- **Fleet shape.** Configured by `SIM_SITES`, `SIM_STATIONS_PER_SITE`, the AC/DC mix and the time
  acceleration `SIM_TIME_SCALE` (1 = real time; 60 = one simulated hour per minute). The credential
  seed matches `sessions` (ADR 0003).
- **Power physics.**
  - AC 11 or 22 kW: a flat curve at `min(connector max, vehicle max, profile limit)`.
  - DC 50–150 kW: a SoC curve with full power up to about 80% SoC, then a linear taper to about 10%
    of max at 100%.
  - Energy is integrated from power on each tick, so the energy register is consistent with power
    samples.
- **Demand.** Session arrivals follow a daily profile with a 10% average and a 40% evening peak of
  connector utilisation. Durations are drawn around 45 min, and DC sessions are shorter.
- **Smart charging is obeyed.** Each connector applies the OCPP stack (TxDefaultProfile at
  level 0, TxProfile at level 1 with `validTo`). The drawn power is
  `min(curve, active profile limit)`, so the measured site sum reflects real enforcement (ADR 0006).
- **Modes, controlled through the simulator HTTP API on port 28089 and env:**
  - `offline`: drop the socket for N minutes up to 24 h, queue transaction messages, replay them in
    order with their original uniqueIds (ADR 0003).
  - `storm`: disconnect all stations at once and reconnect with the ADR 0003 policy.
  - `flood`: connect M stations as fast as possible.
  - `drivers`: driver-api traffic (register, reserve, remote start and stop).
- **Probe stations.** A tagged subset whose every reading is logged with a sequence number. The
  drills use it to prove zero loss or zero gaps end to end.
- Metrics on `/metrics`: connected stations, messages/s by action, the site power sum versus the cap,
  and replay queue depth.

### `scripts/backfill-telemetry` (Go): history straight into TimescaleDB

- Writes the wide `meter_readings` format (ADR 0008) with `COPY`, using the same power physics as
  the simulator: AC flat, DC SoC taper, daily utilisation profile.
- Default run: **7 days × 5,000 stations (10% of the fleet)**, configurable.
- It measures and writes to `docs/load-output/telemetry-backfill-<timestamp>.json`:
  - rows/s;
  - size before and after compression;
  - the compression ratio;
  - chunk count and sizes;
  - p50/p95/p99 latency of five queries (300 runs each): the latest value per connector of one station, one
    session's readings by range on recent and on compressed chunks, and the 1-minute rollup of one site and one day on
    recent and on compressed chunks.
- Full-fleet extrapolation (50,000 stations, 90 days) is written as `[derived]` with its formula,
  next to the `[measured]` 10% figures.

## Rejected alternatives

- **Using only the simulator for storage measurements.** Seven simulated days at peak rate would
  take hours even at a high `SIM_TIME_SCALE`, and it would load the gateway path that is being
  measured separately.
- **Using k6 alone for stations.** k6 WebSockets are fine for raw connection and message-rate tests,
  and those are used in `load/`. Modelling SoC curves, profile enforcement and offline queues is
  application logic, and it belongs in a real program.
- **A Python data generator.** No third language is allowed. Go COPY via pgx is faster anyway.

## Consequences

- The README charts for compression, chunk sizes and query latency come from backfill output. The
  charts for allocation, storms and replays come from simulator runs. Each is labelled with its
  source file.
