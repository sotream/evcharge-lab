# ADR 0000: Requirements, load model and SLOs

- Status: accepted
- Date: 2026-11-08

## Context

Every later ADR picks technologies and topologies. Those choices are only defensible against explicit numbers. This ADR fixes the load model, data volumes and SLOs for the whole platform. Every figure carries a label:

- `[assumption]`: an input chosen for this lab, plausible for a mid-sized German charge point operator (CPO), not taken from a real company;
- `[derived]`: calculated from assumptions, with the formula shown;
- `[measured]`: filled in later from `docs/load-output/` or `docs/drill-output/`; never typed by hand.

## Fleet and usage

| Input | Value | Label |
|---|---|---|
| Charge points (stations), each with one persistent OCPP WebSocket | 50,000 | [assumption] |
| Connectors per station | 2 → 100,000 connectors | [assumption] |
| Stations per site | 10 avg, 40 max → 5,000 sites | [assumption] |
| Power mix | 80% AC 11/22 kW, 20% DC 50-150 kW | [assumption] |
| Connector utilisation | 10% average, 40% peak (evening) | [assumption] |
| Session length | 45 min average (2,700 s) | [assumption] |
| Registered drivers / daily active | 2,000,000 / 200,000 | [assumption] |
| Station offline window (buffers locally, replays on reconnect) | up to 24 h | [assumption] |

## Traffic

| Flow | Average | Peak | Formula | Label |
|---|---|---|---|---|
| Concurrent sessions | 10,000 | 40,000 | connectors × utilisation | [derived] |
| Session starts (= stops) | 3.7/s | 15/s, design 45/s (×3 burst) | concurrent / 2,700 s | [derived] |
| Sessions per day | ~320,000 | | 10,000 × 86,400 / 2,700 | [derived] |
| Heartbeat (every 300 s) | 167/s | 167/s | 50,000 / 300 | [derived] |
| MeterValues (every 30 s per active connector) | 333/s | 1,333/s | concurrent / 30 | [derived] |
| StatusNotification (~5 per session) | 19/s | 75/s | starts × 5 | [derived] |
| **Inbound OCPP total** | ~550/s | **~1,600/s, design 2,400/s (×1.5)** | sum | [derived] |
| Reconnect storm after a gateway deploy or crash (all stations within 60 s) | | 833 connects/s, ~2,500 msg/s (Boot + 2 Status) | 50,000 / 60 | [derived] |
| Smart charging recomputations | | ~115/s | starts+stops 30/s + 5,000 sites / 60 s periodic | [derived] |
| SetChargingProfile outbound | | ~900/s | 115 × ~8 active connectors per busy site | [derived] |
| Driver API reads (map, availability, session status) | 100 RPS | 500 RPS | [assumption] | [assumption] |
| Driver API writes (reserve, start, stop) | 10 RPS | 50 RPS | [assumption] | [assumption] |
| Tariff quotes + final price calculations | | ~100/s | quotes + 15 stops/s | [derived] |

## Data volume

| Data | Volume | Retention | Label |
|---|---|---|---|
| Meter values, ~200 B per row | 28.8 M rows/day ≈ 5.8 GB/day | raw 90 days (~520 GB), 1-min rollups 2 years | [derived] |
| Sessions, ~1 KB per row | ~320 MB/day ≈ 117 GB/year | 10 years (German tax law) | [derived] |
| Billing records and invoices | ~320,000/day | 10 years, append-only | [derived] |
| OCPP raw message log | ~550 msg/s × ~300 B ≈ 14 GB/day | 14 days | [derived] |

## SLOs

| Area | Objective |
|---|---|
| Remote start | driver tap → RemoteStartTransaction delivered to the station: p99 < 2 s; confirmed charging: p99 < 5 s |
| OCPP responses | CALLRESULT for Authorize / StartTransaction / StopTransaction: p99 < 500 ms |
| Telemetry | meter value durable p99 < 5 s, visible in driver API < 10 s; loss ≤ 0.01% in normal operation |
| Driver API | 99.9% monthly availability (43 min budget); read p99 < 300 ms, write p99 < 800 ms |
| Station gateway | 99.95%; after losing one instance, its stations are reconnected and routable within 60 s |
| Reservations | zero double bookings of one connector; reservation TTL 15 min |
| Billing | exactly-once effect: 0 lost, 0 double charges; invoice p99 < 5 min after session end |
| Smart charging | the site power cap is never exceeded by allocation; reallocation after session start/stop p99 < 5 s; a new session starts at a safe minimum profile until allocated |
| Degradation | if the allocation service is down, gateway applies a static safe share (cap / connectors); charging never stops because the backend is down |
| Durability | RPO 0 for sessions and billing (synchronous commit); RPO ≤ 5 min for telemetry. **Amended by ADR 0018:** RPO 0 holds on one node (argued, not re-measured) and across node loss only with a synchronous standby; with the lab's asynchronous replica it is missed (281 and 282 acknowledged writes lost in two runs when the replica was unreachable for 3 s before the primary died). |

## Lab targets (one instance, MacBook Air M1, docker compose)

The full fleet cannot run on one laptop. We measure one instance per service and derive the fleet size.

| Service | Per-instance target | Fleet need | Instances (N+1) | Label |
|---|---|---|---|---|
| Station gateway | ≥ 10,000 WebSocket connections, ≥ 2,000 msg/s | 50,000 conns, 2,400 msg/s | 6 | target; derived |
| Telemetry ingest | ≥ 2,500 rows/s sustained | 1,333 rows/s peak | 2 | target; derived |
| Sessions / reservations | ≥ 200 commands/s | 45 starts/s + 50 writes/s | 2 | target; derived |
| Smart charging (Go) | ≥ 1,000 site allocations/s, p99 < 20 ms | 115/s | 2 | target; derived |
| Tariffs (Go) | ≥ 2,000 calculations/s, p99 < 20 ms | 100/s | 2 | target; derived |
| Driver API | ≥ 1,000 RPS reads at p99 < 300 ms | 500 RPS | 2 | target; derived |

Rate rule (amended 2026-11-09): k6 reports `http_reqs.rate` as the request count divided by the whole test run duration, start-up and shutdown included, so a constant-arrival-rate run offered at exactly the target reports a rate a fraction below it (for example 199.99/s against 200/s). A throughput target measured this way is met when the run dropped no iteration, no request failed and the reported rate is at least 99.9% of the target. Every other target, including every latency, loss and drill check, is compared with no tolerance. A run that dropped iterations misses its throughput target whatever the rate.

README charts show target vs `[measured]` per service. A target that was missed stays in the table as missed, with the bottleneck named.

## Non-goals

OCPI roaming, OCPP 2.0.1, ISO 15118 Plug & Charge, a real payment provider (a mock PSP with latency and failure injection), multi-region active-active (written up as a derived scaling path only), grid-operator or energy-market integration.

## Consequences

- ADR 0001 and every service ADR cite the rows above that drive the decision (for example telemetry store choice ← 5.8 GB/day and 1,333 rows/s; gateway routing ← 50,000 sticky connections, 6 instances).
- Load tests in `load/` and drills in `drills/` use these targets as pass/fail thresholds.
- If a measurement shows a target is impossible on this hardware, the ADR is amended with the measured number rather than the target being quietly lowered.
