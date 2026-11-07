# 0006. Smart charging: power allocation under a hard site cap

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: 5,000 sites (10 stations avg, 40 max, 2 connectors each); ~115 recomputations/s;
  ~900 SetChargingProfile/s; reallocation p99 < 5 s; "the site power cap is never exceeded by allocation";
  "a new session starts at a safe minimum profile until allocated"; "if the allocation service is down,
  gateway applies a static safe share (cap / connectors); charging never stops because the backend is down";
  lab target ≥ 1,000 allocations/s, p99 < 20 ms per instance.

## Context

Each site has a grid cap `siteCapW`. Two cases can push the sum of limits over the cap:

- A session can start at any time, before any allocator has seen it.
- The allocator, `sessions`, the broker or the gateway's upstream can be down.

ADR 0000 requires both cases to stay under the cap, and charging to continue without the backend.
Sites have up to 80 connectors.

## Decision

### Three profile layers, all in W (constants in `ocpp.ts` `ChargingProfiles`)

| Layer | OCPP profile | Limit | Who sets it | Why it is safe |
|---|---|---|---|---|
| Start default | TxDefaultProfile, stackLevel 0, no expiry | `startW = min(minW, floor(cap / connectorsAtSite))` | gateway, right after an accepted BootNotification | the allocator reserves `startW` for every idle connector (below), so a start never exceeds the cap |
| Allocation | TxProfile, stackLevel 1, `validTo = computedAt + 180 s` | from the allocator | `sessions` through `station.commands` | Σ allocations ≤ cap − Σ idle reserves |
| Safe share | TxProfile, stackLevel 1, no expiry | `safeShareW = floor(cap / connectorsAtSite)` | the owning gateway, locally, 5 s after an allocation profile expires with nothing newer | Σ over all connectors ≤ cap, whatever the others do |

- **Starting a session.** The station charges at `startW` immediately, which meets "safe minimum
  until allocated". `sessions` triggers a reallocation on `StartTransaction`. The target is p99
  < 5 s to the new TxProfile.
- **Allocator, `sessions` or broker down.** Allocation profiles stop being refreshed. The station
  drops to `startW` at `validTo`, which is always ≤ the allocation. The gateway then raises the
  connector to `safeShareW` 5 s later.
  - All profiles of one allocation round share one `validTo`, so connectors on one site fall back
    together. The 5 s grace absorbs station clock skew, and the gateway only ever raises a
    connector after its allocation has expired.
  - This is the "static safe share (cap / connectors)" from ADR 0000. It needs nothing but the
    gateway, which got `chargingDefaults` with the station credential (ADR 0003).
  - Charging never stops.
- **Recovery.** The next successful allocation round sends new TxProfiles at stackLevel 1. They
  replace the safe share, because the profile id and stack level are the same.

### Responsibilities

- `smart-charging` (Go) is a **stateless, pure allocator**: `POST /v1/allocate` takes
  `{siteCapW, sessions[], idleConnectors, startW}` and returns limits. It has no database and
  scales horizontally, and the same request always gets the same answer.
  - Load: ~115 allocations/s at fleet peak against ≥ 1,000/s per instance [target]. 2 instances
    give N+1.
- `sessions` owns site state. It reallocates a site:
  - on every session start or stop;
  - every 60 s per site, with measured power from Redis (`power:{station}:{connector}`, written by
    telemetry-ingest). 5,000 sites / 60 s gives about 83/s [derived].
- It sends `SetChargingProfile` only when a limit changed by at least 2% or 200 W, or when the
  profile is within 60 s of expiry. That refresh bounds the outbound rate at about 900/s
  [ADR 0000]. Each round emits `site.power-allocated`.

### Algorithm (max-min fair water-filling with minimums and start reserve)

1. **Inputs.** Each active session has `minW` (6 A × 230 V × phases), `maxW` (connector and
   vehicle), an optional `measuredW`, and a `priority`.
2. **Start reserve.** Compute `reserve = idleConnectors × startW` and allocate against
   `budget = cap − reserve`. Because `startW ≤ cap / connectorsAtSite`, `reserve ≤ cap` and
   `budget ≥ 0`.
3. **Effective demand.** `demand = maxW`, or after 60 s with a measured value,
   `min(maxW, max(minW, 1.1 × measuredW + 500))`. Headroom then goes to cars that can use it.
4. **Admission.** Order by `(priority desc, startedAt asc)` and admit sessions while
   `Σ minW ≤ budget`. Sessions that do not fit get `limitW = startW` and `suspended: true`. They
   were already counted in the reserve at that rate, so this is still safe, and established
   sessions are not starved by new arrivals.
5. **Water-filling.** Give every admitted session `minW`. Distribute the remaining budget equally
   among sessions whose demand is not yet met, cap each at its demand, and redistribute the
   excess. This is O(n log n).
6. **Arithmetic and invariant.** int64 W, rounded down. Every response asserts
   `Σ limitW + reserve ≤ cap`, and the service returns 500 rather than violate it. A property test
   runs 10^5 random sites with up to 80 connectors.

### Contract change

`AllocateReq` gains `idleConnectors` and `startW`. The `AllocateRes` allocation gets `limitW`
equal to `startW` for suspended sessions. Both are in `packages/contracts`.

## Rejected alternatives

- **Fallback equal share among active sessions.** When another session starts before the next
  decision, the old limits plus the new one exceed the cap. That violates "never exceeded".
- **`sessions` applies the fallback.** It needs `sessions` and the broker to be up, but ADR 0000
  requires charging to continue when the backend is down. Only the gateway, or the station
  itself, can guarantee this.
- **No start reserve, reallocate fast instead.** At 45 starts/s, a p99 of 5 s means about 225
  sessions run unallocated at any moment. Without a reserve, each one can overshoot its site.
- **Proportional to maxW, or an LP solver.** Proportional ignores minimums and starves AC
  connectors next to DC ones. LP is unnecessary for one linear constraint with box bounds, where
  water-filling is max-min optimal.
- **A stateful allocator consuming events.** It would need its own state rebuild and failover by
  site. `sessions` already owns the state.

## Consequences

- An idle connector costs `startW` of headroom: up to about 4.1 kW (3-phase minimum) or
  cap/connectors if that is lower. On a busy site the reserve shrinks as connectors fill. The
  allocation chart in the README shows the reserve explicitly.
- Correctness depends on stations honouring stack levels and `validTo`. The simulator does, and
  the "smart-charging down" drill checks that the measured site sum stays at or below the cap
  before, during and after.
