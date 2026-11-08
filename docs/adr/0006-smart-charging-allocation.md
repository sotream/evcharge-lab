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
| Safe share | TxProfile, stackLevel 1, no expiry | `safeShareW = floor(cap / connectorsAtSite)` | the owning gateway, 5 s after the **whole site** is stale (`alloc:site:{id}` in Redis) | every allocation profile at the site has expired, so all connectors are at ≤ startW; Σ safeShareW over all connectors ≤ cap |

- **Starting a session.** The station charges at `startW` immediately, which meets "safe minimum
  until allocated". `sessions` triggers a reallocation on `StartTransaction`. The target is p99
  < 5 s to the new TxProfile.
- **Allocator, `sessions` or broker down.** Allocation profiles stop being refreshed. The station
  drops to `startW` at `validTo`, which is always ≤ the allocation. Once every profile at the site has
  expired, the gateway raises the connectors to `safeShareW` 5 s later, using the site-wide rule
  below.
  - The 5 s grace absorbs station clock skew. The gateway never raises a connector while any
    allocation at its site may still be in force.
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
- It sends `SetChargingProfile` only when a limit changed by at least max(2%, 200 W), or when the
  profile is within 60 s of expiry, and it applies changes in two phases (below). That refresh bounds the outbound rate at about 900/s
  [ADR 0000]. Each round emits `site.power-allocated`.

### Algorithm (max-min fair water-filling with minimums and start reserve)

1. **Inputs.** Each active session has `minW` (6 A × 230 V × phases), `maxW` (connector and
   vehicle), an optional `measuredW`, and a `priority`.
2. **Start reserve.** Compute `reserve = idleConnectors × startW` and allocate against
   `budget = cap − reserve`. Because `startW ≤ cap / connectorsAtSite`, `reserve ≤ cap` and
   `budget ≥ 0`.
3. **Effective demand.** `demand = maxW`, or after 60 s with a measured value,
   `min(maxW, max(minW, 1.1 × measuredW + 500))`. Headroom then goes to cars that can use it.
4. **Admission.** Order by `(priority desc, startedAt asc, sessionId)` and admit the next session
   while `Σ minW(admitted) + startW × (sessions not yet admitted) ≤ budget`. Sessions that do not
   fit get `limitW = startW` and `suspended: true`. The reserve covers idle connectors only, so
   suspended sessions must be budgeted at `startW` explicitly. *Erratum (2026-11-08): the first
   version admitted while `Σ minW ≤ budget` and claimed suspended sessions were already in the
   reserve. That is false: cap 10,000 W, three sessions at minW 4,140 W, startW 3,333 W gives
   4,140 + 2 × 3,333 = 10,806 W.* Established sessions are still not starved by new arrivals.
5. **Water-filling.** Give every admitted session `minW`. Distribute the remaining budget equally
   among sessions whose demand is not yet met, cap each at its demand, and redistribute the
   excess. This is O(n log n).
6. **Arithmetic and invariant.** int64 W, rounded down. Every response asserts
   `Σ limitW (including suspended sessions at startW) + reserve ≤ cap`, and the service returns 500
   rather than violate it. A property test runs 10^5 random sites with up to 80 connectors.

### Profile ids and the site-wide staleness rule (erratum, 2026-11-08 after review)

- **One profile id per connector.** `txProfileId(connectorId) = 100 + connectorId`, shared by the
  allocation and the safe-share profile of that connector. TxDefaultProfile uses id 1 on
  connectorId 0. OCPP 1.6 replaces a profile with the same `chargingProfileId` whatever the
  connector. With the first version's single id 2, connector 2's allocation deleted connector 1's.
- **The safe share is raised per site, not per connector.** Raising one connector as soon as its
  own allocation expired could exceed the cap. Example: cap 30 kW, 3 connectors, safe share
  10 kW, startW 4.14 kW. A holds a live 20 kW allocation, B's refresh failed and B is raised to
  10 kW, and C is idle at startW: 20 + 10 + 4.14 = 34.1 kW.
  - Before a round commits, `sessions` raises `alloc:site:{siteId}` in Redis to the latest
    `validTo` that may be in force at the site. It only ever increases the value. A Redis failure
    aborts the round, so no profile is sent unannounced.
  - A gateway raises a connector to `safeShareW` only when `now > alloc:site + 5 s`. At that
    point every allocation profile at the site has expired, every connector is at most `startW`,
    and raising all of them to `safeShareW` stays within the cap.
  - A missing key or a failed read means no raise. Stations stay at `startW`, which is safe.

### Applying an allocation (two-phase, added 2026-11-08 after review)

A correct allocation is not enough. Profiles reach different stations through different
partitions and gateways, so there is no ordering between them, and a decrease can fail to apply
(`StationOffline`, `Timeout`, `Rejected`). Applying an increase before the matching decrease has
taken effect can exceed the cap. For example: cap 30 kW, A at 22 kW, a round gives 15 kW to both A
and B, A's decrease is lost, and A and B together draw 37 kW. `sessions` therefore applies in two
phases:

1. For every session, `sessions` tracks the **in-force limit**. That is the last Accepted
   profile's limit until its `validTo`, or `startW` before any profile. A non-Accepted or pending
   decrease does not lower it.
2. A round enqueues all decreases at once. It enqueues an increase only if
   `cap − Σ max(in-force, pending) of all other sessions − idle reserve` still covers it. Increases
   that do not fit wait for the next round, and a SetChargingProfile result kicks the site again.
3. If an increase is held back, the session keeps its in-force limit, which is always safe. If a
   decrease is never applied, the old profile expires at `validTo`. The station then drops to
   `startW`, and the gateway safe share takes over.

Change threshold: a profile is re-sent only when `|Δ| ≥ max(2% of the old limit, 200 W)` (both
thresholds), or within 60 s of `validTo`. This keeps measured-demand jitter from re-sending every
profile of a site every round.

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
