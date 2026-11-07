# 0011. Tariff model and price calculation

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: ~100 tariff calculations/s fleet peak; lab target ≥ 2,000 calculations/s, p99 < 20 ms per instance; billing exactness ("0 double charges") requires deterministic prices.

## Context

The price of a session depends on energy, duration, time of day and fixed fees. Billing has to be
able to recompute it on retry and get an identical result, so the calculation must be
deterministic, free of floating-point money, and explicit about rounding and about how a session
that crosses a time-of-use boundary is split.

## Decision

- A tariff (stored in Postgres, `tariff` database, versioned with `valid_from`) has:
  - `currency` (ISO 4217);
  - `session_fee_minor`;
  - `energy_price_minor_per_kwh` (int, with optional TOU windows
    `[{days, from 'HH:MM', to 'HH:MM', price_minor_per_kwh}]` in the tariff's IANA `timezone`);
  - `time_price_minor_per_hour`;
  - `idle_fee_minor_per_min` after `idle_grace_min`;
  - `min_billable_wh`.
- **Energy across TOU windows.** Energy is attributed to windows pro rata by time overlap, because
  the quote has the total energy but not the power curve. The split uses integer watt-hours with
  the largest-remainder method, so the parts sum exactly to the total. Each window's line is
  `ceil(wh × price / 1000)` in minor units. Rounding is per line, up, in the operator's favour,
  and documented on the invoice line.
- **Idle.** Idle is time after the vehicle stopped drawing power. The quote API takes total
  duration only, so idle time is not billed in v1. The structure exists, and the line appears with
  quantity 0. This keeps the contract stable for when billing passes a power curve.
- Totals are int64 minor units. Every response is a pure function of `(tariff version,
  request)`. The tariff in force at `startedAt` applies to the whole session.
- Seeded tariffs: `std` (EUR, 0.39/kWh, 0.49 session fee), `tou` (EUR, peak 0.49 / off-peak
  0.29, Europe/Kyiv), `fleet` (EUR, 0.25/kWh, no fee).
- `GET /v1/tariffs/:id` is cached in-process for 60 s. Quotes are CPU-only after the tariff is
  loaded.

## Rejected alternatives

- **Floating-point or decimal money.** Floats are not deterministic across languages and
  roundings. A decimal library is unnecessary when every amount fits in int64 minor units.
- **Rounding only the total.** It cannot be explained line by line on an invoice, and the lines
  would not sum to the total.
- **Pricing from the full meter time series.** That would be more accurate for TOU, but it couples
  tariff to telemetry and makes quotes depend on a large, mutable input. Revisit this when idle
  fees are needed.

## Consequences

- Golden tests pin quotes for boundary cases: across midnight, across a TOU window, DST
  transitions in Europe/Kyiv, zero energy, and minimum billable energy.
- Changing a price creates a new tariff version. Old sessions reprice with the version they
  started under.
