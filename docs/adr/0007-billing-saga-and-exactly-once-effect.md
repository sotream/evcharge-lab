# 0007. Billing: saga and an exactly-once effect

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: "exactly-once effect: 0 lost, 0 double charges; invoice p99 < 5 min after session end"; ~320,000 invoices/day, 10-year append-only retention; RPO 0 (synchronous commit); final price calculations 15/s.

## Context

A completed session has to produce exactly one invoice and exactly one payment capture, even
though `session.completed` is delivered at least once, the tariff service may be down, the PSP may
time out after charging the card, and billing may crash between steps. The steps span services
(tariff, the PSP, and the event consumers), so one distributed transaction is not available.

## Decision

### Orchestrated saga in `billing`, persisted per session

A `billing_sagas` row keyed by `session_id` holds the state machine:

```
session.started  ─▶ AUTHORIZING ─▶ AUTHORIZED ──(session.completed)─▶ PRICING ─▶ INVOICED ─▶ CAPTURING ─▶ PAID
                        │                                           │                         │
                        └─▶ AUTH_FAILED                             └─▶ (retry w/ backoff)    └─▶ CAPTURE_FAILED ─▶ (release hold, PaymentFailed)
```

- **Authorize at start.** `session.started` places a hold of `HOLD_AMOUNT` (default 50.00 EUR) on
  the driver's payment method through the PSP adapter, with
  `idempotency-key = auth:{sessionId}`. A session without a driver (an RFID idTag that is not
  linked) skips this step and is invoiced as unpaid.
- **Price.** `session.completed` → `POST tariff/v1/quote`. The quote is deterministic (ADR 0011),
  so retrying it is safe. The call has a 1 s timeout, 3 attempts and a circuit breaker. While
  tariff is down, the saga stays in `PRICING` and a scheduler retries with capped backoff (1 s → 30 s), so a tariff outage under about 4 min still meets "invoice p99 < 5 min" (ADR 0000). Longer outages keep retrying for up to 24 h, and the backlog is visible as `billing_sagas_pending{state="PRICING"}`.
- **Invoice.** Within one transaction: insert the invoice (`UNIQUE(session_id)`), advance the saga,
  and add `billing.invoice-issued` to the outbox. A duplicate event hits the unique constraint or
  the inbox, so nothing happens.
- **Capture.** Capture `min(total, hold)` with `idempotency-key = capture:{invoiceId}`. When the PSP
  answers with a timeout, it is **unknown, not failed**. The saga stays `CAPTURING` and retries
  with the same key. The PSP deduplicates on the key, so a capture that actually succeeded is
  returned, not repeated. A definite decline moves to `CAPTURE_FAILED`, which releases the hold
  (compensation) and emits `billing.payment-failed`.
- **Remainder above the hold** (erratum, 2026-11-08). A total above the hold, for example a 150 kWh DC
  session, would otherwise be under-collected. After the hold capture, the saga captures
  `total − hold` by payment token with `idempotency-key = capture:{invoiceId}:rest`, under the
  same unknown-outcome rules. The exact capture request (token, amount) is persisted on the first
  attempt and replayed on every retry, so a card change mid-retry cannot strand a charge. The
  invoice is `Paid` only when both captures succeed.
- **Giving up on authorization.** After repeated unknown authorize outcomes, the saga voids the
  possible hold through the same idempotency key before it proceeds without a hold.
- Workers claim due sagas with `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)` and a lease
  (`SAGA_LEASE_MS`, 60 s by default). No row lock is held while a step calls the PSP or the tariff service. A step
  that outlives its lease can run beside a second worker; that is safe because every external call carries an
  idempotency key and each state transition is a guarded `UPDATE ... WHERE state = ...` (a lost race
  throws `StateChanged` and does nothing). The earlier text of this ADR described a row lock; the code never had one.

### PSP

The PSP is a mock adapter inside billing behind a `PaymentProvider` port, with its own table of
idempotency keys. It simulates success, decline (configurable card tokens) and latency or timeout
injection (`PSP_TIMEOUT_RATE`) to exercise the unknown-outcome path. A real PSP (Stripe, Adyen)
would plug into the same port. Those PSPs support idempotency keys, which this design depends on.

### "Exactly-once effect"

Delivery stays at-least-once. The effects become exactly-once through three things:

1. An inbox on `(consumer, eventId)`.
2. Unique business keys (`invoice.session_id`, `capture:{invoiceId}`).
3. Idempotency keys passed to the external side.

The billing integration test "issues exactly one invoice and one capture per session"
(`services/billing/test/int/saga.test.ts`, two replicas, 40% PSP timeouts) replays every `session.completed` event
3 times and checks that the number of invoices equals the number of sessions and that each invoice has one capture.
There is no billing script in `load/k6`, so billing throughput is not measured.

## Rejected alternatives

- **Choreography** (tariff consumes events and emits priced events). It would spread one
  business process over three services and give no single place to see a session's billing state
  or to drive compensation.
- **2PC/XA across Postgres and the PSP.** No PSP supports it.
- **Capturing in the same transaction that creates the invoice.** An HTTP call inside a database
  transaction holds locks for the duration of the network call. On timeout the system cannot tell
  whether the money moved.

### Retention and volume

`invoices` and `invoice_lines` are append-only, kept for 10 years, and partitioned by month on `issued_at`. Corrections are credit notes, not updates. At about 320,000 invoices/day [ADR 0000], that is about 3.7/s on average and 15/s at peak, well within one Postgres. The billing load test measures sagas/s per instance.

## Consequences

- Billing has a background worker as well as consumers. Saga state is queryable for the runbook
  (`SELECT state, count(*) FROM billing_sagas GROUP BY 1`).
- A session completed while tariff is down gets its invoice late, not never. The billing integration test
  `services/billing/test/int/saga.test.ts` (tariff outage case, "retries with backoff while tariff is down, then invoices
  and captures once") shows the saga waiting in PRICING and completing after recovery. There is no tariff-down drill in
  `drills/`.
- A saga that gives up in PRICING (tariff rejects the quote for good, or the 24 h deadline passes) still holds the
  authorization. It moves to CAPTURE_FAILED so `stepRelease` frees the hold, and `billing.payment-failed` is emitted
  with step `pricing`. Before the November 2026 review it only stopped retrying and the hold stayed on the card.
  Test: "a quote the tariff service rejects for good releases the hold".

## Amendment (November 2026 production-readiness review)

Plan `docs/superpowers/plans/2026-11-10-prod-readiness.md`, Tasks 3, 7, 8, 9, 10 and 12.

- **Energy review (migration `0005_energy_flag`).** `session.completed` carries `energyFlag` from sessions. Any flag other
  than `ok` (`implausible`, `meter_regressed`, `stale`) keeps the saga in PRICING with `next_attempt_at` NULL and
  `failure_reason = energy_review:<flag>`, and emits `billing.payment-failed` with step `pricing`. The hold stays on the
  card until an operator decides. `GET /internal/sagas/parked` lists such sagas, `POST /internal/sagas/:sessionId/resume`
  (body `{ "acceptEnergy": true }` to confirm the reading) restarts them.
- **Invoice numbers and VAT (migration `0007_invoice_number_vat`, `VAT_RATE_BP` default 1900, `INVOICE_PREFIX` default
  `EVC`).** Numbers are `EVC-<year>-NNNNNN`, gapless per calendar year in Europe/Berlin: the `invoice_counters` row of
  (`invoice`, year) is incremented inside the invoice transaction after the transaction owns the registry row, so the
  adopt-an-existing-invoice branch takes no number and a rollback returns it. The number is UNIQUE on
  `invoice_registry` (the partitioned `invoices` cannot hold a global UNIQUE). The sequence is zero-padded to at least
  six digits and grows wider above 999,999 a year, so order numbers by their numeric suffix, not as text. Tariff prices
  are gross (B2C price display, PAngV): `vat = round_half_up(gross × rate / (10,000 + rate))`, `net = gross − vat`,
  enforced by a CHECK. The counter row serialises invoice issuing; at roughly 100–200 invoices/s that is well above the
  ADR 0000 design peak of 45 stops/s.
- **Dunning (migration `0006_capture_retry`, `CAPTURE_RETRY_DELAYS_H` default `24,72`, `none` disables it).** A capture
  declined for a card reason (`DUNNABLE_DECLINE_REASONS` in `domain.ts`) stays CAPTURING and is retried 24 h and 72 h
  after the first decline. Each round uses a new idempotency key (`capture:<invoiceId>[:rest]:r<round>`; round 0 keeps
  the original key) and reads the driver's current card once (`capture_token_round`), so a replaced card is used and the
  timeout retries of a round never send two tokens under one key. Each scheduled round emits a non-final
  `billing.payment-failed` with step `capture_retry`. Any other decline, or the end of the schedule, takes the old
  path: CAPTURE_FAILED, one final PaymentFailed, hold released.
- **Credit notes (migration `0008_credit_notes`).** `POST /internal/invoices/:invoiceId/credit-notes` `{ amountMinor,
  reason }` with a required `idempotency-key`. One transaction locks the invoice's registry row, refuses credits above
  what is left of the total (409), takes a gapless `EVC-CN-<year>-NNNNNN` number from `invoice_counters`, splits VAT at
  the invoice's rate, plans the refund across the invoice's captures (newest first, never more than is left of a
  capture) and emits `billing.credit-note-issued`. The PSP refunds run afterwards with the key
  `refund:<creditNoteId>:<pspReference>`; a PSP timeout leaves a refund `pending` and repeating the request finishes it.
  `credit_notes` is append-only; refund state lives in `credit_note_refunds`. `PaymentProvider` gained `refund()`.
- **Pricing by metered energy (`TELEMETRY_URL`).** Before quoting, billing fetches the session's 1-minute power series
  from telemetry-ingest (2 s timeout, any error means no profile) and sends it as `energyProfile`; tariff then splits
  time-of-use energy by metered energy instead of by time (ADR 0011). Without a profile the quote is time-weighted, as
  before.

