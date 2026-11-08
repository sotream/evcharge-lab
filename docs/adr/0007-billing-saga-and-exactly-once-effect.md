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
- Every step runs as `SELECT ... FOR UPDATE` on the saga row, then the effect, then the state
  transition and the outbox write. Workers claim due sagas with `SKIP LOCKED`, so steps for one
  session never run concurrently.

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

The billing load test replays every `session.completed` event 3 times and checks that the number
of invoices equals the number of sessions and that each invoice has one capture.

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
