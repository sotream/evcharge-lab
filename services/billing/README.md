# billing

Orchestrated saga per session: authorize a hold at `session.started`, price at `session.completed`
(tariff `POST /v1/quote`), issue one append-only invoice, capture `min(total, hold)` through a PSP
port, and compensate (release the hold, emit `billing.payment-failed`) on a definite decline.
Delivery is at-least-once; the effects are exactly-once (inbox, unique business keys, PSP
idempotency keys). Governing ADRs: 0007 (saga), 0002 (events), 0010 (auth), 0011 (tariff), 0012.

- Saga table `billing_sagas` (states AUTHORIZING, AUTHORIZED, AUTH_FAILED, PRICING, INVOICED,
  CAPTURING, PAID, CAPTURE_FAILED). Workers claim due rows with `FOR UPDATE SKIP LOCKED` and a lease,
  so replicas share work and no transaction is open across a network call.
- Tariff down: the saga stays in `PRICING`, retried with capped backoff (1 s to 30 s) for up to 24 h.
  A PSP timeout is an unknown outcome: the same idempotency key is retried.
- Capture: the request (token and amounts) is fixed when the saga enters `CAPTURING` and replayed on
  every retry, so a card change cannot turn a retry into a different request. The hold part
  (`min(total, hold)`, key `capture:{invoiceId}`) is followed by the remainder above the hold, captured
  by token with key `capture:{invoiceId}:rest`. The invoice is `Paid` only when both succeed; a declined
  remainder gives `PaymentFailed` and leaves the captured hold part in place. An idempotency mismatch
  reported by the PSP parks the saga (`billing_psp_idempotency_mismatch_total`, logged at error).
- Authorization: after 5 unknown outcomes the hold is voided through the authorize idempotency key
  (retried until the PSP answers), then the saga continues without a hold.
- Sessions without a driver are invoiced as unpaid (saga ends in `INVOICED`). A `session.completed`
  without a `session.started` is priced and charged directly (no hold).
- `invoices` and `invoice_lines` are append-only (trigger) and partitioned monthly on `issued_at`
  (created at startup and every 6 h; a default partition is the safety net). "One invoice per
  session" is enforced by `invoice_registry` (a unique constraint on a partitioned table must
  contain the partition key).
- The mock PSP lives in the same database (`psp_*` tables). Failure injection: `PSP_TIMEOUT_RATE`
  (half of the timeouts happen after the card was charged), `PSP_DECLINE_TOKENS` (capture
  declined), `PSP_AUTH_DECLINE_TOKENS` (authorization declined), `PSP_LATENCY_MS`.

## API (all routes need `x-internal-token`; `x-driver-id` is only honoured with it)

| Route | Notes |
| --- | --- |
| `PUT /v1/drivers/:driverId/payment-method` | body `{token}`; 204. With `x-driver-id` it must equal `:driverId` (403) |
| `GET /v1/invoices?driverId=&limit=&before=` | `{items: Invoice[], nextBefore}`, newest first; `driverId` defaults to `x-driver-id` |
| `GET /v1/invoices/:invoiceId` | invoice plus `lines`; another driver's invoice is 404 |

Metrics (`/metrics`): `billing_sagas_pending{state}`, `billing_invoice_latency_seconds`
(stoppedAt to issued, SLO p99 < 5 min), `billing_captures_total{outcome}`,
`billing_psp_calls_total{operation,outcome}`, `billing_saga_steps_total`, `billing_sagas_parked_total`.
Runbook query: `SELECT state, count(*) FROM billing_sagas GROUP BY 1`.

## Configuration

Required: `SERVICE_NAME`, `INTERNAL_TOKEN`, `DATABASE_URL`, `KAFKA_BROKERS`, `TARIFF_URL`. Everything
else has a default, see `src/config.ts` (`HOLD_AMOUNT=5000`, `SAGA_*`, `TARIFF_*`, `PSP_*`).

## Run and test

```sh
pnpm --filter @evcharge/billing build
pnpm --filter @evcharge/billing test:unit
make -o infra-up test-int            # compose Postgres + Kafka must be running
docker build -f services/billing/Dockerfile .
```

The integration tests replay every `session.completed` three times, run two replicas in one
consumer group with PSP timeouts injected, and assert invoices == sessions and exactly one capture
per invoice; they also cover a tariff outage, declines and compensation, header spoofing and the
append-only/partition rules.
