/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (worker loops, ordering)
import { randomUUID } from 'node:crypto';
import { api, events, Topics } from '@evcharge/contracts';
import { errorMessage } from '../util';
import { enqueueOutbox, HttpError, makeEnvelope, type Logger, type Pool, type PoolClient, type Queryable, withTx } from '@evcharge/platform';
import type { BillingConfig } from '../config';
import { AUTH_MAX_ATTEMPTS, captureAmount, isPermanentHttpStatus, PENDING_STATES, retryDelayMs, type SagaState } from '../domain';
import { metrics } from '../metrics';
import { type PaymentProvider, PspDeclinedError, PspIdempotencyMismatchError, PspTimeoutError } from '../psp/payment-provider';
import type { TariffQuoter } from '../tariff/tariff-client';

export interface SagaRow {
  session_id: string;
  state: SagaState;
  driver_id: string | null;
  tariff_id: string;
  site_id: string;
  station_id: string;
  connector_id: number;
  transaction_id: string;
  started_at: Date;
  completed_at: Date | null;
  stopped_at: Date | null;
  energy_wh: string | null;
  hold_minor: number;
  currency: string;
  authorization_id: string | null;
  hold_released: boolean;
  invoice_id: string | null;
  total_minor: number | null;
  attempts: number;
  next_attempt_at: Date | null;
  deadline_at: Date | null;
  last_error: string | null;
  auth_void_pending: boolean;
  auth_token: string | null;
  capture_token: string | null;
  capture_hold_minor: number | null;
  capture_rest_minor: number | null;
  hold_captured: boolean;
  rest_captured: boolean;
  capture_reference: string | null;
}

type EngineConfig = Pick<
  BillingConfig,
  'HOLD_AMOUNT' | 'HOLD_CURRENCY' | 'BILLING_EVENTS_TOPIC' | 'SAGA_BACKOFF_BASE_MS' | 'SAGA_BACKOFF_MAX_MS' | 'SAGA_PRICING_DEADLINE_MS' | 'SAGA_LEASE_MS' | 'PSP_CALL_TIMEOUT_MS'
>;

const PRODUCER = 'billing';

type CaptureOutcome = { kind: 'ok'; pspReference: string } | { kind: 'declined'; reason: string } | { kind: 'unknown'; err: unknown } | { kind: 'mismatch'; err: unknown };

/** Thrown inside a transition transaction when the saga no longer is in the state the step started from. */
class StateChanged extends Error {}

export class SagaEngine {
  constructor(
    private readonly pool: Pool,
    private readonly psp: PaymentProvider,
    private readonly tariff: TariffQuoter,
    private readonly cfg: EngineConfig,
    private readonly log: Logger,
  ) {}

  // ------------------------------------------------------------------ event intake (inside the inbox transaction)

  /** session.started: create the saga. A session without a driver skips the authorization. */
  async onSessionStarted(c: Queryable, d: events.SessionStarted['data']): Promise<void> {
    const hasDriver = d.driverId !== null;
    await c.query(
      `INSERT INTO billing_sagas (session_id, state, driver_id, tariff_id, site_id, station_id, connector_id, transaction_id, started_at, hold_minor, currency, next_attempt_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CASE WHEN $2 = 'AUTHORIZING' THEN now() END)
       ON CONFLICT (session_id) DO NOTHING`,
      [d.sessionId, hasDriver ? 'AUTHORIZING' : 'AUTHORIZED', d.driverId, d.tariffId, d.siteId, d.stationId, d.connectorId, d.transactionId, d.startedAt, hasDriver ? this.cfg.HOLD_AMOUNT : 0, this.cfg.HOLD_CURRENCY],
    );
  }

  /**
   * session.completed: record the stop data and move to PRICING (immediately, or after the
   * in-flight authorization finished). Also creates the saga when session.started was never seen.
   * A repeated completion is a no-op (`completed_at IS NULL` guard).
   */
  async onSessionCompleted(c: Queryable, d: events.SessionCompleted['data']): Promise<void> {
    await c.query(
      `INSERT INTO billing_sagas (session_id, state, driver_id, tariff_id, site_id, station_id, connector_id, transaction_id, started_at,
                                  completed_at, stopped_at, energy_wh, hold_minor, currency, next_attempt_at, deadline_at)
       VALUES ($1, 'PRICING', $2, $3, $4, $5, $6, $7, $8, now(), $9, $10, 0, $11, now(), now() + $12 * interval '1 millisecond')
       ON CONFLICT (session_id) DO UPDATE SET
         completed_at = now(),
         stopped_at = EXCLUDED.stopped_at,
         energy_wh = EXCLUDED.energy_wh,
         tariff_id = EXCLUDED.tariff_id,
         driver_id = COALESCE(billing_sagas.driver_id, EXCLUDED.driver_id),
         deadline_at = EXCLUDED.deadline_at,
         state = CASE WHEN billing_sagas.state IN ('AUTHORIZED', 'AUTH_FAILED') THEN 'PRICING' ELSE billing_sagas.state END,
         attempts = CASE WHEN billing_sagas.state IN ('AUTHORIZED', 'AUTH_FAILED') THEN 0 ELSE billing_sagas.attempts END,
         next_attempt_at = CASE WHEN billing_sagas.state IN ('AUTHORIZED', 'AUTH_FAILED') THEN now() ELSE billing_sagas.next_attempt_at END,
         updated_at = now()
       WHERE billing_sagas.completed_at IS NULL`,
      [d.sessionId, d.driverId, d.tariffId, d.siteId, d.stationId, d.connectorId, d.transactionId, d.startedAt, d.stoppedAt, d.energyWh, this.cfg.HOLD_CURRENCY, this.cfg.SAGA_PRICING_DEADLINE_MS],
    );
  }

  // ------------------------------------------------------------------ scheduling

  /**
   * Claims due sagas with FOR UPDATE SKIP LOCKED and leases them, so concurrent workers (and
   * replicas) never process the same saga, and no transaction stays open while a step calls out.
   */
  async claim(limit: number): Promise<SagaRow[]> {
    const { rows } = await this.pool.query<SagaRow>(
      `UPDATE billing_sagas s
          SET lease_until = now() + $2 * interval '1 millisecond', updated_at = now()
         FROM (SELECT session_id FROM billing_sagas
                WHERE next_attempt_at <= now() AND (lease_until IS NULL OR lease_until < now())
                ORDER BY next_attempt_at
                LIMIT $1
                FOR UPDATE SKIP LOCKED) due
        WHERE s.session_id = due.session_id
        RETURNING s.*`,
      [limit, this.cfg.SAGA_LEASE_MS],
    );
    return rows;
  }

  /** Refreshes billing_sagas_pending{state}. */
  async refreshPendingGauge(): Promise<void> {
    const { rows } = await this.pool.query<{ state: string; n: string }>('SELECT state, count(*) AS n FROM billing_sagas WHERE state = ANY($1) GROUP BY state', [PENDING_STATES]);
    const byState = new Map(rows.map((r) => [r.state, Number(r.n)]));
    for (const s of PENDING_STATES) metrics.sagasPending.set({ state: s }, byState.get(s) ?? 0);
  }

  /** Executes the step for the saga's state. Never throws: failures reschedule the saga. */
  async process(row: SagaRow): Promise<void> {
    const t0 = performance.now();
    try {
      switch (row.state) {
        case 'AUTHORIZING':
          return await this.stepAuthorize(row);
        case 'PRICING':
          return await this.stepPrice(row);
        case 'INVOICED':
        case 'CAPTURING':
          return await this.stepCapture(row);
        case 'CAPTURE_FAILED':
          return await this.stepRelease(row);
        default:
          // Not actionable (a stale schedule): clear it.
          await this.pool.query('UPDATE billing_sagas SET next_attempt_at = NULL, lease_until = NULL WHERE session_id = $1 AND state = $2', [row.session_id, row.state]);
      }
    } catch (err) {
      if (err instanceof StateChanged) {
        metrics.sagaSteps.inc({ step: row.state, outcome: 'state_changed' });
        return;
      }
      this.log.error({ err: errorMessage(err), sessionId: row.session_id, state: row.state }, 'saga step crashed; will retry');
      metrics.sagaSteps.inc({ step: row.state, outcome: 'error' });
      await this.retryLater(row, row.state.toLowerCase(), err).catch((e) => this.log.error({ err: errorMessage(e), sessionId: row.session_id }, 'could not reschedule saga'));
    } finally {
      this.log.debug({ sessionId: row.session_id, state: row.state, ms: Math.round(performance.now() - t0) }, 'saga step done');
    }
  }

  // ------------------------------------------------------------------ steps

  private async stepAuthorize(row: SagaRow): Promise<void> {
    if (row.auth_void_pending) return this.stepVoidAuthorization(row);
    const driverId = row.driver_id;
    // The token of the first attempt is replayed: the same idempotency key must carry the same request.
    const token = row.auth_token ?? (driverId ? await this.paymentToken(driverId) : undefined);
    if (!driverId || !token) return this.authFailed(row, 'no_payment_method');
    if (row.auth_token === null) await this.pool.query("UPDATE billing_sagas SET auth_token = $2 WHERE session_id = $1 AND state = 'AUTHORIZING'", [row.session_id, token]);
    try {
      const { authorizationId } = await this.callPsp('authorize', () =>
        this.psp.authorize({ idempotencyKey: `auth:${row.session_id}`, token, amountMinor: row.hold_minor, currency: row.currency }),
      );
      await withTx(this.pool, async (c) => {
        const res = await c.query(
          `UPDATE billing_sagas
              SET authorization_id = $2,
                  state = CASE WHEN completed_at IS NOT NULL THEN 'PRICING' ELSE 'AUTHORIZED' END,
                  next_attempt_at = CASE WHEN completed_at IS NOT NULL THEN now() END,
                  attempts = 0, lease_until = NULL, last_error = NULL, updated_at = now()
            WHERE session_id = $1 AND state = 'AUTHORIZING'`,
          [row.session_id, authorizationId],
        );
        if (res.rowCount === 0) throw new StateChanged();
        await this.emit(c, row.session_id, 'billing.payment-authorized', {
          sessionId: row.session_id,
          driverId,
          authorizationId,
          hold: { amountMinor: row.hold_minor, currency: row.currency },
        });
      });
      metrics.sagaSteps.inc({ step: 'AUTHORIZING', outcome: 'authorized' });
    } catch (err) {
      if (err instanceof PspDeclinedError) return this.authFailed(row, err.reason);
      if (err instanceof PspIdempotencyMismatchError) return this.parkIdempotencyMismatch(row, 'authorize', err);
      if (err instanceof PspTimeoutError && row.attempts + 1 >= AUTH_MAX_ATTEMPTS) {
        // The hold may exist at the PSP (processed, answer lost): void it before giving up on it.
        await this.pool.query(
          "UPDATE billing_sagas SET auth_void_pending = true, attempts = 0, next_attempt_at = now(), lease_until = NULL, updated_at = now() WHERE session_id = $1 AND state = 'AUTHORIZING'",
          [row.session_id],
        );
        metrics.sagaSteps.inc({ step: 'AUTHORIZING', outcome: 'void_scheduled' });
        return;
      }
      if (err instanceof PspTimeoutError) {
        // Unknown outcome: retry with the same idempotency key.
        metrics.sagaSteps.inc({ step: 'AUTHORIZING', outcome: 'unknown' });
        return this.retryLater(row, 'authorize', err);
      }
      throw err;
    }
  }

  /** Voids a possibly existing hold through the authorize idempotency key, retrying until the PSP answers definitively. */
  private async stepVoidAuthorization(row: SagaRow): Promise<void> {
    try {
      await this.callPsp('release', () => this.psp.release({ idempotencyKey: `void:${row.session_id}`, authorizationKey: `auth:${row.session_id}` }));
    } catch (err) {
      if (err instanceof PspTimeoutError) return this.retryLater(row, 'void_authorization', err);
      if (err instanceof PspIdempotencyMismatchError) return this.parkIdempotencyMismatch(row, 'void_authorization', err);
      if (!(err instanceof PspDeclinedError)) throw err;
    }
    metrics.sagaSteps.inc({ step: 'AUTHORIZING', outcome: 'voided' });
    await this.authFailed(row, 'psp_unavailable');
  }

  private async authFailed(row: SagaRow, reason: string): Promise<void> {
    await withTx(this.pool, async (c) => {
      const res = await c.query(
        `UPDATE billing_sagas
            SET state = CASE WHEN completed_at IS NOT NULL THEN 'PRICING' ELSE 'AUTH_FAILED' END,
                next_attempt_at = CASE WHEN completed_at IS NOT NULL THEN now() END,
                hold_minor = 0, attempts = 0, lease_until = NULL, failure_reason = $2, updated_at = now()
          WHERE session_id = $1 AND state = 'AUTHORIZING'`,
        [row.session_id, reason],
      );
      if (res.rowCount === 0) throw new StateChanged();
      await this.emit(c, row.session_id, 'billing.payment-failed', { invoiceId: null, sessionId: row.session_id, step: 'authorize', reason });
    });
    metrics.sagaSteps.inc({ step: 'AUTHORIZING', outcome: 'auth_failed' });
  }

  private async stepPrice(row: SagaRow): Promise<void> {
    if (!row.stopped_at || row.energy_wh === null) throw new Error('PRICING saga without completion data');
    let quote: api.QuoteRes;
    try {
      quote = await this.tariff.quote(row.session_id, { tariffId: row.tariff_id, startedAt: row.started_at.toISOString(), stoppedAt: row.stopped_at.toISOString(), energyWh: Number(row.energy_wh) });
    } catch (err) {
      metrics.sagaSteps.inc({ step: 'PRICING', outcome: 'tariff_unavailable' });
      if (err instanceof HttpError && isPermanentHttpStatus(err.status)) return this.park(row, 'pricing', `tariff rejected the quote: ${errorMessage(err)}`, true);
      return this.retryLater(row, 'pricing', err);
    }

    const issuedAt = new Date();
    const invoiceId = randomUUID();
    const hasDriver = row.driver_id !== null;
    await withTx(this.pool, async (c) => {
      const reg = await c.query<{ invoice_id: string }>(
        'INSERT INTO invoice_registry (session_id, invoice_id, driver_id, issued_at) VALUES ($1, $2, $3, $4) ON CONFLICT (session_id) DO NOTHING RETURNING invoice_id',
        [row.session_id, invoiceId, row.driver_id, issuedAt],
      );
      if (reg.rowCount === 0) {
        // The invoice already exists (UNIQUE session_id): adopt it, never write a second one.
        const existing = await c.query<{ invoice_id: string; total_minor: number }>(
          'SELECT r.invoice_id, i.total_minor FROM invoice_registry r JOIN invoices i ON i.invoice_id = r.invoice_id AND i.issued_at = r.issued_at WHERE r.session_id = $1',
          [row.session_id],
        );
        const inv = existing.rows[0];
        if (!inv) throw new Error('invoice registry entry without invoice');
        await this.markInvoiced(c, row, inv.invoice_id, inv.total_minor, hasDriver);
        return;
      }
      await c.query(
        `INSERT INTO invoices (invoice_id, session_id, driver_id, tariff_id, currency, total_minor, energy_wh, started_at, stopped_at, station_id, site_id, issued_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
        [invoiceId, row.session_id, row.driver_id, quote.tariffId, quote.currency, quote.totalMinor, row.energy_wh, row.started_at, row.stopped_at, row.station_id, row.site_id, issuedAt],
      );
      for (const [i, l] of quote.lines.entries()) {
        await c.query('INSERT INTO invoice_lines (invoice_id, issued_at, line_no, kind, description, quantity, unit, amount_minor) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', [
          invoiceId,
          issuedAt,
          i + 1,
          l.kind,
          l.description,
          l.quantity,
          l.unit,
          l.amountMinor,
        ]);
      }
      await this.markInvoiced(c, row, invoiceId, quote.totalMinor, hasDriver, quote.currency);
      await this.emit(c, row.session_id, 'billing.invoice-issued', {
        invoiceId,
        sessionId: row.session_id,
        driverId: row.driver_id,
        total: { amountMinor: quote.totalMinor, currency: quote.currency },
        lines: quote.lines.map((l) => ({ kind: l.kind, description: l.description, quantity: l.quantity, unit: l.unit, amountMinor: l.amountMinor })),
      });
    });
    metrics.invoices.inc();
    metrics.invoiceLatency.observe(Math.max(0, (issuedAt.getTime() - row.stopped_at.getTime()) / 1000));
    metrics.sagaSteps.inc({ step: 'PRICING', outcome: 'invoiced' });
  }

  private async markInvoiced(c: Queryable, row: SagaRow, invoiceId: string, totalMinor: number, hasDriver: boolean, currency?: string): Promise<void> {
    const res = await c.query(
      // A session without a driver is invoiced as unpaid: INVOICED is final for it.
      `UPDATE billing_sagas
          SET state = 'INVOICED', invoice_id = $2, total_minor = $3, currency = COALESCE($5, currency), attempts = 0, last_error = NULL,
              next_attempt_at = CASE WHEN $4 THEN now() END, lease_until = NULL,
              deadline_at = now() + $6 * interval '1 millisecond', updated_at = now()
        WHERE session_id = $1 AND state = 'PRICING'`,
      [row.session_id, invoiceId, totalMinor, hasDriver, currency ?? null, this.cfg.SAGA_PRICING_DEADLINE_MS],
    );
    if (res.rowCount === 0) throw new StateChanged();
  }

  /**
   * Captures the hold (min(total, hold)) and then, when the total exceeded the hold, the remainder by
   * token. The request (token and amounts) is fixed when the saga enters CAPTURING and replayed on
   * every retry. Each sub-capture is marked done in the database, so a retry resumes after it.
   */
  private async stepCapture(row: SagaRow): Promise<void> {
    const invoiceId = row.invoice_id;
    if (!invoiceId || row.total_minor === null) throw new Error('capture without invoice');
    if (row.state === 'INVOICED') await this.beginCapture(row, row.total_minor);
    const holdPart = row.capture_hold_minor ?? 0;
    const rest = row.capture_rest_minor ?? 0;
    const token = row.capture_token ?? '';
    const hasAuth = row.authorization_id !== null;

    if (holdPart + rest === 0) {
      // Nothing to charge: hand the hold back and close the saga.
      if (row.authorization_id) {
        const authorizationId = row.authorization_id;
        try {
          await this.callPsp('release', () => this.psp.release({ idempotencyKey: `release:${row.session_id}`, authorizationId }));
        } catch (err) {
          if (err instanceof PspTimeoutError) return this.retryLater(row, 'capture', err);
          if (err instanceof PspIdempotencyMismatchError) return this.parkIdempotencyMismatch(row, 'capture', err);
          if (!(err instanceof PspDeclinedError)) throw err;
        }
      }
      metrics.captures.inc({ outcome: 'zero' });
      return this.paid(row, invoiceId, 0, 'none');
    }

    if (!row.hold_captured) {
      if (!hasAuth && !token) return this.captureFailed(row, invoiceId, 'no_payment_method', 'capture');
      const outcome = await this.captureStep('capture', { idempotencyKey: `capture:${invoiceId}`, authorizationId: row.authorization_id, token, amountMinor: holdPart, currency: row.currency });
      if (outcome.kind !== 'ok') return this.afterFailedCapture(row, invoiceId, outcome, 'capture');
      await this.recordCapture(row, invoiceId, 'hold', holdPart, outcome.pspReference);
      row.hold_captured = true;
      row.capture_reference = outcome.pspReference;
    }

    if (rest > 0 && !row.rest_captured) {
      if (!token) return this.captureFailed(row, invoiceId, 'no_payment_method', 'capture_remainder');
      const outcome = await this.captureStep('capture_remainder', { idempotencyKey: `capture:${invoiceId}:rest`, authorizationId: null, token, amountMinor: rest, currency: row.currency });
      if (outcome.kind !== 'ok') return this.afterFailedCapture(row, invoiceId, outcome, 'capture_remainder');
      await this.recordCapture(row, invoiceId, 'remainder', rest, outcome.pspReference);
    }
    await this.paid(row, invoiceId, holdPart + rest, row.capture_reference ?? 'none');
  }

  /** INVOICED -> CAPTURING, fixing the capture request (token and amounts) for all later retries. */
  private async beginCapture(row: SagaRow, total: number): Promise<void> {
    const hasAuth = row.authorization_id !== null;
    const holdPart = captureAmount(total, row.hold_minor, hasAuth);
    const rest = hasAuth ? total - holdPart : 0;
    const token = row.driver_id ? ((await this.paymentToken(row.driver_id)) ?? '') : '';
    const res = await this.pool.query(
      `UPDATE billing_sagas SET state = 'CAPTURING', capture_token = $2, capture_hold_minor = $3, capture_rest_minor = $4, updated_at = now()
        WHERE session_id = $1 AND state = 'INVOICED'`,
      [row.session_id, token, holdPart, rest],
    );
    if (res.rowCount === 0) throw new StateChanged();
    Object.assign(row, { state: 'CAPTURING', capture_token: token, capture_hold_minor: holdPart, capture_rest_minor: rest });
  }

  private async captureStep(step: 'capture' | 'capture_remainder', req: Parameters<PaymentProvider['capture']>[0]): Promise<CaptureOutcome> {
    try {
      const res = await this.callPsp('capture', () => this.psp.capture(req));
      metrics.captures.inc({ outcome: step === 'capture' ? 'captured' : 'remainder_captured' });
      return { kind: 'ok', pspReference: res.pspReference };
    } catch (err) {
      if (err instanceof PspDeclinedError) {
        metrics.captures.inc({ outcome: step === 'capture' ? 'declined' : 'remainder_declined' });
        return { kind: 'declined', reason: err.reason };
      }
      if (err instanceof PspTimeoutError) {
        // Unknown, not failed: stay CAPTURING and repeat the SAME request with the SAME idempotency key.
        metrics.captures.inc({ outcome: 'unknown_timeout' });
        return { kind: 'unknown', err };
      }
      if (err instanceof PspIdempotencyMismatchError) return { kind: 'mismatch', err };
      throw err;
    }
  }

  private async afterFailedCapture(row: SagaRow, invoiceId: string, outcome: Exclude<CaptureOutcome, { kind: 'ok' }>, step: 'capture' | 'capture_remainder'): Promise<void> {
    if (outcome.kind === 'declined') return this.captureFailed(row, invoiceId, outcome.reason, step);
    if (outcome.kind === 'unknown') return this.retryLater(row, step, outcome.err);
    return this.parkIdempotencyMismatch(row, step, outcome.err);
  }

  /** Marks one sub-capture done and records its payment row, in one transaction. */
  private async recordCapture(row: SagaRow, invoiceId: string, kind: 'hold' | 'remainder', amountMinor: number, pspReference: string): Promise<void> {
    await withTx(this.pool, async (c) => {
      const res =
        kind === 'hold'
          ? await c.query("UPDATE billing_sagas SET hold_captured = true, capture_reference = $2, attempts = 0, last_error = NULL, updated_at = now() WHERE session_id = $1 AND state = 'CAPTURING'", [row.session_id, pspReference])
          : await c.query("UPDATE billing_sagas SET rest_captured = true, attempts = 0, last_error = NULL, updated_at = now() WHERE session_id = $1 AND state = 'CAPTURING'", [row.session_id]);
      if (res.rowCount === 0) throw new StateChanged();
      await c.query('INSERT INTO payments (invoice_id, kind, session_id, amount_minor, currency, psp_reference) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (invoice_id, kind) DO NOTHING', [
        invoiceId,
        kind,
        row.session_id,
        amountMinor,
        row.currency,
        pspReference,
      ]);
    });
    row.attempts = 0;
  }

  private async paid(row: SagaRow, invoiceId: string, capturedMinor: number, pspReference: string): Promise<void> {
    await withTx(this.pool, async (c) => {
      const res = await c.query(
        `UPDATE billing_sagas SET state = 'PAID', hold_released = true, next_attempt_at = NULL, lease_until = NULL, attempts = 0, last_error = NULL, updated_at = now()
          WHERE session_id = $1 AND state = 'CAPTURING'`,
        [row.session_id],
      );
      if (res.rowCount === 0) throw new StateChanged();
      await this.emit(c, row.session_id, 'billing.payment-captured', { invoiceId, sessionId: row.session_id, captured: { amountMinor: capturedMinor, currency: row.currency }, pspReference });
    });
    metrics.sagaSteps.inc({ step: 'CAPTURING', outcome: 'paid' });
  }

  /**
   * Definite decline: CAPTURE_FAILED and PaymentFailed emitted. A hold that was not captured is
   * released as compensation (by stepRelease). A declined remainder leaves the captured hold part in place.
   */
  private async captureFailed(row: SagaRow, invoiceId: string, reason: string, step: 'capture' | 'capture_remainder'): Promise<void> {
    await withTx(this.pool, async (c) => {
      const res = await c.query(
        `UPDATE billing_sagas
            SET state = 'CAPTURE_FAILED', failure_reason = $2, attempts = 0, lease_until = NULL, last_error = NULL,
                hold_released = (authorization_id IS NULL OR hold_captured),
                next_attempt_at = CASE WHEN authorization_id IS NOT NULL AND NOT hold_captured THEN now() END,
                updated_at = now()
          WHERE session_id = $1 AND state = 'CAPTURING'`,
        [row.session_id, reason],
      );
      if (res.rowCount === 0) throw new StateChanged();
      await this.emit(c, row.session_id, 'billing.payment-failed', { invoiceId, sessionId: row.session_id, step, reason });
    });
    metrics.sagaSteps.inc({ step: 'CAPTURING', outcome: step === 'capture' ? 'capture_failed' : 'remainder_failed' });
  }

  private async stepRelease(row: SagaRow): Promise<void> {
    if (row.hold_released || !row.authorization_id) {
      await this.pool.query('UPDATE billing_sagas SET next_attempt_at = NULL, lease_until = NULL, hold_released = true WHERE session_id = $1 AND state = $2', [row.session_id, row.state]);
      return;
    }
    const authorizationId = row.authorization_id;
    try {
      await this.callPsp('release', () => this.psp.release({ idempotencyKey: `release:${row.session_id}`, authorizationId }));
    } catch (err) {
      if (err instanceof PspTimeoutError) return this.retryLater(row, 'release', err);
      if (!(err instanceof PspDeclinedError)) throw err;
      this.log.warn({ sessionId: row.session_id, reason: err.reason }, 'PSP refused to release the hold');
    }
    await this.pool.query("UPDATE billing_sagas SET hold_released = true, next_attempt_at = NULL, lease_until = NULL, updated_at = now() WHERE session_id = $1 AND state = 'CAPTURE_FAILED'", [row.session_id]);
    metrics.sagaSteps.inc({ step: 'CAPTURE_FAILED', outcome: 'hold_released' });
  }

  // ------------------------------------------------------------------ helpers

  private async paymentToken(driverId: string): Promise<string | undefined> {
    const { rows } = await this.pool.query<{ token: string }>('SELECT token FROM payment_methods WHERE driver_id = $1', [driverId]);
    return rows[0]?.token;
  }

  /** One PSP call with a client-side timeout; a timeout is an unknown outcome (PspTimeoutError). */
  private async callPsp<T>(operation: string, fn: () => Promise<T>): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new PspTimeoutError(`${operation} did not answer within ${this.cfg.PSP_CALL_TIMEOUT_MS} ms`)), this.cfg.PSP_CALL_TIMEOUT_MS);
    });
    try {
      const out = await Promise.race([fn(), timeout]);
      metrics.pspCalls.inc({ operation, outcome: 'ok' });
      return out;
    } catch (err) {
      metrics.pspCalls.inc({ operation, outcome: err instanceof PspDeclinedError ? 'declined' : err instanceof PspTimeoutError ? 'timeout' : 'error' });
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }

  /** Schedules the next attempt with capped backoff, or parks the saga once its deadline passed. */
  private async retryLater(row: SagaRow, step: string, err: unknown): Promise<void> {
    const attempts = row.attempts + 1;
    const message = errorMessage(err).slice(0, 500);
    if (row.deadline_at && row.deadline_at.getTime() < Date.now()) return this.park(row, step, `deadline exceeded: ${message}`, row.state === 'PRICING');
    const delay = retryDelayMs(attempts, this.cfg.SAGA_BACKOFF_BASE_MS, this.cfg.SAGA_BACKOFF_MAX_MS);
    await this.pool.query(
      "UPDATE billing_sagas SET attempts = $3, last_error = $4, next_attempt_at = now() + $5 * interval '1 millisecond', lease_until = NULL, updated_at = now() WHERE session_id = $1 AND state = $2",
      [row.session_id, row.state, attempts, message, delay],
    );
    this.log.warn({ sessionId: row.session_id, step, attempts, delayMs: delay, err: message }, 'saga step will be retried');
  }

  /**
   * The PSP says the idempotency key was used for a different request. Retrying cannot fix that and
   * the money state is uncertain, so the saga is parked for an operator, loudly.
   */
  private async parkIdempotencyMismatch(row: SagaRow, step: string, err: unknown): Promise<void> {
    metrics.pspIdempotencyMismatch.inc({ step });
    this.log.error({ sessionId: row.session_id, step, err: errorMessage(err) }, 'PSP idempotency key mismatch; saga parked for manual review');
    await this.park(row, 'psp_idempotency_mismatch', `PSP idempotency mismatch in ${step}: ${errorMessage(err)}`, false);
  }

  /** Stops automatic retries (visible as billing_sagas_parked_total and last_error). */
  private async park(row: SagaRow, step: string, reason: string, emitFailure: boolean): Promise<void> {
    this.log.error({ sessionId: row.session_id, step, reason }, 'saga parked');
    metrics.sagasParked.inc({ step });
    // A saga stopped in PRICING still holds the authorization. Hand it to the compensation path (CAPTURE_FAILED, stepRelease)
    // so the hold is released instead of staying on the card until the PSP expires it.
    const holdToRelease = row.state === 'PRICING' && row.authorization_id !== null && !row.hold_released;
    await withTx(this.pool, async (c) => {
      const res = await c.query(
        holdToRelease
          ? "UPDATE billing_sagas SET state = 'CAPTURE_FAILED', attempts = 0, next_attempt_at = now(), lease_until = NULL, last_error = $3, failure_reason = $3, updated_at = now() WHERE session_id = $1 AND state = $2"
          : 'UPDATE billing_sagas SET next_attempt_at = NULL, lease_until = NULL, last_error = $3, failure_reason = $3, updated_at = now() WHERE session_id = $1 AND state = $2',
        [row.session_id, row.state, reason.slice(0, 500)],
      );
      if (res.rowCount === 0) throw new StateChanged();
      if (emitFailure) await this.emit(c, row.session_id, 'billing.payment-failed', { invoiceId: null, sessionId: row.session_id, step, reason: reason.slice(0, 500) });
    });
  }

  private async emit<T extends events.BillingEvent['type']>(c: PoolClient | Queryable, sessionId: string, type: T, data: Extract<events.BillingEvent, { type: T }>['data']): Promise<void> {
    await enqueueOutbox(c, { topic: this.cfg.BILLING_EVENTS_TOPIC, event: makeEnvelope(type, sessionId, data, PRODUCER) });
  }
}

export { Topics };
