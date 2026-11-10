import { createHash, randomUUID } from 'node:crypto';
import { type Pool, type PoolClient, sleep, withTx } from '@evcharge/platform';
import {
  type AuthorizeRequest,
  type CaptureRequest,
  type PaymentProvider,
  PspDeclinedError,
  PspIdempotencyMismatchError,
  PspTimeoutError,
  type RefundRequest,
  type ReleaseRequest,
} from './payment-provider';

export interface MockPspOptions {
  /** Probability that a call ends in a timeout. Half of those happen after the effect was applied. */
  timeoutRate?: number;
  /** Capture is declined for these payment tokens. */
  declineTokens?: ReadonlySet<string>;
  /** Authorization is declined for these payment tokens. */
  authDeclineTokens?: ReadonlySet<string>;
  latencyMs?: number;
  random?: () => number;
}

type Outcome = { outcome: 'ok'; response: Record<string, unknown> } | { outcome: 'declined'; response: { reason: string } };

/**
 * In-process PSP with its own idempotency table (`psp_idempotency`). Behaves like a real one for
 * the properties billing relies on: a repeated key replays the stored result (also a decline),
 * and failure injection includes the nasty case, a timeout AFTER the card was charged.
 */
export class MockPsp implements PaymentProvider {
  private readonly timeoutRate: number;
  private readonly declineTokens: ReadonlySet<string>;
  private readonly authDeclineTokens: ReadonlySet<string>;
  private readonly latencyMs: number;
  private readonly random: () => number;

  constructor(
    private readonly pool: Pool,
    opts: MockPspOptions = {},
  ) {
    this.timeoutRate = opts.timeoutRate ?? 0;
    this.declineTokens = opts.declineTokens ?? new Set();
    this.authDeclineTokens = opts.authDeclineTokens ?? new Set();
    this.latencyMs = opts.latencyMs ?? 0;
    this.random = opts.random ?? Math.random;
  }

  async authorize(req: AuthorizeRequest): Promise<{ authorizationId: string }> {
    const res = await this.call(req.idempotencyKey, 'authorize', req, async (c) => {
      if (this.authDeclineTokens.has(req.token)) return { outcome: 'declined', response: { reason: 'card_declined' } };
      const authorizationId = `auth_${randomUUID()}`;
      await c.query('INSERT INTO psp_authorizations (authorization_id, token, amount_minor, currency) VALUES ($1, $2, $3, $4)', [authorizationId, req.token, req.amountMinor, req.currency]);
      return { outcome: 'ok', response: { authorizationId } };
    });
    return { authorizationId: String(res['authorizationId']) };
  }

  async capture(req: CaptureRequest): Promise<{ pspReference: string; capturedMinor: number }> {
    const res = await this.call(req.idempotencyKey, 'capture', req, async (c) => {
      if (this.declineTokens.has(req.token)) return { outcome: 'declined', response: { reason: 'insufficient_funds' } };
      if (req.authorizationId) {
        const { rows } = await c.query<{ amount_minor: number; captured_minor: number; released: boolean }>(
          'SELECT amount_minor, captured_minor, released FROM psp_authorizations WHERE authorization_id = $1 FOR UPDATE',
          [req.authorizationId],
        );
        const auth = rows[0];
        if (!auth) return { outcome: 'declined', response: { reason: 'unknown_authorization' } };
        if (auth.released) return { outcome: 'declined', response: { reason: 'authorization_released' } };
        if (req.amountMinor > auth.amount_minor - auth.captured_minor) return { outcome: 'declined', response: { reason: 'amount_exceeds_authorization' } };
        await c.query('UPDATE psp_authorizations SET captured_minor = captured_minor + $2 WHERE authorization_id = $1', [req.authorizationId, req.amountMinor]);
      }
      const pspReference = `cap_${randomUUID()}`;
      await c.query('INSERT INTO psp_captures (psp_reference, authorization_id, token, amount_minor, currency) VALUES ($1, $2, $3, $4, $5)', [
        pspReference,
        req.authorizationId,
        req.token,
        req.amountMinor,
        req.currency,
      ]);
      return { outcome: 'ok', response: { pspReference, capturedMinor: req.amountMinor } };
    });
    return { pspReference: String(res['pspReference']), capturedMinor: Number(res['capturedMinor']) };
  }

  async release(req: ReleaseRequest): Promise<void> {
    await this.call(req.idempotencyKey, 'release', req, async (c) => {
      let authorizationId = req.authorizationId;
      if (!authorizationId && req.authorizationKey) {
        await c.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [req.authorizationKey]);
        const orig = await c.query<{ outcome: string; response: { authorizationId?: string } }>('SELECT outcome, response FROM psp_idempotency WHERE idempotency_key = $1', [req.authorizationKey]);
        const found = orig.rows[0];
        if (found) authorizationId = found.response.authorizationId;
        else
          // Never seen: leave a tombstone so a late authorize with this key is refused.
          await c.query("INSERT INTO psp_idempotency (idempotency_key, operation, request_hash, outcome, response) VALUES ($1, 'authorize', 'voided', 'declined', '{\"reason\":\"voided\"}'::jsonb)", [req.authorizationKey]);
      }
      if (authorizationId) await c.query('UPDATE psp_authorizations SET released = true WHERE authorization_id = $1', [authorizationId]);
      return { outcome: 'ok', response: {} };
    });
  }

  async refund(req: RefundRequest): Promise<{ refundReference: string }> {
    const res = await this.call(req.idempotencyKey, 'refund', req, async (c) => {
      const { rows } = await c.query<{ amount_minor: number; currency: string }>('SELECT amount_minor, currency FROM psp_captures WHERE psp_reference = $1 FOR UPDATE', [req.pspReference]);
      const cap = rows[0];
      if (!cap) return { outcome: 'declined', response: { reason: 'unknown_capture' } };
      if (cap.currency !== req.currency) return { outcome: 'declined', response: { reason: 'currency_mismatch' } };
      const done = await c.query<{ n: string }>('SELECT coalesce(sum(amount_minor), 0) AS n FROM psp_refunds WHERE psp_reference = $1', [req.pspReference]);
      if (req.amountMinor > cap.amount_minor - Number(done.rows[0]?.n)) return { outcome: 'declined', response: { reason: 'amount_exceeds_capture' } };
      const refundReference = `ref_${randomUUID()}`;
      await c.query('INSERT INTO psp_refunds (refund_reference, psp_reference, amount_minor, currency) VALUES ($1, $2, $3, $4)', [refundReference, req.pspReference, req.amountMinor, req.currency]);
      return { outcome: 'ok', response: { refundReference } };
    });
    return { refundReference: String(res['refundReference']) };
  }

  private async call(key: string, operation: string, request: object, effect: (c: PoolClient) => Promise<Outcome>): Promise<Record<string, unknown>> {
    if (this.latencyMs > 0) await sleep(this.latencyMs);
    const roll = this.random();
    // [0, rate/2): the request is lost before the PSP did anything. [rate/2, rate): it was processed, the answer is lost.
    if (roll < this.timeoutRate / 2) throw new PspTimeoutError(`${operation} timed out before reaching the PSP (injected)`);

    const requestHash = createHash('sha256').update(JSON.stringify({ operation, request })).digest('hex');
    const stored = await withTx(this.pool, async (c) => {
      await c.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [key]);
      const prev = await c.query<{ operation: string; request_hash: string; outcome: 'ok' | 'declined'; response: Record<string, unknown> }>(
        'SELECT operation, request_hash, outcome, response FROM psp_idempotency WHERE idempotency_key = $1',
        [key],
      );
      const row = prev.rows[0];
      if (row) {
        if (row.operation !== operation || row.request_hash !== requestHash) throw new PspIdempotencyMismatchError(`idempotency key ${key} reused with a different request`);
        return { outcome: row.outcome, response: row.response } as Outcome;
      }
      const result = await effect(c);
      await c.query('INSERT INTO psp_idempotency (idempotency_key, operation, request_hash, outcome, response) VALUES ($1, $2, $3, $4, $5::jsonb)', [
        key,
        operation,
        requestHash,
        result.outcome,
        JSON.stringify(result.response),
      ]);
      return result;
    });
    if (roll < this.timeoutRate) throw new PspTimeoutError(`${operation} timed out after the PSP processed it (injected)`);
    if (stored.outcome === 'declined') throw new PspDeclinedError(stored.response.reason);
    return stored.response;
  }
}
