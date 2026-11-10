/* oxlint-disable no-await-in-loop */ // refunds against one invoice's captures run one after another
import { randomUUID } from 'node:crypto';
import { enqueueOutbox, type Logger, makeEnvelope, type Pool, withTx } from '@evcharge/platform';
import { splitVat } from '../domain';
import { metrics } from '../metrics';
import { type PaymentProvider, PspDeclinedError, PspIdempotencyMismatchError, PspTimeoutError } from '../psp/payment-provider';
import { nextDocumentNumber } from '../sagas/saga-engine';

export type RefundState = 'none' | 'pending' | 'done' | 'failed';

export interface CreditNoteResult {
  creditNoteId: string;
  creditNumber: string;
  invoiceId: string;
  amountMinor: number;
  netMinor: number;
  vatMinor: number;
  vatRateBp: number;
  currency: string;
  reason: string;
  issuedAt: string;
  /** none: the invoice was not paid, nothing to give back; pending: a PSP call had an unknown outcome, repeat the request. */
  refund: { state: RefundState; amountMinor: number };
}

/** Request errors that map to an HTTP status (400, 404, 409). */
export class CreditNoteError extends Error {
  constructor(
    readonly status: 400 | 404 | 409,
    message: string,
  ) {
    super(message);
  }
}

export interface CreditNoteConfig {
  INVOICE_PREFIX: string;
  VAT_RATE_BP: number;
  BILLING_EVENTS_TOPIC: string;
}

interface CreditNoteRow {
  credit_note_id: string;
  credit_number: string;
  invoice_id: string;
  amount_minor: number;
  net_minor: number;
  vat_minor: number;
  vat_rate_bp: number;
  currency: string;
  reason: string;
  issued_at: Date;
}

/**
 * Credit notes (ADR 0007): the document is written first, in one transaction that locks the invoice, checks that the
 * credits stay within its total, takes a gapless number and plans the refund across the invoice's captures (newest
 * first, never more than is left of a capture). The PSP refunds run afterwards, one per planned row, each with the
 * stable key `refund:<creditNoteId>:<pspReference>`. A timeout leaves the row pending; repeating the request with the
 * same idempotency-key returns the same credit note and finishes the pending refunds, never issuing a second one.
 */
export class CreditNotes {
  constructor(
    private readonly pool: Pool,
    private readonly psp: PaymentProvider,
    private readonly cfg: CreditNoteConfig,
    private readonly log: Logger,
  ) {}

  async issue(invoiceId: string, idempotencyKey: string, amountMinor: number, reason: string): Promise<CreditNoteResult> {
    const note = await withTx(this.pool, async (c) => {
      const inv = await c.query<{ session_id: string; driver_id: string | null; invoice_number: string | null; total_minor: number; currency: string; vat_rate_bp: number | null }>(
        `SELECT r.session_id, r.driver_id, r.invoice_number, i.total_minor, i.currency, i.vat_rate_bp
           FROM invoice_registry r JOIN invoices i ON i.invoice_id = r.invoice_id AND i.issued_at = r.issued_at
          WHERE r.invoice_id = $1
            FOR UPDATE OF r`, // serialises credit notes of one invoice: the sum check below must see every committed one
        [invoiceId],
      );
      const invoice = inv.rows[0];
      if (!invoice) throw new CreditNoteError(404, `invoice ${invoiceId} not found`);

      const prev = await c.query<CreditNoteRow>('SELECT * FROM credit_notes WHERE invoice_id = $1 AND idempotency_key = $2', [invoiceId, idempotencyKey]);
      if (prev.rows[0]) {
        const p = prev.rows[0];
        if (p.amount_minor !== amountMinor || p.reason !== reason) throw new CreditNoteError(409, `idempotency-key ${idempotencyKey} was used for a different credit note`);
        return p;
      }

      const credited = await c.query<{ n: string }>('SELECT coalesce(sum(amount_minor), 0) AS n FROM credit_notes WHERE invoice_id = $1', [invoiceId]);
      const left = invoice.total_minor - Number(credited.rows[0]?.n);
      if (amountMinor > left) throw new CreditNoteError(409, `credit of ${amountMinor} exceeds the ${left} left of invoice ${invoiceId}`);

      const issuedAt = new Date();
      const creditNoteId = randomUUID();
      const creditNumber = await nextDocumentNumber(c, 'credit_note', this.cfg.INVOICE_PREFIX, issuedAt);
      const vatRateBp = invoice.vat_rate_bp ?? this.cfg.VAT_RATE_BP;
      const { netMinor, vatMinor } = splitVat(amountMinor, vatRateBp);
      const ins = await c.query<CreditNoteRow>(
        `INSERT INTO credit_notes (credit_note_id, invoice_id, session_id, credit_number, idempotency_key, amount_minor, net_minor, vat_minor, vat_rate_bp, currency, reason, issued_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING *`,
        [creditNoteId, invoiceId, invoice.session_id, creditNumber, idempotencyKey, amountMinor, netMinor, vatMinor, vatRateBp, invoice.currency, reason, issuedAt],
      );

      // Refund plan: what was actually collected and not yet refunded (failed refunds gave nothing back), newest capture first.
      const captures = await c.query<{ psp_reference: string; refundable: number }>(
        `SELECT p.psp_reference,
                p.amount_minor - coalesce((SELECT sum(r.amount_minor) FROM credit_note_refunds r WHERE r.psp_reference = p.psp_reference AND r.state <> 'failed'), 0) AS refundable
           FROM payments p WHERE p.invoice_id = $1
          ORDER BY p.captured_at DESC, p.kind DESC`,
        [invoiceId],
      );
      let toRefund = amountMinor;
      for (const cap of captures.rows) {
        const part = Math.min(toRefund, Number(cap.refundable));
        if (part <= 0) continue;
        await c.query('INSERT INTO credit_note_refunds (credit_note_id, psp_reference, amount_minor) VALUES ($1, $2, $3)', [creditNoteId, cap.psp_reference, part]);
        toRefund -= part;
        if (toRefund === 0) break;
      }

      await enqueueOutbox(c, {
        topic: this.cfg.BILLING_EVENTS_TOPIC,
        event: makeEnvelope(
          'billing.credit-note-issued',
          invoice.session_id,
          {
            creditNoteId,
            creditNumber,
            invoiceId,
            ...(invoice.invoice_number !== null && { invoiceNumber: invoice.invoice_number }),
            sessionId: invoice.session_id,
            driverId: invoice.driver_id,
            amount: { amountMinor, currency: invoice.currency },
            net: { amountMinor: netMinor, currency: invoice.currency },
            vat: { amountMinor: vatMinor, currency: invoice.currency },
            vatRateBp,
            reason,
          },
          'billing',
        ),
      });
      metrics.creditNotes.inc();
      return ins.rows[0] as CreditNoteRow;
    });

    const refund = await this.runRefunds(note);
    return {
      creditNoteId: note.credit_note_id,
      creditNumber: note.credit_number,
      invoiceId: note.invoice_id,
      amountMinor: note.amount_minor,
      netMinor: note.net_minor,
      vatMinor: note.vat_minor,
      vatRateBp: note.vat_rate_bp,
      currency: note.currency,
      reason: note.reason,
      issuedAt: note.issued_at.toISOString(),
      refund,
    };
  }

  /** Runs the planned refunds that are still pending; returns the overall state. */
  private async runRefunds(note: CreditNoteRow): Promise<{ state: RefundState; amountMinor: number }> {
    const { rows } = await this.pool.query<{ psp_reference: string; amount_minor: number; state: RefundState }>(
      'SELECT psp_reference, amount_minor, state FROM credit_note_refunds WHERE credit_note_id = $1 ORDER BY psp_reference',
      [note.credit_note_id],
    );
    if (rows.length === 0) return { state: 'none', amountMinor: 0 };
    for (const r of rows) {
      if (r.state !== 'pending') continue;
      try {
        const res = await this.psp.refund({ idempotencyKey: `refund:${note.credit_note_id}:${r.psp_reference}`, pspReference: r.psp_reference, amountMinor: r.amount_minor, currency: note.currency });
        r.state = 'done';
        await this.pool.query("UPDATE credit_note_refunds SET state = 'done', refund_reference = $3, updated_at = now() WHERE credit_note_id = $1 AND psp_reference = $2 AND state = 'pending'", [
          note.credit_note_id,
          r.psp_reference,
          res.refundReference,
        ]);
        metrics.refunds.inc({ outcome: 'done' });
      } catch (err) {
        if (err instanceof PspTimeoutError) {
          metrics.refunds.inc({ outcome: 'unknown_timeout' }); // stays pending: the same request with the same key finishes it
          continue;
        }
        if (!(err instanceof PspDeclinedError) && !(err instanceof PspIdempotencyMismatchError)) throw err;
        r.state = 'failed';
        const why = err instanceof PspDeclinedError ? err.reason : 'idempotency_mismatch';
        await this.pool.query("UPDATE credit_note_refunds SET state = 'failed', failure_reason = $3, updated_at = now() WHERE credit_note_id = $1 AND psp_reference = $2 AND state = 'pending'", [
          note.credit_note_id,
          r.psp_reference,
          why,
        ]);
        metrics.refunds.inc({ outcome: 'failed' });
        this.log.error({ creditNoteId: note.credit_note_id, pspReference: r.psp_reference, reason: why }, 'refund failed; the credit note stands and the money is owed to the driver');
      }
    }
    const amountMinor = rows.filter((r) => r.state === 'done').reduce((s, r) => s + r.amount_minor, 0);
    const state: RefundState = rows.some((r) => r.state === 'pending') ? 'pending' : rows.some((r) => r.state === 'failed') ? 'failed' : 'done';
    return { state, amountMinor };
  }
}
