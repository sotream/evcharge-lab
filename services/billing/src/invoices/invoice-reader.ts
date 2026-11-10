import type { api } from '@evcharge/contracts';
import type { Pool } from '@evcharge/platform';
import type z from 'zod';
import { invoiceStatus } from '../domain';

export type InvoiceView = z.infer<typeof api.BillingPublic.invoice>;
export type InvoiceDetail = InvoiceView & { lines: { kind: string; description: string; quantity: number; unit: string; amountMinor: number }[]; energyWh: number; startedAt: string; stoppedAt: string; tariffId: string };

interface InvoiceRow {
  invoice_id: string;
  session_id: string;
  driver_id: string | null;
  total_minor: number;
  currency: string;
  issued_at: Date;
  state: string;
  invoice_number: string | null;
  net_minor: number | null;
  vat_minor: number | null;
  vat_rate_bp: number | null;
}

const SELECT = `
  SELECT r.invoice_id, r.session_id, r.driver_id, i.total_minor, i.currency, r.issued_at, s.state, i.invoice_number, i.net_minor, i.vat_minor, i.vat_rate_bp
    FROM invoice_registry r
    JOIN invoices i ON i.invoice_id = r.invoice_id AND i.issued_at = r.issued_at
    JOIN billing_sagas s ON s.session_id = r.session_id`;

const view = (r: InvoiceRow): InvoiceView => ({
  invoiceId: r.invoice_id,
  sessionId: r.session_id,
  driverId: r.driver_id,
  status: invoiceStatus(r.state),
  total: { amountMinor: r.total_minor, currency: r.currency },
  issuedAt: r.issued_at.toISOString(),
  ...(r.invoice_number !== null && { invoiceNumber: r.invoice_number }),
  ...(r.net_minor !== null && r.vat_minor !== null && r.vat_rate_bp !== null && {
    net: { amountMinor: r.net_minor, currency: r.currency },
    vat: { amountMinor: r.vat_minor, currency: r.currency },
    vatRateBp: r.vat_rate_bp,
  }),
});

export class InvoiceReader {
  constructor(private readonly pool: Pool) {}

  /** Newest first; `before` is the issuedAt of the last item of the previous page. */
  async listForDriver(driverId: string, limit: number, before?: Date): Promise<{ items: InvoiceView[]; nextBefore: string | null }> {
    const { rows } = await this.pool.query<InvoiceRow>(`${SELECT} WHERE r.driver_id = $1 AND ($2::timestamptz IS NULL OR r.issued_at < $2) ORDER BY r.issued_at DESC, r.invoice_id DESC LIMIT $3`, [
      driverId,
      before ?? null,
      limit,
    ]);
    const items = rows.map(view);
    return { items, nextBefore: rows.length === limit ? (items.at(-1)?.issuedAt ?? null) : null };
  }

  async get(invoiceId: string): Promise<InvoiceDetail | undefined> {
    const { rows } = await this.pool.query<InvoiceRow & { energy_wh: string; started_at: Date; stopped_at: Date; tariff_id: string }>(
      `SELECT r.invoice_id, r.session_id, r.driver_id, i.total_minor, i.currency, r.issued_at, s.state, i.invoice_number, i.net_minor, i.vat_minor, i.vat_rate_bp, i.energy_wh, i.started_at, i.stopped_at, i.tariff_id
         FROM invoice_registry r
         JOIN invoices i ON i.invoice_id = r.invoice_id AND i.issued_at = r.issued_at
         JOIN billing_sagas s ON s.session_id = r.session_id
        WHERE r.invoice_id = $1`,
      [invoiceId],
    );
    const r = rows[0];
    if (!r) return undefined;
    const lines = await this.pool.query<{ kind: string; description: string; quantity: number; unit: string; amount_minor: number }>(
      'SELECT kind, description, quantity, unit, amount_minor FROM invoice_lines WHERE invoice_id = $1 AND issued_at = $2 ORDER BY line_no',
      [invoiceId, r.issued_at],
    );
    return {
      ...view(r),
      energyWh: Number(r.energy_wh),
      startedAt: r.started_at.toISOString(),
      stoppedAt: r.stopped_at.toISOString(),
      tariffId: r.tariff_id,
      lines: lines.rows.map((l) => ({ kind: l.kind, description: l.description, quantity: l.quantity, unit: l.unit, amountMinor: l.amount_minor })),
    };
  }
}
