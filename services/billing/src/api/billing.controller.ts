import { api } from '@evcharge/contracts';
import type { CreditNotes } from '../invoices/credit-notes';
import { InternalTokenGuard, type Pool } from '@evcharge/platform';
import { Body, Controller, ForbiddenException, Get, Headers, HttpCode, HttpException, Inject, NotFoundException, Param, Put, Query, UseGuards } from '@nestjs/common';
import { z } from 'zod';
import type { InvoiceReader } from '../invoices/invoice-reader';
import type { SagaEngine } from '../sagas/saga-engine';
import { isDriverDeleted } from '../gdpr';
import { encryptToken } from '../token-crypto';

export const BILLING_DEPS = Symbol('BILLING_DEPS');
export interface BillingDeps {
  pool: Pool;
  invoices: InvoiceReader;
  engine: SagaEngine;
  /** AES-256 key for payment tokens at rest (TOKEN_ENC_KEY). */
  tokenKey: Buffer;
  creditNotes: CreditNotes;
}

const uuid = z.uuid();

function problem(status: number, title: string, detail: string): HttpException {
  return new HttpException({ type: 'about:blank', title, status, detail }, status);
}

/**
 * Identity rules (ADR 0010): every route needs a valid x-internal-token. `x-driver-id` is only
 * trusted after that, and only driver-api sets it, so a request carrying it may touch just that
 * driver's data. Calls without it are internal (service to service) and may name any driver.
 */
@Controller('v1')
@UseGuards(InternalTokenGuard)
export class BillingController {
  constructor(@Inject(BILLING_DEPS) private readonly deps: BillingDeps) {}

  @Put('drivers/:driverId/payment-method')
  @HttpCode(204)
  async setPaymentMethod(@Param('driverId') driverId: string, @Body() body: unknown, @Headers('x-driver-id') actor?: string): Promise<void> {
    if (!uuid.safeParse(driverId).success) throw problem(400, 'Bad Request', 'driverId must be a uuid');
    this.assertActor(actor, driverId);
    if (await isDriverDeleted(this.deps.pool, driverId)) throw problem(410, 'Gone', 'the account was deleted');
    const parsed = api.BillingPublic.paymentMethodReq.safeParse(body);
    if (!parsed.success) throw problem(400, 'Bad Request', parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; '));
    await this.deps.pool.query(
      `INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2)
       ON CONFLICT (driver_id) DO UPDATE SET token = EXCLUDED.token, updated_at = now()`,
      [driverId, encryptToken(parsed.data.token, this.deps.tokenKey)],
    );
  }

  @Get('invoices')
  async list(@Query() query: Record<string, string | undefined>, @Headers('x-driver-id') actor?: string) {
    const driverId = query['driverId'] ?? actor;
    if (!driverId || !uuid.safeParse(driverId).success) throw problem(400, 'Bad Request', 'driverId (uuid) is required');
    this.assertActor(actor, driverId);
    const limit = query['limit'] === undefined ? 50 : Number(query['limit']);
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw problem(400, 'Bad Request', 'limit must be an integer in 1..100');
    let before: Date | undefined;
    if (query['before'] !== undefined) {
      before = new Date(query['before']);
      if (Number.isNaN(before.getTime())) throw problem(400, 'Bad Request', 'before must be an ISO-8601 timestamp');
    }
    return this.deps.invoices.listForDriver(driverId, limit, before);
  }

  @Get('invoices/:invoiceId')
  async get(@Param('invoiceId') invoiceId: string, @Headers('x-driver-id') actor?: string) {
    if (!uuid.safeParse(invoiceId).success) throw problem(400, 'Bad Request', 'invoiceId must be a uuid');
    const invoice = await this.deps.invoices.get(invoiceId);
    // Another driver's invoice looks exactly like a missing one.
    if (!invoice || (actor !== undefined && invoice.driverId !== actor)) throw new NotFoundException(problem(404, 'Not Found', 'invoice not found').getResponse());
    return invoice;
  }

  private assertActor(actor: string | undefined, driverId: string): void {
    if (actor !== undefined && actor !== driverId) throw new ForbiddenException(problem(403, 'Forbidden', 'x-driver-id does not match the requested driver').getResponse());
  }
}
