import { InternalTokenGuard } from '@evcharge/platform';
import { Body, Controller, Delete, Get, Headers, HttpCode, HttpException, Inject, Param, Post, Query, UseGuards } from '@nestjs/common';
import { z } from 'zod';
import { markDriverDeleted, scrubDeletedDrivers } from '../gdpr';
import { CreditNoteError } from '../invoices/credit-notes';
import { BILLING_DEPS, type BillingDeps } from './billing.controller';

const uuid = z.uuid();
const creditNoteReq = z.object({ amountMinor: z.number().int().positive().max(10_000_000), reason: z.string().trim().min(1).max(500) });

function problem(status: number, title: string, detail: string): HttpException {
  return new HttpException({ type: 'about:blank', title, status, detail }, status);
}

/** Service-to-service and operator routes (internal token only; no driver identity). */
@Controller('internal')
@UseGuards(InternalTokenGuard)
export class InternalController {
  constructor(@Inject(BILLING_DEPS) private readonly deps: BillingDeps) {}

  /** Sagas that stopped on their own and wait for a person: held for energy review, or parked by an error. */
  @Get('sagas/parked')
  async parked(@Query('limit') limitRaw?: string) {
    const limit = limitRaw === undefined ? 100 : Number(limitRaw);
    if (!Number.isInteger(limit) || limit < 1 || limit > 500) throw problem(400, 'Bad Request', 'limit must be an integer in 1..500');
    const { rows } = await this.deps.pool.query<{ session_id: string; state: string; failure_reason: string | null; updated_at: Date }>(
      "SELECT session_id, state, failure_reason, updated_at FROM billing_sagas WHERE next_attempt_at IS NULL AND state IN ('PRICING', 'INVOICED', 'CAPTURING') ORDER BY updated_at DESC LIMIT $1",
      [limit],
    );
    return rows.map((r) => ({ sessionId: r.session_id, state: r.state, failureReason: r.failure_reason, updatedAt: r.updated_at.toISOString() }));
  }

  @Post('sagas/:sessionId/resume')
  @HttpCode(200)
  async resume(@Param('sessionId') sessionId: string, @Body() body?: unknown) {
    if (!uuid.safeParse(sessionId).success) throw problem(400, 'Bad Request', 'sessionId must be a uuid');
    const acceptEnergy = z.object({ acceptEnergy: z.boolean().optional() }).safeParse(body ?? {});
    if (!acceptEnergy.success) throw problem(400, 'Bad Request', 'body must be { acceptEnergy?: boolean }');
    const result = await this.deps.engine.resume(sessionId, acceptEnergy.data.acceptEnergy === true);
    if (result === 'not_found') throw problem(404, 'Not Found', 'saga not found');
    if (result === 'not_parked') throw problem(409, 'Conflict', 'saga is not parked');
    return { result };
  }

  /** Account deletion (GDPR): marks the driver deleted and drops the stored card and saga tokens once no saga of theirs is unfinished (the runtime sweeps the rest). Invoices stay (retention). Idempotent. */
  @Delete('drivers/:driverId/payment-data')
  @HttpCode(204)
  async deletePaymentData(@Param('driverId') driverId: string): Promise<void> {
    if (!uuid.safeParse(driverId).success) throw problem(400, 'Bad Request', 'driverId must be a uuid');
    await markDriverDeleted(this.deps.pool, driverId);
    await scrubDeletedDrivers(this.deps.pool);
  }

  /**
   * Credit note against an invoice, with a PSP refund of what was collected (ADR 0007). `idempotency-key` is required:
   * repeating a request returns the same credit note and finishes any refund left pending by a PSP timeout.
   */
  @Post('invoices/:invoiceId/credit-notes')
  @HttpCode(201)
  async creditNote(@Param('invoiceId') invoiceId: string, @Body() body: unknown, @Headers('idempotency-key') key?: string) {
    if (!uuid.safeParse(invoiceId).success) throw problem(400, 'Bad Request', 'invoiceId must be a uuid');
    if (!key || key.length > 200) throw problem(400, 'Bad Request', 'idempotency-key header is required (at most 200 characters)');
    const parsed = creditNoteReq.safeParse(body);
    if (!parsed.success) throw problem(400, 'Bad Request', parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; '));
    try {
      return await this.deps.creditNotes.issue(invoiceId, key, parsed.data.amountMinor, parsed.data.reason);
    } catch (err) {
      if (err instanceof CreditNoteError) throw problem(err.status, err.status === 404 ? 'Not Found' : err.status === 409 ? 'Conflict' : 'Bad Request', err.message);
      throw err;
    }
  }

}
