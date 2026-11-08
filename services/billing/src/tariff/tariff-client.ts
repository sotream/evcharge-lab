import { api } from '@evcharge/contracts';
import { createHttpClient, type HttpClient } from '@evcharge/platform';
import type { BillingConfig } from '../config';

export interface TariffQuoter {
  quote(sessionId: string, req: api.QuoteReq): Promise<api.QuoteRes>;
}

/**
 * tariff POST /v1/quote: 1 s timeout per attempt, 3 attempts (the quote is deterministic, ADR 0011,
 * so the request is sent with an idempotency key and may be retried), consecutive-failure circuit
 * breaker. When this still fails, the saga stays in PRICING and the scheduler retries later.
 */
export class TariffClient implements TariffQuoter {
  private readonly http: HttpClient;

  constructor(config: Pick<BillingConfig, 'TARIFF_URL' | 'INTERNAL_TOKEN' | 'TARIFF_TIMEOUT_MS' | 'TARIFF_ATTEMPTS' | 'TARIFF_BREAKER_OPEN_MS'>) {
    this.http = createHttpClient({
      name: 'tariff',
      baseUrl: config.TARIFF_URL,
      internalToken: config.INTERNAL_TOKEN,
      timeoutMs: config.TARIFF_TIMEOUT_MS,
      retries: config.TARIFF_ATTEMPTS - 1,
      backoffBaseMs: 50,
      backoffMaxMs: 400,
      circuitBreaker: { failureThreshold: 5, openMs: config.TARIFF_BREAKER_OPEN_MS },
    });
  }

  async quote(sessionId: string, req: api.QuoteReq): Promise<api.QuoteRes> {
    const res = await this.http.post<unknown>('/v1/quote', req, { idempotencyKey: `quote:${sessionId}` });
    return api.QuoteRes.parse(res.body);
  }
}
