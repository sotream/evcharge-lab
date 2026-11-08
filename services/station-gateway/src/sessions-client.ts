import { api } from '@evcharge/contracts';
import { createHttpClient, HttpError, type HttpClient } from '@evcharge/platform';
import type { z } from 'zod';

export type StationCredential = z.infer<typeof api.SessionsInternal.stationCredential>;
export type AuthorizeRes = z.infer<typeof api.SessionsInternal.authorizeRes>;
export type StartTxReq = z.infer<typeof api.SessionsInternal.startTxReq>;
export type StartTxRes = z.infer<typeof api.SessionsInternal.startTxRes>;
export type StopTxReq = z.infer<typeof api.SessionsInternal.stopTxReq>;
export type StopTxRes = z.infer<typeof api.SessionsInternal.stopTxRes>;

/** A sessions response that does not match the contract. */
export class ContractViolationError extends Error {
  override readonly name = 'ContractViolationError';
}

function parse<S extends z.ZodType>(schema: S, body: unknown, what: string): z.infer<S> {
  const r = schema.safeParse(body);
  if (!r.success) throw new ContractViolationError(`${what}: ${r.error.issues[0]?.message ?? 'invalid body'}`);
  return r.data;
}

export interface SessionsClientOptions {
  baseUrl: string;
  internalToken: string;
  /** Hot-path timeout (ADR 0003: 1 s, single attempt). */
  timeoutMs: number;
  fetch?: typeof fetch;
}

/**
 * The gateway's synchronous calls to `sessions` (ADR 0001 rule 1, ADR 0003 hot-path budget):
 * one attempt with a 1 s timeout; the station's own retry (same uniqueId) is the retry, and
 * `idempotency-key = ${stationId}:${uniqueId}` makes it safe.
 */
export class SessionsClient {
  readonly http: HttpClient;

  constructor(opts: SessionsClientOptions) {
    this.http = createHttpClient({
      name: 'sessions',
      baseUrl: opts.baseUrl,
      timeoutMs: opts.timeoutMs,
      retries: 0,
      internalToken: opts.internalToken,
      // Fail fast while sessions is down instead of holding each CALL for the full timeout.
      circuitBreaker: { failureThreshold: 20, openMs: 5000 },
      ...(opts.fetch ? { fetch: opts.fetch } : {}),
    });
  }

  /** null when sessions does not know the station (404). */
  async credential(stationId: string): Promise<StationCredential | null> {
    try {
      const res = await this.http.get(`/internal/stations/${encodeURIComponent(stationId)}/credential`, { retries: 0 });
      return parse(api.SessionsInternal.stationCredential, res.body, 'station credential');
    } catch (err) {
      if (err instanceof HttpError && err.status === 404) return null;
      throw err;
    }
  }

  async authorize(stationId: string, idTag: string): Promise<AuthorizeRes> {
    const res = await this.http.post('/internal/ocpp/authorize', { stationId, idTag } satisfies z.infer<typeof api.SessionsInternal.authorizeReq>);
    return parse(api.SessionsInternal.authorizeRes, res.body, 'authorize');
  }

  async startTransaction(req: StartTxReq): Promise<StartTxRes> {
    const res = await this.http.post('/internal/ocpp/start-transaction', req, { idempotencyKey: `${req.stationId}:${req.ocppMessageId}`, retries: 0 });
    return parse(api.SessionsInternal.startTxRes, res.body, 'start-transaction');
  }

  async stopTransaction(req: StopTxReq): Promise<StopTxRes> {
    const res = await this.http.post('/internal/ocpp/stop-transaction', req, { idempotencyKey: `${req.stationId}:${req.ocppMessageId}`, retries: 0 });
    return parse(api.SessionsInternal.stopTxRes, res.body, 'stop-transaction');
  }
}
