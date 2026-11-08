import { type ArgumentsHost, Catch, type ExceptionFilter, HttpException, Inject, Injectable } from '@nestjs/common';
import { CircuitOpenError, HttpError, HttpNetworkError, HttpTimeoutError, type Logger } from '@evcharge/platform';
import type { FastifyReply } from 'fastify';
import type { z } from 'zod';
import { LOGGER } from './tokens';

/** An error that renders as application/problem+json (ADR 0002) with a stable machine `code`. */
export class ProblemError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    readonly title: string,
    readonly detail?: string,
    readonly headers: Record<string, string> = {},
  ) {
    super(detail ?? title);
  }
}

export const badRequest = (detail: string) => new ProblemError(400, 'bad_request', 'Bad request', detail);
export const unauthorized = (code: string, detail: string) => new ProblemError(401, code, 'Unauthorized', detail, { 'www-authenticate': 'Bearer' });

/** Parses `value` with `schema` or throws a 400 problem listing every issue. */
export function parse<S extends z.ZodType>(schema: S, value: unknown): z.infer<S> {
  const r = schema.safeParse(value);
  if (!r.success) throw badRequest(r.error.issues.map((i) => `${i.path.join('.') || '(body)'}: ${i.message}`).join('; '));
  return r.data;
}

interface ProblemBody {
  type: string;
  title: string;
  status: number;
  detail?: string;
  code?: string;
}

const mk = (status: number, title: string, code: string, detail?: string, headers: Record<string, string> = {}) => ({
  body: { type: 'about:blank', title, status, code, ...(detail ? { detail } : {}) } as ProblemBody,
  headers,
});

/**
 * Downstream problem codes that are safe and useful to show a driver. Anything else keeps its status but loses
 * its code, title and detail, so internal wording (table names, hosts, stack hints) never leaks.
 */
const PASS_THROUGH_CODES = new Set([
  'connector_unavailable',
  'connector_reserved',
  'reservation_exists',
  'station_offline',
  'unknown_station',
  'unknown_connector',
  'driver_blocked',
  'driver_not_registered',
  'driver_conflict',
  'in_flight',
  'idempotency_key_required',
  'idempotency_key_reused',
  'idempotency_in_flight',
  'not_found',
  'forbidden',
  'bad_request',
]);

/**
 * Downstream failures become driver-facing problems: a 4xx the driver can act on (409 connector_unavailable, 404,
 * 422...) passes through with its code; a 401 from downstream means our own service credentials are wrong, which
 * is not the driver's fault (502). 5xx -> 502, timeout -> 504, network failure or open circuit -> 503.
 */
export function toProblem(err: unknown): { body: ProblemBody; headers: Record<string, string> } {
  if (err instanceof ProblemError) return mk(err.status, err.title, err.code, err.detail, err.headers);
  if (err instanceof HttpError) {
    const p = err.problem;
    if (err.status >= 400 && err.status < 500 && err.status !== 401) {
      const code = typeof p['code'] === 'string' && PASS_THROUGH_CODES.has(p['code']) ? p['code'] : undefined;
      if (code) return mk(err.status, p.title, code, p.detail);
      if (err.status === 404) return mk(404, 'Not found', 'not_found', 'resource not found');
      if (err.status === 403) return mk(403, 'Forbidden', 'forbidden', 'not allowed');
      return mk(err.status, 'Request rejected', 'downstream_rejected', 'the request was rejected');
    }
    return mk(502, 'Bad gateway', 'bad_gateway', `${err.request.target} answered ${err.status}`);
  }
  if (err instanceof HttpTimeoutError) return mk(504, 'Gateway timeout', 'downstream_timeout', `${err.request.target} did not answer in time`);
  if (err instanceof CircuitOpenError) return mk(503, 'Service unavailable', 'downstream_unavailable', `${err.request.target} is temporarily unavailable`, { 'retry-after': '10' });
  if (err instanceof HttpNetworkError) return mk(503, 'Service unavailable', 'downstream_unavailable', `${err.request.target} is unreachable`, { 'retry-after': '5' });
  if (err instanceof HttpException) {
    const status = err.getStatus();
    const res = err.getResponse();
    return mk(status, err.name.replace(/Exception$/, ''), status === 404 ? 'not_found' : 'http_error', typeof res === 'string' ? res : String((res as { message?: unknown }).message ?? err.message));
  }
  // Fastify's own errors (bad JSON, body too large, ...) carry a 4xx statusCode.
  const sc = (err as { statusCode?: unknown } | null)?.statusCode;
  if (typeof sc === 'number' && sc >= 400 && sc < 500) return mk(sc, 'Bad request', 'bad_request', (err as Error).message);
  return mk(500, 'Internal error', 'internal');
}

@Catch()
@Injectable()
export class ProblemFilter implements ExceptionFilter {
  constructor(@Inject(LOGGER) private readonly logger: Logger) {}

  catch(err: unknown, host: ArgumentsHost): void {
    const reply = host.switchToHttp().getResponse<FastifyReply>();
    const { body, headers } = toProblem(err);
    if (body.status >= 500) this.logger.error({ code: body.code, err: err instanceof Error ? { message: err.message, stack: err.stack } : String(err) }, 'request failed');
    void reply.status(body.status).headers(headers).header('content-type', 'application/problem+json').send(body);
  }
}
