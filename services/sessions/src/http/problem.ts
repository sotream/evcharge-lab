import { type ArgumentsHost, Catch, type ExceptionFilter, HttpException, Inject, Injectable } from '@nestjs/common';
import { IdempotencyInFlightError, IdempotencyMismatchError, type Logger } from '@evcharge/platform';
import type { FastifyReply } from 'fastify';
import type { z } from 'zod';
import { LOGGER } from '../tokens';

/** An error that renders as application/problem+json (ADR 0002) with a stable machine `code`. */
export class ProblemError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    readonly title: string,
    readonly detail?: string,
  ) {
    super(detail ?? title);
  }
}

export const badRequest = (detail: string) => new ProblemError(400, 'bad_request', 'Bad request', detail);
export const notFound = (what: string) => new ProblemError(404, 'not_found', 'Not found', `${what} not found`);

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

export function toProblem(err: unknown): ProblemBody {
  if (err instanceof ProblemError) return { type: 'about:blank', title: err.title, status: err.status, code: err.code, ...(err.detail ? { detail: err.detail } : {}) };
  if (err instanceof IdempotencyMismatchError) return { type: 'about:blank', title: 'Idempotency key reused', status: 422, code: 'idempotency_key_reused', detail: err.message };
  if (err instanceof IdempotencyInFlightError) return { type: 'about:blank', title: 'Request in progress', status: 409, code: 'idempotency_in_flight', detail: err.message };
  if (err instanceof HttpException) {
    const status = err.getStatus();
    const res = err.getResponse();
    const detail = typeof res === 'string' ? res : String((res as { message?: unknown }).message ?? err.message);
    return { type: 'about:blank', title: err.name.replace(/Exception$/, ''), status, code: status === 401 ? 'unauthorized' : status === 404 ? 'not_found' : 'http_error', detail };
  }
  return { type: 'about:blank', title: 'Internal error', status: 500, code: 'internal' };
}

/** Renders every error of the service's own controllers as problem+json. */
@Catch()
@Injectable()
export class ProblemFilter implements ExceptionFilter {
  constructor(@Inject(LOGGER) private readonly logger: Logger) {}

  catch(err: unknown, host: ArgumentsHost): void {
    const reply = host.switchToHttp().getResponse<FastifyReply>();
    const body = toProblem(err);
    if (body.status >= 500) this.logger.error({ err: err instanceof Error ? { message: err.message, stack: err.stack } : String(err) }, 'request failed');
    void reply.status(body.status).header('content-type', 'application/problem+json').send(body);
  }
}
