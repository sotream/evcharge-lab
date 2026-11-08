import {
  BadRequestException,
  type CallHandler,
  ConflictException,
  type DynamicModule,
  type ExecutionContext,
  Global,
  Inject,
  Injectable,
  Module,
  type NestInterceptor,
  UnprocessableEntityException,
} from '@nestjs/common';
import type { FastifyReply, FastifyRequest } from 'fastify';
import { catchError, from, mergeMap, type Observable, of } from 'rxjs';
import type { Pool } from '../postgres/postgres';
import { beginIdempotent, completeIdempotent, hashRequest, IdempotencyInFlightError, IdempotencyMismatchError, releaseIdempotent } from './idempotency';

export interface IdempotencyModuleOptions {
  pool: Pool;
  /** Reject requests without an idempotency-key header (400). Default false (pass through). */
  required?: boolean;
  lockTtlMs?: number;
}

export const IDEMPOTENCY_OPTIONS = Symbol('IDEMPOTENCY_OPTIONS');
export const IDEMPOTENCY_KEY_HEADER = 'idempotency-key';
export const IDEMPOTENT_REPLAYED_HEADER = 'idempotent-replayed';

/**
 * `@UseInterceptors(IdempotencyInterceptor)` on a route: same key + same body replays the stored
 * status/body (header `idempotent-replayed: true`), same key + different body -> 422, duplicate
 * while the first is in flight -> 409. The response is stored after the handler returns, i.e. not
 * in the handler's transaction; for strict atomicity use `withIdempotency` inside the handler.
 */
@Injectable()
export class IdempotencyInterceptor implements NestInterceptor {
  constructor(@Inject(IDEMPOTENCY_OPTIONS) private readonly opts: IdempotencyModuleOptions) {}

  intercept(ctx: ExecutionContext, next: CallHandler): Observable<unknown> {
    const http = ctx.switchToHttp();
    const req = http.getRequest<FastifyRequest>();
    const reply = http.getResponse<FastifyReply>();
    const raw = req.headers[IDEMPOTENCY_KEY_HEADER];
    const key = Array.isArray(raw) ? raw[0] : raw;
    if (!key) {
      if (this.opts.required) throw new BadRequestException(`${IDEMPOTENCY_KEY_HEADER} header is required`);
      return next.handle();
    }
    if (key.length > 255) throw new BadRequestException(`${IDEMPOTENCY_KEY_HEADER} is too long`);
    // Caller identity is part of the scope: two drivers must never share a stored response for the same key.
    const caller = req.headers['x-driver-id'] ?? '-';
    const scope = `${Array.isArray(caller) ? caller[0] : caller} ${req.method} ${req.routeOptions?.url ?? req.url}`;
    const requestHash = hashRequest(req.method, req.url, req.body);
    const { pool } = this.opts;

    return from(beginIdempotent(pool, { scope, key, requestHash, ...(this.opts.lockTtlMs ? { lockTtlMs: this.opts.lockTtlMs } : {}) })).pipe(
      catchError((err: unknown) => {
        if (err instanceof IdempotencyMismatchError) throw new UnprocessableEntityException({ type: 'about:blank', title: 'Idempotency key reused', status: 422, detail: err.message });
        if (err instanceof IdempotencyInFlightError) throw new ConflictException({ type: 'about:blank', title: 'Request in progress', status: 409, detail: err.message });
        throw err;
      }),
      mergeMap((begin) => {
        if (begin.kind === 'replay') {
          reply.status(begin.response.status);
          reply.header(IDEMPOTENT_REPLAYED_HEADER, 'true');
          return of(begin.response.body);
        }
        return next.handle().pipe(
          mergeMap(async (body: unknown) => {
            await completeIdempotent(pool, scope, key, { status: reply.statusCode, body });
            return body;
          }),
          catchError(async (failure: unknown) => {
            await releaseIdempotent(pool, scope, key).catch(() => {});
            throw failure;
          }),
        );
      }),
    );
  }
}

@Global()
@Module({})
export class IdempotencyModule {
  static forRoot(opts: IdempotencyModuleOptions): DynamicModule {
    return {
      module: IdempotencyModule,
      providers: [{ provide: IDEMPOTENCY_OPTIONS, useValue: opts }, IdempotencyInterceptor],
      exports: [IDEMPOTENCY_OPTIONS, IdempotencyInterceptor],
    };
  }
}
