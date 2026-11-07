import { type CanActivate, type DynamicModule, type ExecutionContext, Global, Inject, Injectable, Module, UnauthorizedException } from '@nestjs/common';
import { createHash, timingSafeEqual } from 'node:crypto';
import { INTERNAL_TOKEN_HEADER } from './constants';

export { INTERNAL_TOKEN_HEADER };
export const INTERNAL_TOKEN = Symbol('INTERNAL_TOKEN');

const digest = (s: string) => createHash('sha256').update(s).digest();

/** Constant-time comparison (hashing first so differing lengths don't leak through timing). */
export function safeTokenEqual(given: string, expected: string): boolean {
  return timingSafeEqual(digest(given), digest(expected)) && given.length === expected.length;
}

/**
 * Guards service-to-service endpoints: requires header `x-internal-token` equal to the configured
 * INTERNAL_TOKEN (401 otherwise). bootstrapService provides the token globally, so services only
 * write `@UseGuards(InternalTokenGuard)`.
 */
@Injectable()
export class InternalTokenGuard implements CanActivate {
  constructor(@Inject(INTERNAL_TOKEN) private readonly token: string) {
    if (!token) throw new Error('InternalTokenGuard: INTERNAL_TOKEN is empty');
  }

  canActivate(ctx: ExecutionContext): boolean {
    const req = ctx.switchToHttp().getRequest<{ headers: Record<string, string | string[] | undefined> }>();
    const raw = req.headers[INTERNAL_TOKEN_HEADER];
    const given = Array.isArray(raw) ? raw[0] : raw;
    if (!given || !safeTokenEqual(given, this.token)) throw new UnauthorizedException('missing or invalid internal token');
    return true;
  }
}

@Global()
@Module({})
export class InternalAuthModule {
  static forRoot(token: string): DynamicModule {
    return {
      module: InternalAuthModule,
      providers: [{ provide: INTERNAL_TOKEN, useValue: token }, InternalTokenGuard],
      exports: [INTERNAL_TOKEN, InternalTokenGuard],
    };
  }
}
