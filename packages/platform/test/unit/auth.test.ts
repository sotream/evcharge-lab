import { UnauthorizedException, type ExecutionContext } from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import { InternalTokenGuard, safeTokenEqual } from '../../src/auth/internal-token.guard';

const ctx = (headers: Record<string, string | string[]>) => ({ switchToHttp: () => ({ getRequest: () => ({ headers }) }) }) as unknown as ExecutionContext;

describe('InternalTokenGuard', () => {
  const guard = new InternalTokenGuard('a-very-secret-token');

  it('accepts the exact token', () => {
    expect(guard.canActivate(ctx({ 'x-internal-token': 'a-very-secret-token' }))).toBe(true);
    expect(guard.canActivate(ctx({ 'x-internal-token': ['a-very-secret-token'] }))).toBe(true);
  });

  it('rejects missing, wrong and prefix tokens with 401', () => {
    for (const h of [{} as Record<string, string>,{ 'x-internal-token': '' }, { 'x-internal-token': 'a-very-secret-toke' }, { 'x-internal-token': 'a-very-secret-token-x' }]) {
      expect(() => guard.canActivate(ctx(h))).toThrow(UnauthorizedException);
    }
  });

  it('refuses an empty configured token', () => {
    expect(() => new InternalTokenGuard('')).toThrow(/INTERNAL_TOKEN is empty/);
  });

  it('safeTokenEqual compares by value', () => {
    expect(safeTokenEqual('abc', 'abc')).toBe(true);
    expect(safeTokenEqual('abc', 'abd')).toBe(false);
    expect(safeTokenEqual('abc', 'abcd')).toBe(false);
  });
});
