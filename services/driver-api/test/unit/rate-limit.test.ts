/* oxlint-disable no-await-in-loop */ // sequential requests are the point of these tests
import { describe, expect, it } from 'vitest';
import type { Redis } from '@evcharge/platform';
import { RateLimiter } from '../../src/rate-limit';

/** Just enough of ioredis: MULTI INCR/EXPIRE with per-key counters. */
function fakeRedis(fail = false): Redis {
  const counts = new Map<string, number>();
  return {
    multi() {
      let key = '';
      const chain = {
        incr(k: string) {
          key = k;
          return chain;
        },
        expire() {
          return chain;
        },
        async exec() {
          if (fail) throw new Error('redis down');
          counts.set(key, (counts.get(key) ?? 0) + 1);
          return [
            [null, counts.get(key)],
            [null, 1],
          ];
        },
      };
      return chain;
    },
  } as unknown as Redis;
}

describe('RateLimiter (fixed window)', () => {
  it('allows `limit` hits per window and id, then blocks until the next window', async () => {
    let now = 120_000;
    const rl = new RateLimiter(fakeRedis(), 'p:', 3, 60, () => now);
    for (let i = 0; i < 3; i++) expect((await rl.hit('login', 'a')).allowed).toBe(true);
    expect(await rl.hit('login', 'a')).toEqual({ allowed: false, retryAfterS: 60 });
    expect((await rl.hit('login', 'b')).allowed).toBe(true);
    now += 30_000;
    expect((await rl.hit('login', 'a')).retryAfterS).toBe(30);
    now += 30_000;
    expect((await rl.hit('login', 'a')).allowed).toBe(true);
  });

  it('fails open and reports when Redis is down', async () => {
    const errors: unknown[] = [];
    const rl = new RateLimiter(fakeRedis(true), 'p:', 1, 60, Date.now, (e) => errors.push(e));
    expect((await rl.hit('login', 'a')).allowed).toBe(true);
    expect(errors).toHaveLength(1);
  });
});
