import { afterAll, describe, expect, it } from 'vitest';
import { closeRedis, createRedis, redisReadinessCheck } from '../../src/redis/redis';
import { REDIS_URL, uniq } from './infra';

describe('redis', () => {
  const r = createRedis(REDIS_URL, { commandTimeoutMs: 300 });
  afterAll(() => closeRedis(r));

  it('connects and passes readiness', async () => {
    expect(await redisReadinessCheck(r)()).toBe(true);
    const k = uniq('k');
    await r.set(k, 'v', 'EX', 10);
    expect(await r.get(k)).toBe('v');
  });

  it('enforces the command timeout', async () => {
    await expect(r.blpop(uniq('empty'), 2)).rejects.toThrow(/timed out/i);
  });

  it('readiness fails for an unreachable server', async () => {
    const bad = createRedis('redis://127.0.0.1:1', { connectTimeoutMs: 200, maxRetriesPerRequest: 0, options: { retryStrategy: () => null } });
    await expect(redisReadinessCheck(bad)()).rejects.toThrow(/connect|ECONNREFUSED|Connection|closed/i);
    bad.disconnect();
  });
});
