import { describe, expect, it } from 'vitest';
import { ConfigError } from '@evcharge/platform';
import { readConfig } from '../../src/config';
import { TtlCache } from '../../src/sites-cache';

const base = {
  INTERNAL_TOKEN: 'internal-token-0123456789',
  DATABASE_URL: 'postgres://u:p@localhost:5432/drivers',
  REDIS_URL: 'redis://localhost:6379',
  SESSIONS_URL: 'http://sessions:8080',
  BILLING_URL: 'http://billing:8080',
  TELEMETRY_URL: 'http://telemetry:8080',
  JWT_SECRET: 'x'.repeat(32),
};

describe('config', () => {
  it('accepts a 32 byte secret and parses previous secrets', () => {
    const c = readConfig({ ...base, JWT_PREVIOUS_SECRETS: `${'y'.repeat(32)}, ${'z'.repeat(40)}` });
    expect(c.JWT_PREVIOUS_SECRETS).toHaveLength(2);
    expect(c.ACCESS_TOKEN_TTL_S).toBe(900);
    expect(c.REFRESH_TOKEN_TTL_S).toBe(30 * 24 * 3600);
  });

  it('rejects short secrets, including short previous ones', () => {
    expect(() => readConfig({ ...base, JWT_SECRET: 'x'.repeat(31) })).toThrow(ConfigError);
    expect(() => readConfig({ ...base, JWT_PREVIOUS_SECRETS: 'short' })).toThrow(/at least 32 bytes/);
  });
});

describe('TRUST_PROXY', () => {
  it('is a hop count or an IP/CIDR list, never "trust all"', () => {
    expect(readConfig(base).TRUST_PROXY).toBe(0);
    expect(readConfig({ ...base, TRUST_PROXY: '1' }).TRUST_PROXY).toBe(1);
    expect(readConfig({ ...base, TRUST_PROXY: '10.0.0.0/8, 192.168.1.5' }).TRUST_PROXY).toEqual(['10.0.0.0/8', '192.168.1.5']);
    for (const bad of ['true', 'all', '*', 'yes']) expect(() => readConfig({ ...base, TRUST_PROXY: bad })).toThrow(ConfigError);
  });
});

describe('TtlCache', () => {
  it('coalesces concurrent loads, expires, and does not cache failures', async () => {
    let now = 0;
    let calls = 0;
    const c = new TtlCache<number>(3000, () => now);
    const load = async () => ++calls;
    expect(await Promise.all([c.get('k', load), c.get('k', load), c.get('k', load)])).toEqual([1, 1, 1]);
    now = 2999;
    expect(await c.get('k', load)).toBe(1);
    now = 3000;
    expect(await c.get('k', load)).toBe(2);
    await expect(c.get('bad', () => Promise.reject(new Error('x')))).rejects.toThrow('x');
    expect(await c.get('bad', load)).toBe(3);
  });
});
