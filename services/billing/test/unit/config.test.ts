import { ConfigError, loadConfig } from '@evcharge/platform';
import { describe, expect, it } from 'vitest';
import { billingConfigSchema } from '../../src/config';

const base = {
  SERVICE_NAME: 'billing',
  INTERNAL_TOKEN: 'test-internal-token-0123456789',
  DATABASE_URL: 'postgres://x@localhost/billing',
  KAFKA_BROKERS: 'a:9092,b:9092',
  TARIFF_URL: 'http://tariff:8080',
  TOKEN_ENC_KEY: Buffer.alloc(32, 7).toString('base64'),
};

describe('billing config', () => {
  it('applies ADR 0007 defaults', () => {
    const c = loadConfig(billingConfigSchema, base);
    expect(c.HOLD_AMOUNT).toBe(5000);
    expect(c.HOLD_CURRENCY).toBe('EUR');
    expect(c.TARIFF_TIMEOUT_MS).toBe(1000);
    expect(c.TARIFF_ATTEMPTS).toBe(3);
    expect(c.SAGA_BACKOFF_BASE_MS).toBe(1000);
    expect(c.SAGA_BACKOFF_MAX_MS).toBe(30_000);
    expect(c.SAGA_PRICING_DEADLINE_MS).toBe(24 * 3600 * 1000);
    expect(c.PSP_TIMEOUT_RATE).toBe(0);
    expect(c.PSP_DECLINE_TOKENS).toEqual([]);
    expect(c.KAFKA_BROKERS).toEqual(['a:9092', 'b:9092']);
  });

  it('parses PSP failure injection', () => {
    const c = loadConfig(billingConfigSchema, { ...base, PSP_TIMEOUT_RATE: '0.25', PSP_DECLINE_TOKENS: 'tok_a, tok_b', PSP_LATENCY_MS: '15' });
    expect(c.PSP_TIMEOUT_RATE).toBe(0.25);
    expect(c.PSP_DECLINE_TOKENS).toEqual(['tok_a', 'tok_b']);
    expect(c.PSP_LATENCY_MS).toBe(15);
  });

  it('fails fast, listing every bad variable', () => {
    const thrown = (() => {
      try {
        loadConfig(billingConfigSchema, { SERVICE_NAME: 'billing', INTERNAL_TOKEN: 'short', PSP_TIMEOUT_RATE: '2', TARIFF_URL: 'nope' });
      } catch (err) {
        return err;
      }
    })();
    expect(thrown).toBeInstanceOf(ConfigError);
    const issues = (thrown as ConfigError).issues.join('\n');
    for (const v of ['INTERNAL_TOKEN', 'DATABASE_URL', 'KAFKA_BROKERS', 'TARIFF_URL', 'PSP_TIMEOUT_RATE']) expect(issues).toContain(v);
  });

  it('requires a 32-byte TOKEN_ENC_KEY, with no built-in default', () => {
    const { TOKEN_ENC_KEY: _key, ...noKey } = base;
    expect(() => loadConfig(billingConfigSchema, noKey)).toThrow(ConfigError);
    expect(() => loadConfig(billingConfigSchema, { ...base, TOKEN_ENC_KEY: Buffer.alloc(16, 7).toString('base64') })).toThrow(ConfigError);
    expect(loadConfig(billingConfigSchema, base).TOKEN_ENC_KEY).toBe(base.TOKEN_ENC_KEY);
  });
});
