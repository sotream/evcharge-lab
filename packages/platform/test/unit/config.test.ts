import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import { baseConfigSchema, ConfigError, csv, envBool, loadConfig } from '../../src/config/config';

const good = { SERVICE_NAME: 'sessions', INTERNAL_TOKEN: 'x'.repeat(16) };

describe('loadConfig', () => {
  it('applies defaults and coerces numbers', () => {
    const c = loadConfig(baseConfigSchema, { ...good, HTTP_PORT: '9090' });
    expect(c).toEqual({ SERVICE_NAME: 'sessions', INTERNAL_TOKEN: 'x'.repeat(16), LOG_LEVEL: 'info', HTTP_PORT: 9090, SHUTDOWN_GRACE_MS: 15000 });
    expect(c.OTEL_EXPORTER_OTLP_ENDPOINT).toBeUndefined();
  });

  it('treats empty strings as unset', () => {
    const c = loadConfig(baseConfigSchema, { ...good, OTEL_EXPORTER_OTLP_ENDPOINT: '', LOG_LEVEL: '' });
    expect(c.OTEL_EXPORTER_OTLP_ENDPOINT).toBeUndefined();
    expect(c.LOG_LEVEL).toBe('info');
  });

  it('lists every bad variable at once', () => {
    let err: unknown;
    try {
      loadConfig(baseConfigSchema, { INTERNAL_TOKEN: 'short', HTTP_PORT: 'abc', LOG_LEVEL: 'loud', OTEL_EXPORTER_OTLP_ENDPOINT: 'not a url' });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(ConfigError);
    const issues = (err as ConfigError).issues;
    const names = issues.map((i) => i.split(':')[0]);
    expect(names.sort()).toEqual(['HTTP_PORT', 'INTERNAL_TOKEN', 'LOG_LEVEL', 'OTEL_EXPORTER_OTLP_ENDPOINT', 'SERVICE_NAME']);
    expect(issues.find((i) => i.startsWith('SERVICE_NAME'))).toContain('required but not set');
    expect((err as Error).message).toContain('Invalid configuration (5 problems)');
  });

  it('extends with service-specific variables and helpers', () => {
    const schema = baseConfigSchema.extend({ KAFKA_BROKERS: csv(), FEATURE_X: envBool().default(false) });
    const c = loadConfig(schema, { ...good, KAFKA_BROKERS: 'a:1, b:2,', FEATURE_X: 'false' });
    expect(c.KAFKA_BROKERS).toEqual(['a:1', 'b:2']);
    expect(c.FEATURE_X).toBe(false);
    expect(() => loadConfig(schema, { ...good, KAFKA_BROKERS: ' , ' })).toThrow(/KAFKA_BROKERS/);
  });

  it('defaults env to process.env', () => {
    process.env['PLATFORM_TEST_VAR'] = '42';
    expect(loadConfig(z.object({ PLATFORM_TEST_VAR: z.coerce.number() })).PLATFORM_TEST_VAR).toBe(42);
    delete process.env['PLATFORM_TEST_VAR'];
  });
});
