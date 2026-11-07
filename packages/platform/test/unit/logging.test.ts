import { context } from '@opentelemetry/api';
import { beforeAll, describe, expect, it } from 'vitest';
import { NestLoggerAdapter } from '../../src/logging/logger';
import { captureLogger, enableContext, SPAN_ID, spanContext, TRACE_ID } from '../helpers';

describe('createLogger', () => {
  beforeAll(() => enableContext());

  it('writes JSON with service and level label', () => {
    const { logger, lines } = captureLogger('billing');
    logger.info({ a: 1 }, 'hello');
    expect(lines[0]).toMatchObject({ service: 'billing', level: 'info', msg: 'hello', a: 1 });
    expect(lines[0]).not.toHaveProperty('traceId');
    expect(typeof lines[0]?.['time']).toBe('string');
  });

  it('adds traceId/spanId of the active span', () => {
    const { logger, lines } = captureLogger();
    context.with(spanContext(), () => logger.info('in span'));
    expect(lines[0]).toMatchObject({ traceId: TRACE_ID, spanId: SPAN_ID });
  });

  it('redacts secrets at several depths', () => {
    const { logger, lines } = captureLogger();
    logger.info({
      password: 'p',
      token: 't',
      req: { headers: { authorization: 'Bearer x', 'x-internal-token': 'secret', accept: 'json' } },
      user: { name: 'n', password: 'p2' },
    });
    const l = lines[0] as Record<string, unknown> & { req: { headers: Record<string, string> } };
    expect(l['password']).toBe('[REDACTED]');
    expect(l['token']).toBe('[REDACTED]');
    expect(l['req'].headers.authorization).toBe('[REDACTED]');
    expect(l['req'].headers['x-internal-token']).toBe('[REDACTED]');
    expect(l['req'].headers.accept).toBe('json');
    expect(l['user']).toEqual({ name: 'n', password: '[REDACTED]' });
  });
});

describe('NestLoggerAdapter', () => {
  it('maps Nest levels and extracts context/stack', () => {
    const { logger, lines } = captureLogger();
    const nest = new NestLoggerAdapter(logger);
    nest.log('started', 'NestApplication');
    nest.error('boom', 'Error: boom\n    at x', 'ExceptionsHandler');
    nest.warn({ msg: 'structured', id: 7 });
    nest.verbose('v');
    nest.error(new Error('as error'));
    expect(lines[0]).toMatchObject({ level: 'info', msg: 'started', context: 'NestApplication' });
    expect(lines[1]).toMatchObject({ level: 'error', msg: 'boom', context: 'ExceptionsHandler', stack: 'Error: boom\n    at x' });
    expect(lines[2]).toMatchObject({ level: 'warn', msg: 'structured', id: 7 });
    expect(lines[3]).toMatchObject({ level: 'trace', msg: 'v' });
    expect(lines[4]).toMatchObject({ level: 'error', msg: 'as error' });
  });

  it('setLogLevels picks the lowest enabled level', () => {
    const { logger } = captureLogger();
    new NestLoggerAdapter(logger).setLogLevels(['error', 'warn']);
    expect(logger.level).toBe('warn');
  });
});
