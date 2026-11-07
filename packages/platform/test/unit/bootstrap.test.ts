import { Controller, Get, Module, UseGuards } from '@nestjs/common';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { InternalTokenGuard } from '../../src/auth/internal-token.guard';
import { HealthRegistry } from '../../src/health/health';
import { bootstrapService, type RunningService } from '../../src/lifecycle/bootstrap';
import { captureLogger, sample } from '../helpers';

const TOKEN = 'unit-test-internal-token';

@Controller('internal')
@UseGuards(InternalTokenGuard)
class InternalController {
  @Get('ping')
  ping() {
    return { pong: true };
  }
}

@Controller('public')
class PublicController {
  @Get('hello')
  hello() {
    return { hello: 'world' };
  }
}

@Module({ controllers: [InternalController, PublicController] })
class AppModule {}

describe('bootstrapService', () => {
  let svc: RunningService;
  const health = new HealthRegistry();
  const events: string[] = [];
  const { logger, lines } = captureLogger('boot-test');
  let dbUp = true;

  beforeAll(async () => {
    health.register('db', () => dbUp);
    svc = await bootstrapService({
      module: AppModule,
      config: { SERVICE_NAME: 'boot-test', LOG_LEVEL: 'info', HTTP_PORT: 0, INTERNAL_TOKEN: TOKEN, SHUTDOWN_GRACE_MS: 3000 },
      logger,
      health,
      drainDelayMs: 150,
      handleSignals: false,
      onShutdown: [
        { name: 'pool', run: () => events.push('pool') },
        { name: 'consumer', run: () => events.push('consumer') },
      ],
    });
  });
  afterAll(async () => {
    await svc.shutdown();
  });

  it('serves app routes and liveness', async () => {
    expect(await (await fetch(`${svc.url}/public/hello`)).json()).toEqual({ hello: 'world' });
    const live = await fetch(`${svc.url}/health/live`);
    expect(live.status).toBe(200);
    expect(await live.json()).toEqual({ status: 'ok' });
  });

  it('readiness reflects checks with 503 + detail', async () => {
    let r = await fetch(`${svc.url}/health/ready`);
    expect(r.status).toBe(200);
    dbUp = false;
    r = await fetch(`${svc.url}/health/ready`);
    expect(r.status).toBe(503);
    expect(await r.json()).toMatchObject({ status: 'fail', draining: false, checks: { db: { status: 'fail' } } });
    dbUp = true;
  });

  it('guards internal routes with x-internal-token', async () => {
    expect((await fetch(`${svc.url}/internal/ping`)).status).toBe(401);
    expect((await fetch(`${svc.url}/internal/ping`, { headers: { 'x-internal-token': 'wrong-token-value!!' } })).status).toBe(401);
    const ok = await fetch(`${svc.url}/internal/ping`, { headers: { 'x-internal-token': TOKEN } });
    expect(ok.status).toBe(200);
    expect(await ok.json()).toEqual({ pong: true });
  });

  it('exposes /metrics with the request histogram', async () => {
    const text = await (await fetch(`${svc.url}/metrics`)).text();
    expect(sample(text, 'http_server_requests_seconds_count', { route: '/public/hello', status: '200', service: 'boot-test' })).toBe(1);
    expect(sample(text, 'http_server_requests_seconds_count', { route: '/health/ready', status: '503' })).toBeGreaterThan(0);
  });

  it('routes Nest logs through pino', () => {
    expect(lines.some((l) => l['service'] === 'boot-test' && typeof l['context'] === 'string')).toBe(true);
  });

  it('graceful shutdown: ready -> 503 while draining, then HTTP closes and hooks run in reverse', async () => {
    const done = svc.shutdown('test');
    const r = await fetch(`${svc.url}/health/ready`);
    expect(r.status).toBe(503);
    expect(await r.json()).toMatchObject({ draining: true });
    expect(await done).toEqual({ ok: true, timedOut: false, failed: [] });
    expect(events).toEqual(['consumer', 'pool']);
    await expect(fetch(`${svc.url}/health/live`)).rejects.toThrow(/fetch failed/);
  });
});
