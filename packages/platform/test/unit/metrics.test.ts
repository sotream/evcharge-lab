import Fastify from 'fastify';
import { describe, expect, it } from 'vitest';
import { counter, gauge, histogram, initMetrics, registerHttpMetrics, registry } from '../../src/metrics/metrics';
import { sample } from '../helpers';

describe('metrics', () => {
  it('helpers get-or-create on the shared registry', async () => {
    const a = counter({ name: 'test_things_total', help: 'h', labelNames: ['k'] as const });
    const b = counter({ name: 'test_things_total', help: 'h', labelNames: ['k'] as const });
    expect(a).toBe(b);
    a.inc({ k: 'v' }, 2);
    gauge({ name: 'test_gauge', help: 'h' }).set(5);
    histogram({ name: 'test_hist_seconds', help: 'h', buckets: [1] }).observe(0.5);
    const text = await registry.metrics();
    expect(text).toContain('test_things_total{k="v"');
    expect(text).toContain('test_gauge');
    expect(text).toContain('test_hist_seconds_bucket');
  });

  it('adds service label, default metrics, request histogram and /metrics', async () => {
    initMetrics({ service: 'unit-svc' });
    initMetrics({ service: 'unit-svc' }); // idempotent
    const app = Fastify();
    registerHttpMetrics(app);
    app.get('/items/:id', async () => ({ ok: true }));
    await app.ready();
    expect((await app.inject({ url: '/items/42' })).statusCode).toBe(200);
    expect((await app.inject({ url: '/nope' })).statusCode).toBe(404);
    const res = await app.inject({ url: '/metrics' });
    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('text/plain');
    expect(sample(res.body, 'http_server_requests_seconds_count', { method: 'GET', route: '/items/:id', status: '200', service: 'unit-svc' })).toBe(1);
    expect(sample(res.body, 'http_server_requests_seconds_count', { route: 'unmatched', status: '404' })).toBe(1);
    expect(res.body).toContain('process_cpu_user_seconds_total');
    expect(res.body).not.toContain('route="/metrics"');
    await app.close();
  });
});
