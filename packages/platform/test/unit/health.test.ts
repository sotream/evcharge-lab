import { describe, expect, it } from 'vitest';
import { HealthRegistry } from '../../src/health/health';

describe('HealthRegistry', () => {
  it('is ready with no checks', async () => {
    expect(await new HealthRegistry().check()).toEqual({ status: 'ok', draining: false, checks: {} });
  });

  it('fails on throw, false and timeout; passes otherwise', async () => {
    const h = new HealthRegistry();
    h.register('ok', async () => {});
    h.register('sync', () => true);
    expect((await h.check()).status).toBe('ok');
    h.register('throws', async () => {
      throw new Error('db down');
    });
    h.register('false', () => false);
    h.register('slow', () => new Promise((r) => setTimeout(r, 500)), { timeoutMs: 30 });
    const r = await h.check();
    expect(r.status).toBe('fail');
    expect(r.checks['ok']?.status).toBe('ok');
    expect(r.checks['throws']).toMatchObject({ status: 'fail', error: 'db down' });
    expect(r.checks['false']).toMatchObject({ status: 'fail', error: 'check returned false' });
    expect(r.checks['slow']?.error).toMatch(/timed out after 30ms/);
    expect(r.checks['slow']?.durationMs).toBeLessThan(400);
  });

  it('unregister removes only its own registration', async () => {
    const h = new HealthRegistry();
    const off1 = h.register('a', () => false);
    h.register('a', () => true);
    off1();
    expect(h.names()).toEqual(['a']);
    expect((await h.check()).status).toBe('ok');
  });

  it('draining fails readiness even if checks pass', async () => {
    const h = new HealthRegistry();
    h.register('ok', () => true);
    h.setDraining();
    expect(await h.check()).toMatchObject({ status: 'fail', draining: true, checks: { ok: { status: 'ok' } } });
  });
});
