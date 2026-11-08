import { describe, expect, it } from 'vitest';
import { HealthRegistry } from '../../src/health/health';
import { Lifecycle } from '../../src/lifecycle/lifecycle';
import { captureLogger } from '../helpers';

function make(graceMs = 1000, drainDelayMs = 20) {
  const { logger, lines } = captureLogger();
  const health = new HealthRegistry();
  return { lc: new Lifecycle({ logger, health, graceMs, drainDelayMs }), health, lines };
}

describe('Lifecycle', () => {
  it('drains, waits, closes http, then runs hooks in reverse order', async () => {
    const { lc, health } = make(1000, 50);
    const events: string[] = [];
    const t0 = Date.now();
    lc.onShutdown('otel', () => events.push('otel'));
    lc.onShutdown('pool', async () => events.push('pool'));
    lc.onShutdown('consumer', () => events.push('consumer'));
    lc.setHttpCloser(async () => {
      events.push(`http@${Date.now() - t0 >= 45 ? 'after-delay' : 'too-early'}`);
    });
    const p = lc.shutdown('test');
    expect(health.draining).toBe(true);
    expect(lc.shuttingDown).toBe(true);
    expect(lc.shutdown()).toBe(p); // idempotent
    expect(await p).toEqual({ ok: true, timedOut: false, failed: [] });
    expect(events).toEqual(['http@after-delay', 'consumer', 'pool', 'otel']);
  });

  it('continues past a failing hook and reports it', async () => {
    const { lc } = make();
    const events: string[] = [];
    lc.onShutdown('a', () => events.push('a'));
    lc.onShutdown('bad', () => {
      throw new Error('nope');
    });
    const r = await lc.shutdown();
    expect(r).toEqual({ ok: false, timedOut: false, failed: ['bad'] });
    expect(events).toEqual(['a']);
  });

  it('enforces the overall grace deadline', async () => {
    const { lc, lines } = make(150, 10);
    const events: string[] = [];
    lc.onShutdown('never-runs', () => events.push('never-runs'));
    lc.onShutdown('hangs', () => new Promise(() => {}));
    const t0 = Date.now();
    const r = await lc.shutdown();
    expect(Date.now() - t0).toBeLessThan(2000); // grace is 150 ms; the hook never settles, so anything bounded proves the deadline (wide margin for loaded machines)
    expect(r.ok).toBe(false);
    expect(r.timedOut || r.failed.includes('hangs')).toBe(true);
    expect(events).toEqual([]);
    expect(lines.some((l) => String(l['msg']).includes('deadline') || String(l['msg']).includes('hook failed'))).toBe(true);
  });

  it('signal handler shuts down then exits; second signal exits at once', async () => {
    const { lc } = make(500, 10);
    const before = new Set(process.listeners('SIGTERM'));
    const codes: number[] = [];
    let resolveExit!: () => void;
    const exited = new Promise<void>((r) => (resolveExit = r));
    lc.installSignalHandlers((c) => {
      codes.push(c);
      resolveExit();
    });
    const handler = process.listeners('SIGTERM').find((l) => !before.has(l)) as (s: NodeJS.Signals) => void;
    const sigint = process.listeners('SIGINT').at(-1) as (s: NodeJS.Signals) => void;
    try {
      handler('SIGTERM');
      handler('SIGTERM');
      expect(codes).toEqual([1]);
      codes.length = 0;
      await lc.shutdown();
      await exited;
      await new Promise((r) => setTimeout(r, 10));
      expect(codes).toContain(0);
    } finally {
      process.removeListener('SIGTERM', handler);
      process.removeListener('SIGINT', sigint);
    }
  });
});
