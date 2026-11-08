import { describe, expect, it } from 'vitest';
import { CallQueue, type QueuedCall } from '../../src/call-queue';
import { BACKOFF_CAP_MS, reconnectDelayMs } from '../../src/reconnect';
import { mulberry32 } from '../../src/util';
import { TimerWheel } from '../../src/wheel';

const call = (id: string, action: QueuedCall['action'] = 'MeterValues'): QueuedCall => ({ uniqueId: id, action, payload: {} });

describe('CallQueue (offline replay ordering, ADR 0003)', () => {
  it('is strictly FIFO and only the head can be acknowledged', () => {
    const q = new CallQueue(100);
    q.push(call('a', 'StartTransaction'));
    q.push(call('b'));
    q.push(call('c', 'StopTransaction'));
    expect(q.ack('b')).toBeUndefined();
    expect(q.ids()).toEqual(['a', 'b', 'c']);
    expect(q.ack('a')?.uniqueId).toBe('a');
    expect(q.peek()?.uniqueId).toBe('b');
  });

  it('keeps the same head (same uniqueId) until it is acknowledged, i.e. a resend repeats the original id', () => {
    const q = new CallQueue(10);
    q.push(call('x1'));
    q.push(call('x2'));
    expect(q.peek()?.uniqueId).toBe('x1');
    expect(q.peek()?.uniqueId).toBe('x1'); // a lost CALLRESULT: the next attempt is the same CALL
    q.ack('x1');
    expect(q.peek()?.uniqueId).toBe('x2');
  });

  it('over capacity drops the oldest MeterValues, never the in-flight head or Start/Stop while a reading exists', () => {
    const q = new CallQueue(4);
    q.push(call('start', 'StartTransaction'));
    for (const id of ['m1', 'm2', 'm3']) q.push(call(id));
    expect(q.push(call('stop', 'StopTransaction'))).toBe(1);
    expect(q.ids()).toEqual(['start', 'm2', 'm3', 'stop']);
    expect(q.dropped).toBe(1);
  });
});

const max = (attempt: number) => Math.max(...Array.from({ length: 4000 }, () => reconnectDelayMs(attempt, mulberry32(attempt * 31 + Math.random() * 1e6))));

describe('reconnectDelayMs (ADR 0003)', () => {
  it('first attempt is uniform in 0..10 s', () => {
    const rng = mulberry32(7);
    const xs = Array.from({ length: 5000 }, () => reconnectDelayMs(0, rng));
    expect(Math.min(...xs)).toBeLessThan(200);
    expect(Math.max(...xs)).toBeLessThanOrEqual(10_000);
    expect(Math.max(...xs)).toBeGreaterThan(9_800);
    const mean = xs.reduce((a, b) => a + b, 0) / xs.length;
    expect(mean).toBeGreaterThan(4_700);
    expect(mean).toBeLessThan(5_300);
  });

  it('later attempts use full-jitter exponential backoff capped at 60 s', () => {
    expect(max(1)).toBeLessThanOrEqual(10_000);
    expect(max(2)).toBeLessThanOrEqual(20_000);
    expect(max(2)).toBeGreaterThan(10_000);
    expect(max(3)).toBeLessThanOrEqual(40_000);
    expect(max(4)).toBeLessThanOrEqual(BACKOFF_CAP_MS);
    expect(max(12)).toBeLessThanOrEqual(BACKOFF_CAP_MS);
    expect(max(12)).toBeGreaterThan(50_000);
  });

  it('a server Retry-After is a floor', () => {
    expect(reconnectDelayMs(0, () => 0, 3_000)).toBe(3_000);
    expect(reconnectDelayMs(0, () => 0.999, 3_000)).toBeGreaterThan(9_000);
  });
});

const mk = () => {
  let now = 1_000_000;
  const w = new TimerWheel(100, 64, () => now);
  return { w, advance: (ms: number) => w.advance((now += ms)), at: () => now };
};

describe('TimerWheel', () => {

  it('fires timers once, at the first tick at or after their due time, in due order', () => {
    const { w, advance } = mk();
    const fired: string[] = [];
    w.schedule(250, () => fired.push('b'));
    w.schedule(120, () => fired.push('a'));
    w.schedule(0, () => fired.push('now'));
    advance(50);
    expect(fired).toEqual([]);
    advance(100);
    expect([...fired].sort()).toEqual(['a', 'now']); // same tick: insertion order
    advance(500);
    expect(fired.at(-1)).toBe('b');
    expect(fired).toHaveLength(3);
    advance(10_000);
    expect(fired).toHaveLength(3);
  });

  it('never fires early, also for delays longer than one revolution (64 slots x 100 ms)', () => {
    const { w, advance } = mk();
    let fired = 0;
    w.schedule(20_000, () => fired++);
    advance(6_400);
    advance(6_400);
    expect(fired).toBe(0);
    advance(7_300);
    expect(fired).toBe(1);
  });

  it('cancelled timers do not fire and are swept', () => {
    const { w, advance } = mk();
    let fired = 0;
    const t = w.schedule(300, () => fired++);
    t.cancel();
    advance(1_000);
    expect(fired).toBe(0);
    expect(w.pending).toBe(0);
  });

  it('a callback may schedule more timers, and an error in one does not stop the others', () => {
    let now = 0;
    const errors: unknown[] = [];
    const w = new TimerWheel(100, 64, () => now, (e) => errors.push(e));
    const log: string[] = [];
    w.schedule(100, () => {
      log.push('first');
      w.schedule(100, () => log.push('chained'));
    });
    w.schedule(100, () => {
      throw new Error('boom');
    });
    w.schedule(100, () => log.push('third'));
    w.advance((now += 150));
    w.advance((now += 150));
    expect(log).toEqual(['first', 'third', 'chained']);
    expect(errors).toHaveLength(1);
  });

  it('handles 100,000 timers cheaply', () => {
    const { w, advance } = mk();
    let fired = 0;
    for (let i = 0; i < 100_000; i++) w.schedule(i % 5_000, () => fired++);
    const t0 = performance.now();
    advance(6_000);
    expect(fired).toBe(100_000);
    expect(performance.now() - t0).toBeLessThan(1_000);
  });
});
