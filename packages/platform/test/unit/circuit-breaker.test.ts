import { describe, expect, it } from 'vitest';
import { CircuitBreaker } from '../../src/http-client/circuit-breaker';
import { fullJitterBackoff } from '../../src/util';

describe('CircuitBreaker', () => {
  it('an abandoned half-open trial frees the slot but does not close the circuit', () => {
    let now = 0;
    const cb = new CircuitBreaker({ failureThreshold: 1, openMs: 1000, now: () => now });
    cb.onFailure();
    now = 1000;
    expect(cb.tryAcquire()).toBe(true);
    cb.onAbandon();
    expect(cb.state).toBe('half-open');
    expect(cb.tryAcquire()).toBe(true); // a new trial may run
  });

  it('opens after N consecutive failures, half-opens after openMs, closes on success', () => {
    let now = 0;
    const cb = new CircuitBreaker({ failureThreshold: 3, openMs: 1000, now: () => now });
    cb.onFailure();
    cb.onFailure();
    cb.onSuccess(); // resets the streak
    cb.onFailure();
    cb.onFailure();
    expect(cb.state).toBe('closed');
    cb.onFailure();
    expect(cb.state).toBe('open');
    expect(cb.isOpen).toBe(true);
    expect(cb.tryAcquire()).toBe(false);
    now = 999;
    expect(cb.tryAcquire()).toBe(false);
    now = 1000;
    expect(cb.state).toBe('half-open');
    expect(cb.tryAcquire()).toBe(true);
    expect(cb.tryAcquire()).toBe(false); // only one trial
    cb.onSuccess();
    expect(cb.state).toBe('closed');
    expect(cb.consecutiveFailures).toBe(0);
  });

  it('a failed trial re-opens immediately', () => {
    let now = 0;
    const cb = new CircuitBreaker({ failureThreshold: 1, openMs: 100, now: () => now });
    cb.onFailure();
    now = 100;
    expect(cb.tryAcquire()).toBe(true);
    cb.onFailure();
    expect(cb.state).toBe('open');
    now = 150;
    expect(cb.isOpen).toBe(true);
  });
});

describe('fullJitterBackoff', () => {
  it('is within [0, min(max, base*2^n))', () => {
    expect(fullJitterBackoff(0, 100, 1000, () => 0.999)).toBe(99);
    expect(fullJitterBackoff(3, 100, 1000, () => 0.5)).toBe(400);
    expect(fullJitterBackoff(10, 100, 1000, () => 0.5)).toBe(500);
    expect(fullJitterBackoff(5, 100, 1000, () => 0)).toBe(0);
  });
});
