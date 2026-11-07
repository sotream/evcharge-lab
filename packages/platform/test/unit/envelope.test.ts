import { events } from '@evcharge/contracts';
import { describe, expect, it } from 'vitest';
import { hashRequest } from '../../src/idempotency/idempotency';
import { makeEnvelope } from '../../src/kafka/envelope';

describe('makeEnvelope', () => {
  it('produces a contract-valid envelope with uuid v4 and ISO time', () => {
    const e = makeEnvelope('station.connected', 'CP-1', { stationId: 'CP-1', gatewayInstance: 'gw-1' }, 'station-gateway');
    expect(events.StationConnected.parse(e)).toEqual(e);
    expect(e.eventId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(new Date(e.occurredAt).toISOString()).toBe(e.occurredAt);
    expect(e.version).toBe(1);
    expect(makeEnvelope('t', 'k', {}, 'p').eventId).not.toBe(e.eventId);
  });

  it('accepts explicit eventId/occurredAt', () => {
    const at = new Date('2026-01-01T00:00:00Z');
    const e = makeEnvelope('t', 'k', 1, 'p', { eventId: 'id', occurredAt: at });
    expect(e).toMatchObject({ eventId: 'id', occurredAt: '2026-01-01T00:00:00.000Z' });
  });
});

describe('hashRequest', () => {
  it('ignores key order but not values, method or path', () => {
    const h = hashRequest('post', '/a', { x: 1, y: { b: 2, a: [1, { d: 1, c: 2 }] } });
    expect(hashRequest('POST', '/a', { y: { a: [1, { c: 2, d: 1 }], b: 2 }, x: 1 })).toBe(h);
    expect(hashRequest('POST', '/a', { x: 2, y: { b: 2, a: [1, { d: 1, c: 2 }] } })).not.toBe(h);
    expect(hashRequest('PUT', '/a', { x: 1, y: { b: 2, a: [1, { d: 1, c: 2 }] } })).not.toBe(h);
    expect(hashRequest('POST', '/b', { x: 1, y: { b: 2, a: [1, { d: 1, c: 2 }] } })).not.toBe(h);
    expect(hashRequest('POST', '/a', undefined)).toBe(hashRequest('POST', '/a', null));
  });
});
