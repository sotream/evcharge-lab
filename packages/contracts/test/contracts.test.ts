import { describe, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { events, ocpp, Topics, TOPIC_SPECS } from '../src';

describe('ocpp frames', () => {
  it('parses CALL/RESULT/ERROR', () => {
    expect(ocpp.Frame.parse([2, 'a1', 'Heartbeat', {}])[0]).toBe(2);
    expect(ocpp.Frame.parse([3, 'a1', { currentTime: '2026-11-08T00:00:00Z' }])[0]).toBe(3);
    expect(ocpp.Frame.parse([4, 'a1', 'NotImplemented', 'x', {}])[0]).toBe(4);
    expect(() => ocpp.Frame.parse([9, 'a1'])).toThrow(/Invalid/);
  });
  it('validates StartTransaction', () => {
    const r = ocpp.StationToCsms.StartTransaction.req.safeParse({ connectorId: 0, idTag: 't', meterStart: 0, timestamp: '2026-11-08T00:00:00Z' });
    expect(r.success).toBe(false);
  });
});

describe('events', () => {
  it('round-trips SessionCompleted in the union', () => {
    const e = {
      eventId: randomUUID(), type: 'session.completed', version: 1, occurredAt: new Date().toISOString(), producer: 'sessions', key: 'site-1',
      data: { sessionId: randomUUID(), transactionId: 1, driverId: null, siteId: 'site-1', stationId: 'st-1', connectorId: 1, tariffId: 'std',
        meterStartWh: 0, meterStopWh: 1000, energyWh: 1000, startedAt: new Date().toISOString(), stoppedAt: new Date().toISOString(), stopReason: 'Local' },
    };
    expect(events.SessionEvent.parse(e).type).toBe('session.completed');
  });
  it('every topic has a spec', () => {
    expect(new Set(TOPIC_SPECS.map((t) => t.name))).toEqual(new Set(Object.values(Topics)));
  });
});
