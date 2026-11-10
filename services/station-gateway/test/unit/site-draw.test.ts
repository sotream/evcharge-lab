import { describe, expect, it } from 'vitest';
import type { Reading } from '../../src/ocpp/meter';
import { SiteDrawTracker } from '../../src/site-draw';

const reading = (w: number, unit = 'W', at = '2026-11-10T10:00:00Z'): Reading[] => [{ timestamp: at, samples: [{ measurand: 'Power.Active.Import', value: w, unit }] }];
function setup() {
  const drawn = new Map<string, number>();
  const caps = new Map<string, number>();
  const t = new SiteDrawTracker(
    { siteDrawn: { set: (l: { site_id: string }, v: number) => void drawn.set(l.site_id, v) }, siteCap: { set: (l: { site_id: string }, v: number) => void caps.set(l.site_id, v) } } as never,
    'gw-1',
  );
  return { t, drawn, caps };
}

const phased = (ws: number[], total?: number): Reading[] => [{ timestamp: '2026-11-10T10:00:00Z', samples: [...ws.map((value, i) => ({ measurand: 'Power.Active.Import', value, unit: 'W', phase: `L${i + 1}` })), ...(total === undefined ? [] : [{ measurand: 'Power.Active.Import', value: total, unit: 'W' }])] }];

describe('SiteDrawTracker', () => {
  it('sums live readings per site, drops a disconnected station, ignores replayed readings', () => {
    const { t, drawn } = setup();
    const a = {};
    const b = {};
    t.meterValues(a, 'site-1', 1, reading(4000), false);
    t.meterValues(b, 'site-1', 1, reading(3000), false);
    expect(drawn.get('site-1')).toBe(7000);
    t.meterValues(a, 'site-1', 1, reading(9999), true);
    expect(drawn.get('site-1')).toBe(7000);
    t.clear(a, 'site-1');
    expect(drawn.get('site-1')).toBe(3000);
    t.clear(b, 'site-1');
    expect(drawn.get('site-1')).toBe(0);
  });
  it('keeps the latest value per connector, converts kW, ignores non-power samples', () => {
    const { t, drawn } = setup();
    const a = {};
    t.meterValues(a, 'site-1', 1, reading(4000), false);
    t.meterValues(a, 'site-1', 1, reading(2.5, 'kW'), false);
    expect(drawn.get('site-1')).toBe(2500);
    t.meterValues(a, 'site-1', 2, [{ timestamp: '2026-11-10T10:00:00Z', samples: [{ measurand: 'Power.Active.Import', value: 900, unit: 'W', phase: 'L1' }, { measurand: 'Voltage', value: 230, unit: 'V' }] }], false);
    expect(drawn.get('site-1')).toBe(3400); // 2500 + the phase L1 value of connector 2; Voltage is ignored
  });

  it('drops a connector on StopTransaction and on a non-Charging status, and ages out silent readings', () => {
    let now = 1_000_000;
    const { drawn, caps: _c } = setup();
    const m = { siteDrawn: { set: (l: { site_id: string }, v: number) => void drawn.set(l.site_id, v) }, siteCap: { set: () => {} } } as never;
    const t = new SiteDrawTracker(m, 'gw-1', () => now);
    const a = {};
    t.meterValues(a, 'site-1', 1, reading(11000), false, 77);
    t.meterValues(a, 'site-1', 2, reading(7000), false, 78);
    expect(drawn.get('site-1')).toBe(18000);
    t.clearTransaction(a, 'site-1', 77);
    expect(drawn.get('site-1')).toBe(7000);
    t.clearConnector(a, 'site-1', 2);
    expect(drawn.get('site-1')).toBe(0);
    t.meterValues(a, 'site-1', 1, reading(5000), false);
    now += 61_000;
    t.expire();
    expect(drawn.get('site-1')).toBe(0);
  });

  it('sums L1-L3 when only per-phase power is reported, and prefers a reported total', () => {
    const { t, drawn } = setup();
    const a = {};
    t.meterValues(a, 'site-1', 1, phased([3000, 3100, 2900]), false);
    expect(drawn.get('site-1')).toBe(9000);
    t.meterValues(a, 'site-1', 1, phased([3000, 3100, 2900], 8800), false);
    expect(drawn.get('site-1')).toBe(8800);
  });
});
