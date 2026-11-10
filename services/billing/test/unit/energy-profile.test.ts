import { describe, expect, it } from 'vitest';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { fetchEnergyProfile, profileFromBuckets } from '../../src/tariff/energy-profile';

const t0 = new Date('2026-11-10T10:00:00.000Z');
describe('profileFromBuckets', () => {
  it('turns 3 buckets into 4 cumulative samples, first at the session start with 0 Wh', () => {
    const out = profileFromBuckets(t0, [
      { bucket: '2026-11-10T10:01:00.000Z', energyWhDelta: 100 },
      { bucket: '2026-11-10T10:02:00.000Z', energyWhDelta: 250 },
      { bucket: '2026-11-10T10:03:00.000Z', energyWhDelta: 0 },
    ]);
    expect(out).toEqual([
      { at: '2026-11-10T10:00:00.000Z', energyWh: 0 },
      { at: '2026-11-10T10:01:00.000Z', energyWh: 100 },
      { at: '2026-11-10T10:02:00.000Z', energyWh: 350 },
      { at: '2026-11-10T10:03:00.000Z', energyWh: 350 },
    ]);
  });
  it('drops buckets that are not after the previous sample (the tariff needs strictly increasing times)', () => {
    const out = profileFromBuckets(t0, [
      { bucket: '2026-11-10T09:59:00.000Z', energyWhDelta: 5 },
      { bucket: '2026-11-10T10:01:00.000Z', energyWhDelta: 100 },
      { bucket: '2026-11-10T10:01:00.000Z', energyWhDelta: 7 },
    ]);
    expect(out.map((s) => s.energyWh)).toEqual([0, 100]);
  });
});

const serve = async (handler: Parameters<typeof createServer>[1]) => {
  const srv = createServer(handler);
  await new Promise<void>((r) => srv.listen(0, '127.0.0.1', r));
  return { url: `http://127.0.0.1:${(srv.address() as AddressInfo).port}`, close: () => srv.close() };
};

describe('fetchEnergyProfile', () => {
  const t1 = new Date('2026-11-10T10:00:00.000Z');
  const t2 = new Date('2026-11-10T10:03:00.000Z');
  it('builds the cumulative profile from avg W per minute and sends the internal token', async () => {
    let seen = '';
    const s = await serve((req, res) => {
      seen = `${req.headers['x-internal-token']} ${req.url}`;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ points: [{ t: '2026-11-10T10:00:00Z', avgW: 6000 }, { t: '2026-11-10T10:01:00Z', avgW: 6000 }, { t: '2026-11-10T10:02:00Z', avgW: 0 }] }));
    });
    try {
      const p = await fetchEnergyProfile(s.url, 'tok', 'st-1', 2, t1, t2);
      expect(p?.map((x) => x.energyWh)).toEqual([0, 100, 200, 200]);
      expect(seen).toContain('tok /v1/stations/st-1/connectors/2/power?');
      expect(seen).toContain('step=1m');
    } finally {
      s.close();
    }
  });
  it('gives undefined on an error status and on a refused connection', async () => {
    const s = await serve((_req, res) => void res.writeHead(500).end());
    try {
      expect(await fetchEnergyProfile(s.url, 'tok', 'st-1', 1, t1, t2)).toBeUndefined();
    } finally {
      s.close();
    }
    expect(await fetchEnergyProfile('http://127.0.0.1:1', 'tok', 'st-1', 1, t1, t2)).toBeUndefined();
  });
});
