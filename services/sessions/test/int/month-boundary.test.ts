/* oxlint-disable no-await-in-loop */
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { type Harness, startHarness } from './harness';

let h: Harness;
let connectors: number[] = [];
let boundary: Date;

beforeAll(async () => {
  h = await startHarness({ sites: 1, stationsPerSite: 2, workers: false });
  // first instant of next month (UTC): partitions for it exist from startup
  const n = new Date();
  boundary = new Date(Date.UTC(n.getUTCFullYear(), n.getUTCMonth() + 1, 1));
  const { rows } = await h.db.pool.query<{ connector_pk: string }>(
    `INSERT INTO connectors (station_id, connector_id, site_id, current_type, phases, max_power_w, min_power_w, tariff_id)
     SELECT 'st-0001-001', 100 + g, 'site-0001', 'AC', 3, 11000, 4140, 'std' FROM generate_series(1, 60) g RETURNING connector_pk`,
  );
  connectors = rows.map((r) => Number(r.connector_pk));
});
afterAll(async () => {
  await h?.close();
});

const name = (d: Date) => `reservations_${d.getUTCFullYear()}_${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
const at = (minutesFromBoundary: number) => new Date(boundary.getTime() + minutesFromBoundary * 60_000);
const insert = (driverId: string, connectorPk: number, startsAt: Date) =>
  h.db.pool.query(
    `INSERT INTO reservations (reservation_id, driver_id, id_tag, site_id, station_id, connector_id, connector_pk, status, starts_at, expires_at)
     VALUES (nextval('reservation_id_seq'), $1, 'tag', 'site-0001', 'st-0001-001', 1, $2, 'Pending', $3::timestamptz, $3::timestamptz + interval '15 minutes')`,
    [driverId, connectorPk, startsAt],
  );
const code = (r: PromiseSettledResult<unknown>) => (r.status === 'rejected' ? (r.reason as { code?: string }).code : 'ok');

describe('reservations across a month boundary (ADR 0005 constraint trigger)', () => {
  it('the two ranges live in different monthly partitions, both present', async () => {
    expect(name(at(-5))).not.toBe(name(at(1)));
    for (const d of [at(-5), at(1)]) {
      const { rows } = await h.db.pool.query<{ r: string | null }>('SELECT to_regclass($1)::text AS r', [name(d)]);
      expect(rows[0]?.r).toBe(name(d));
    }
  });

  it('concurrent inserts at month end - 5 min and month start + 1 min on one connector: exactly one winner (30 rounds)', async () => {
    for (let i = 0; i < 30; i++) {
      const pk = connectors[i] as number;
      const results = await Promise.allSettled([insert(randomUUID(), pk, at(-5)), insert(randomUUID(), pk, at(1))]);
      const codes = results.map(code);
      expect(codes.filter((c) => c === 'ok'), `round ${i}: ${codes.join(',')}`).toHaveLength(1);
      expect(codes.filter((c) => c === '23P01')).toHaveLength(1);
    }
    const { rows } = await h.db.pool.query<{ n: string }>("SELECT count(*) AS n FROM reservations WHERE connector_pk = ANY($1::bigint[]) AND status IN ('Pending','Active')", [connectors.slice(0, 30)]);
    expect(Number(rows[0]?.n)).toBe(30);
  });

  it('non-overlapping ranges across the boundary both succeed', async () => {
    const pk = connectors[40] as number;
    await insert(randomUUID(), pk, at(-30));
    await insert(randomUUID(), pk, at(0)); // [-30, -15) and [0, 15)
    const { rows } = await h.db.pool.query<{ n: string }>('SELECT count(*) AS n FROM reservations WHERE connector_pk = $1', [pk]);
    expect(Number(rows[0]?.n)).toBe(2);
  });

  it('one open reservation per driver across the two partitions (concurrent, 20 rounds)', async () => {
    for (let i = 0; i < 20; i++) {
      const driver = randomUUID();
      const [x, y] = [connectors[30 + (i % 10)] as number, connectors[50 + (i % 10)] as number];
      // different connectors, overlapping time, same driver
      const results = await Promise.allSettled([insert(driver, x, at(-5)), insert(driver, y, at(1))]);
      const codes = results.map(code);
      expect(codes.filter((c) => c === 'ok'), `round ${i}: ${codes.join(',')}`).toHaveLength(1);
      expect(codes.filter((c) => c === '23505')).toHaveLength(1);
      const { rows } = await h.db.pool.query<{ n: string }>("SELECT count(*) AS n FROM reservations WHERE driver_id = $1 AND status IN ('Pending','Active')", [driver]);
      expect(Number(rows[0]?.n)).toBe(1);
      await h.db.pool.query('DELETE FROM reservations WHERE driver_id = $1', [driver]);
    }
  });
});
