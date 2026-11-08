/* oxlint-disable no-await-in-loop */
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { rejectUnconfirmedReservations, sweepExpiredReservations } from '../../src/workers/maintenance';
import { type Harness, startHarness, waitFor } from './harness';

let h: Harness;
beforeAll(async () => {
  h = await startHarness({ sites: 1, stationsPerSite: 3 });
});
afterAll(async () => {
  await h?.close();
});

const rowOf = async (id: number) => (await h.db.pool.query<{ status: string }>('SELECT status FROM reservations WHERE reservation_id = $1', [id])).rows[0]?.status;

describe('reservations (ADR 0005)', () => {
  it('zero double booking: 50 concurrent attempts on one connector -> exactly one 201 and 49 x 409', async () => {
    const drivers = await Promise.all(Array.from({ length: 50 }, () => h.registerDriver()));
    const results = await Promise.all(
      drivers.map((d) => h.driver(d.driverId)('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-001', connectorId: 1 } })),
    );
    const ok = results.filter((r) => r.status === 201);
    const conflict = results.filter((r) => r.status === 409);
    const seen = JSON.stringify(Object.entries(Object.groupBy(results, (r) => `${r.status} ${r.body?.code ?? ''}`)).map(([k, v]) => [k, v?.length]));
    // The status distribution is part of the compared object, so a failure shows what the other requests got.
    expect({ ok: ok.length, conflict: conflict.length, seen }).toMatchObject({ ok: 1, conflict: 49 });
    for (const c of conflict) expect(c.body).toMatchObject({ status: 409, code: 'connector_unavailable' });
    const { rows } = await h.db.pool.query<{ n: string }>("SELECT count(*) AS n FROM reservations WHERE station_id = 'st-0001-001' AND connector_id = 1 AND status IN ('Pending','Active')");
    expect(Number(rows[0]?.n)).toBe(1);
    // the single winner has exactly one ReserveNow command + one reservation.created event
    const winnerId = ok[0]?.body.reservationId as number;
    await waitFor(() => h.sessionEvents.some((e) => e.type === 'reservation.created' && e.data.reservationId === winnerId));
    expect(h.sessionEvents.filter((e) => e.type === 'reservation.created' && e.data.connectorId === 1 && e.data.stationId === 'st-0001-001')).toHaveLength(1);
    const reserveNow = await waitFor(() => h.commands.filter((c) => c.data.action === 'ReserveNow' && c.data.payload['reservationId'] === winnerId));
    expect(reserveNow).toHaveLength(1);
    expect(Date.parse(reserveNow[0]?.data.expiresAt ?? '') - Date.parse(reserveNow[0]?.occurredAt ?? '')).toBeLessThanOrEqual(31_000);
  });

  it('reservation lasts 15 minutes and the retry with the same idempotency-key returns the original', async () => {
    const d = await h.registerDriver();
    const call = h.driver(d.driverId);
    const key = randomUUID();
    const first = await call('POST', '/v1/reservations', { key, body: { stationId: 'st-0001-002', connectorId: 1 } });
    expect(first.status).toBe(201);
    expect(first.body.status).toBe('Pending');
    expect(Date.parse(first.body.expiresAt) - Date.parse(first.body.startsAt)).toBe(15 * 60_000);
    const again = await call('POST', '/v1/reservations', { key, body: { stationId: 'st-0001-002', connectorId: 1 } });
    expect(again.status).toBe(201);
    expect(again.body).toEqual(first.body);
    expect(again.headers.get('idempotent-replayed')).toBe('true');
    // same key, different request: 422
    const mismatch = await call('POST', '/v1/reservations', { key, body: { stationId: 'st-0001-002', connectorId: 2 } });
    expect(mismatch.status).toBe(422);
    const { rows } = await h.db.pool.query<{ n: string }>('SELECT count(*) AS n FROM reservations WHERE driver_id = $1', [d.driverId]);
    expect(Number(rows[0]?.n)).toBe(1);
  });

  it('requires an idempotency-key, and a driver may hold one open reservation only', async () => {
    const d = await h.registerDriver();
    const call = h.driver(d.driverId);
    const noKey = await call('POST', '/v1/reservations', { body: { stationId: 'st-0001-003', connectorId: 1 } });
    expect(noKey.status).toBe(400);
    expect(noKey.body.code).toBe('idempotency_key_required');
    expect((await call('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-003', connectorId: 1 } })).status).toBe(201);
    const second = await call('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-003', connectorId: 2 } });
    expect(second.status).toBe(409);
    expect(second.body.code).toBe('reservation_exists');
  });

  it('Accepted -> Active; a rejected ReserveNow (StationOffline) releases the connector; cancel sends CancelReservation', async () => {
    const a = await h.registerDriver();
    const b = await h.registerDriver();
    const r1 = await h.driver(a.driverId)('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-003', connectorId: 2 } });
    expect(r1.status).toBe(201);
    const cmd1 = await waitFor(() => h.commands.find((c) => c.data.action === 'ReserveNow' && c.data.payload['reservationId'] === r1.body.reservationId));
    await h.publishCommandResult(cmd1.data.commandId, 'st-0001-003', 'ReserveNow', 'Accepted');
    await waitFor(async () => (await rowOf(r1.body.reservationId)) === 'Active');
    // a result requested by someone else is ignored
    const ignored = await h.registerDriver();
    const r2 = await h.driver(ignored.driverId)('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-002', connectorId: 2 } });
    const cmd2 = await waitFor(() => h.commands.find((c) => c.data.action === 'ReserveNow' && c.data.payload['reservationId'] === r2.body.reservationId));
    await h.publishCommandResult(cmd2.data.commandId, 'st-0001-002', 'ReserveNow', 'StationOffline', 'someone-else');
    await h.publishCommandResult(cmd2.data.commandId, 'st-0001-002', 'ReserveNow', 'StationOffline');
    await waitFor(async () => (await rowOf(r2.body.reservationId)) === 'Rejected');
    await waitFor(() => h.sessionEvents.some((e) => e.type === 'reservation.ended' && e.data.reservationId === r2.body.reservationId && e.data.reason === 'Rejected'));
    // the released connector can be reserved again
    const r3 = await h.driver(b.driverId)('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-002', connectorId: 2 } });
    expect(r3.status).toBe(201);
    // cancel
    const del = await h.driver(b.driverId)('DELETE', `/v1/reservations/${r3.body.reservationId}`);
    expect(del.status).toBe(200);
    expect(del.body.status).toBe('Cancelled');
    await waitFor(() => h.commands.find((c) => c.data.action === 'CancelReservation' && c.data.payload['reservationId'] === r3.body.reservationId));
    expect((await h.driver(b.driverId)('DELETE', `/v1/reservations/${r3.body.reservationId}`)).body.status).toBe('Cancelled');
    // another driver cannot see or cancel it
    expect((await h.driver(a.driverId)('GET', `/v1/reservations/${r3.body.reservationId}`)).status).toBe(404);
  });

  it('sweeper expires overdue reservations (SKIP LOCKED, safe to run concurrently) and emits reservation.ended', async () => {
    const d = await h.registerDriver();
    const r = await h.driver(d.driverId)('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-001', connectorId: 2 } });
    expect(r.status).toBe(201);
    await h.db.pool.query("UPDATE reservations SET starts_at = now() - interval '10 minutes', expires_at = now() - interval '1 second' WHERE reservation_id = $1", [r.body.reservationId]);
    const counts = await Promise.all([sweepExpiredReservations(h.db.pool, h.topics), sweepExpiredReservations(h.db.pool, h.topics)]);
    await waitFor(async () => (await rowOf(r.body.reservationId)) === 'Expired');
    expect(counts[0]! + counts[1]!).toBeLessThanOrEqual(1);
    await waitFor(() => h.sessionEvents.filter((e) => e.type === 'reservation.ended' && e.data.reservationId === r.body.reservationId && e.data.reason === 'Expired').length === 1);
  });

  it('two drivers never share a stored response for the same idempotency-key', async () => {
    const [d1, d2] = [await h.registerDriver(), await h.registerDriver()];
    await h.db.pool.query("INSERT INTO connectors (station_id, connector_id, site_id, current_type, phases, max_power_w, min_power_w, tariff_id) SELECT 'st-0001-001', g, 'site-0001', 'AC', 3, 11000, 4140, 'std' FROM generate_series(51, 52) g");
    const key = randomUUID();
    const r1 = await h.driver(d1.driverId)('POST', '/v1/reservations', { key, body: { stationId: 'st-0001-001', connectorId: 51 } });
    const r2 = await h.driver(d2.driverId)('POST', '/v1/reservations', { key, body: { stationId: 'st-0001-001', connectorId: 52 } });
    expect(r1.status).toBe(201);
    expect(r2.status).toBe(201);
    expect(r1.body.driverId).toBe(d1.driverId);
    expect(r2.body.driverId).toBe(d2.driverId);
    expect(r2.body.reservationId).not.toBe(r1.body.reservationId);
    expect(r2.headers.get('idempotent-replayed')).toBeNull();
    // same for remote-start
    await h.db.pool.query("UPDATE stations SET online = true WHERE station_id = 'st-0001-001'");
    const k2 = randomUUID();
    const s1 = await h.driver(d1.driverId)('POST', '/v1/sessions/remote-start', { key: k2, body: { stationId: 'st-0001-001', connectorId: 51 } });
    const s2 = await h.driver(d2.driverId)('POST', '/v1/sessions/remote-start', { key: k2, body: { stationId: 'st-0001-001', connectorId: 52 } });
    await h.db.pool.query('DELETE FROM connectors WHERE connector_id IN (51, 52)');
    expect([s1.status, s2.status]).toEqual([202, 202]);
    expect(s1.body.commandId).not.toBe(s2.body.commandId);
    await h.db.pool.query("UPDATE reservations SET status = 'Cancelled' WHERE driver_id IN ($1, $2)", [d1.driverId, d2.driverId]);
  });

  it('a Pending reservation whose ReserveNow got no result is rejected shortly after the command expired, and a late result is ignored', async () => {
    const d = await h.registerDriver();
    const r = await h.driver(d.driverId)('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-001', connectorId: 2 } });
    expect(r.status).toBe(201);
    const cmd = await waitFor(() => h.commands.find((c) => c.data.action === 'ReserveNow' && c.data.payload['reservationId'] === r.body.reservationId));
    expect(await rejectUnconfirmedReservations(h.db.pool, h.topics)).toBe(0); // command still live
    await h.db.pool.query("UPDATE commands SET expires_at = now() - interval '5 seconds' WHERE command_id = $1", [cmd.data.commandId]);
    expect(await rejectUnconfirmedReservations(h.db.pool, h.topics)).toBe(0); // inside the 10 s grace
    await h.db.pool.query("UPDATE commands SET expires_at = now() - interval '11 seconds' WHERE command_id = $1", [cmd.data.commandId]);
    const counts = await Promise.all([rejectUnconfirmedReservations(h.db.pool, h.topics), rejectUnconfirmedReservations(h.db.pool, h.topics)]);
    expect(counts[0]! + counts[1]!).toBe(1);
    expect(await rowOf(r.body.reservationId)).toBe('Rejected');
    await waitFor(() => h.sessionEvents.filter((e) => e.type === 'reservation.ended' && e.data.reservationId === r.body.reservationId && e.data.reason === 'Rejected').length === 1);
    await h.publishCommandResult(cmd.data.commandId, 'st-0001-001', 'ReserveNow', 'Accepted');
    await new Promise((res) => setTimeout(res, 800));
    expect(await rowOf(r.body.reservationId)).toBe('Rejected');
  });

  it('a busy connector is rejected up front with 409', async () => {
    await h.publishStationEvent('station.connector-status-changed', 'st-0001-002', { connectorId: 1, status: 'Charging', errorCode: 'NoError' });
    await waitFor(async () => (await h.db.pool.query("SELECT 1 FROM connectors WHERE station_id = 'st-0001-002' AND connector_id = 1 AND status = 'Charging'")).rowCount === 1);
    const d = await h.registerDriver();
    const r = await h.driver(d.driverId)('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-002', connectorId: 1 } });
    expect(r.status).toBe(409);
    expect(r.body.code).toBe('connector_unavailable');
  });
});

describe('driver identity (ADR 0010)', () => {
  it('rejects x-driver-id without a valid internal token (header spoofing) on every driver route', async () => {
    const victim = randomUUID();
    const routes: [string, string, unknown?][] = [
      ['POST', '/v1/reservations', { stationId: 'st-0001-001', connectorId: 1 }],
      ['GET', `/v1/reservations/1`],
      ['DELETE', `/v1/reservations/1`],
      ['POST', '/v1/sessions/remote-start', { stationId: 'st-0001-001', connectorId: 1 }],
      ['GET', `/v1/sessions/${randomUUID()}`],
      ['POST', `/v1/sessions/${randomUUID()}/stop`],
      ['GET', `/v1/drivers/${victim}/sessions`],
      ['GET', '/v1/sites?lat=52.5&lng=13.4'],
    ];
    for (const [method, path, body] of routes) {
      const headers = { 'idempotency-key': randomUUID() };
      const noToken = await h.http(method, path, { token: null, driverId: victim, body, headers });
      expect(noToken.status, `${method} ${path} without token`).toBe(401);
      const badToken = await h.http(method, path, { token: 'wrong-token-wrong-token', driverId: victim, body, headers });
      expect(badToken.status, `${method} ${path} with bad token`).toBe(401);
    }
    const before = await h.db.pool.query('SELECT 1 FROM reservations WHERE driver_id = $1', [victim]);
    expect(before.rowCount).toBe(0);
  });

  it('with a valid token the driver id must be present and well-formed, and cannot read another driver', async () => {
    const noDriver = await h.http('GET', `/v1/sessions/${randomUUID()}`);
    expect(noDriver.status).toBe(401);
    const bad = await h.http('GET', `/v1/sessions/${randomUUID()}`, { driverId: 'not-a-uuid' });
    expect(bad.status).toBe(400);
    const me = randomUUID();
    const other = randomUUID();
    const mismatch = await h.driver(me)('GET', `/v1/drivers/${other}/sessions`);
    expect(mismatch.status).toBe(403);
    expect((await h.driver(me)('GET', `/v1/drivers/${me}/sessions`)).body).toEqual({ sessions: [] });
  });

  it('internal OCPP routes need the internal token too', async () => {
    expect((await h.http('GET', '/internal/stations/st-0001-001/credential', { token: null })).status).toBe(401);
    expect((await h.http('POST', '/internal/ocpp/authorize', { token: null, body: { stationId: 'st-0001-001', idTag: 'x' } })).status).toBe(401);
  });
});

describe('catalog', () => {
  it('lists sites around a point with stations, connector status and online flag', async () => {
    const d = randomUUID();
    const r = await h.driver(d)('GET', '/v1/sites?lat=52.52&lng=13.405&radiusKm=50');
    expect(r.status).toBe(200);
    expect(r.body.sites).toHaveLength(1);
    const site = r.body.sites[0];
    expect(site.stations).toHaveLength(3);
    expect(site.stations[0].connectors).toHaveLength(2);
    expect(typeof site.stations[0].online).toBe('boolean');
    const far = await h.driver(d)('GET', '/v1/sites?lat=48.1&lng=11.5&radiusKm=5');
    expect(far.body.sites).toEqual([]);
    expect((await h.driver(d)('GET', '/v1/sites?lat=abc&lng=1')).status).toBe(400);
  });
});
