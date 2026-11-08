import { fleet } from '@evcharge/contracts';
/* oxlint-disable no-await-in-loop */
import { createHash, createHmac, randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { type Harness, SEED, startHarness, waitFor } from './harness';

let h: Harness;
beforeAll(async () => {
  h = await startHarness({ sites: 1, stationsPerSite: 4 });
});
afterAll(async () => {
  await h?.close();
});

const count = async (sql: string, args: unknown[] = []) => Number((await h.db.pool.query<{ n: string }>(sql, args)).rows[0]?.n);
const types = (t: string, sessionId?: string) => h.sessionEvents.filter((e) => e.type === t && (!sessionId || (e.data as { sessionId?: string }).sessionId === sessionId));

describe('internal OCPP API (ADR 0003)', () => {
  it('serves the station credential with ADR 0006 chargingDefaults', async () => {
    const r = await h.http('GET', '/internal/stations/st-0001-002/credential');
    expect(r.status).toBe(200);
    const secret = createHmac('sha256', SEED).update('st-0001-002').digest('hex').slice(0, 32);
    expect(r.body.secretSha256).toBe(createHash('sha256').update(secret).digest('hex'));
    expect(r.body).toMatchObject({ stationId: 'st-0001-002', siteId: 'site-0001', enabled: true });
    const { rows } = await h.db.pool.query<{ cap_w: number }>("SELECT cap_w FROM sites WHERE site_id = 'site-0001'");
    const capW = rows[0]?.cap_w as number;
    expect(r.body.chargingDefaults).toEqual(fleet.chargingDefaults(capW, 8));
    expect(r.body.chargingDefaults.startW).toBeLessThanOrEqual(r.body.chargingDefaults.safeShareW);
    const sumMax = Number((await h.db.pool.query("SELECT sum(max_power_w) AS s FROM connectors WHERE site_id = 'site-0001'")).rows[0].s);
    expect(capW).toBeLessThan(sumMax);
    expect((await h.http('GET', '/internal/stations/nope/credential')).status).toBe(404);
  });

  it('authorize maps idTags to drivers', async () => {
    const d = await h.registerDriver('TAG-AUTH');
    expect((await h.http('POST', '/internal/ocpp/authorize', { body: { stationId: 'st-0001-001', idTag: d.idTag } })).body).toEqual({ idTagInfo: { status: 'Accepted' } });
    expect((await h.http('POST', '/internal/ocpp/authorize', { body: { stationId: 'st-0001-001', idTag: 'UNKNOWN' } })).body).toEqual({ idTagInfo: { status: 'Invalid' } });
    await h.db.pool.query('UPDATE drivers SET blocked = true WHERE driver_id = $1', [d.driverId]);
    expect((await h.http('POST', '/internal/ocpp/authorize', { body: { stationId: 'st-0001-001', idTag: d.idTag } })).body.idTagInfo.status).toBe('Blocked');
    expect((await h.http('POST', '/internal/ocpp/authorize', { body: { stationId: 'st-0001-001' } })).status).toBe(400);
  });

  it('driver registration is idempotent and conflicts are 409', async () => {
    const d = await h.registerDriver('TAG-REG');
    expect((await h.http('POST', '/internal/drivers', { body: d })).status).toBe(200);
    expect((await h.http('POST', '/internal/drivers', { body: { driverId: randomUUID(), idTag: 'TAG-REG' } })).status).toBe(409);
  });

  it('StartTransaction is idempotent on stationId:ocppMessageId, also under concurrent duplicates', async () => {
    const d = await h.registerDriver();
    const msgId = randomUUID();
    const results = await Promise.all(Array.from({ length: 20 }, () => h.startTx('st-0001-001', 1, d.idTag, msgId)));
    // a concurrent duplicate either waits for the first commit and replays it, or never sees an in-flight 409 for long
    const ok = results.filter((r) => r.status === 200);
    expect(ok.length).toBeGreaterThanOrEqual(1);
    const txIds = new Set(ok.map((r) => r.body.transactionId));
    expect(txIds.size).toBe(1);
    for (const r of results.filter((x) => x.status !== 200)) expect(r.status).toBe(409);
    const replay = await h.startTx('st-0001-001', 1, d.idTag, msgId);
    expect(replay.status).toBe(200);
    expect(replay.body.transactionId).toBe([...txIds][0]);
    expect(replay.body.sessionId).toBe(ok[0]?.body.sessionId);
    expect(await count("SELECT count(*) AS n FROM sessions WHERE station_id = 'st-0001-001' AND connector_id = 1")).toBe(1);
    expect(await count('SELECT count(*) AS n FROM active_sessions WHERE session_id = $1', [ok[0]?.body.sessionId])).toBe(1);
    await waitFor(() => types('session.started', ok[0]?.body.sessionId).length >= 1);
    expect(types('session.started', ok[0]?.body.sessionId)).toHaveLength(1);
  });

  it('start -> stop lifecycle emits session.started/session.completed once, StopTransaction replay returns the same answer', async () => {
    const d = await h.registerDriver();
    const start = await h.startTx('st-0001-002', 1, d.idTag);
    expect(start.status).toBe(200);
    expect(start.body.idTagInfo.status).toBe('Accepted');
    const { transactionId, sessionId } = start.body;
    const sess = await h.driver(d.driverId)('GET', `/v1/sessions/${sessionId}`);
    expect(sess.body).toMatchObject({ sessionId, transactionId, status: 'Active', stationId: 'st-0001-002', connectorId: 1, driverId: d.driverId, stoppedAt: null });
    const msgId = randomUUID();
    const stop = await h.stopTx('st-0001-002', transactionId, msgId, 8500);
    expect(stop.status).toBe(200);
    expect(stop.body.sessionId).toBe(sessionId);
    const replay = await h.stopTx('st-0001-002', transactionId, msgId, 8500);
    expect(replay.body).toEqual(stop.body);
    // a second StopTransaction with a new message id is accepted but does not complete the session again
    const again = await h.stopTx('st-0001-002', transactionId, randomUUID(), 9999);
    expect(again.status).toBe(200);
    expect(again.body.sessionId).toBe(sessionId);
    const done = await h.driver(d.driverId)('GET', `/v1/sessions/${sessionId}`);
    expect(done.body).toMatchObject({ status: 'Completed', energyWh: 7500 });
    expect(await count('SELECT count(*) AS n FROM active_sessions WHERE session_id = $1', [sessionId])).toBe(0);
    await waitFor(() => types('session.completed', sessionId).length >= 1);
    await new Promise((r) => setTimeout(r, 500));
    expect(types('session.completed', sessionId)).toHaveLength(1);
    expect(types('session.started', sessionId)).toHaveLength(1);
    const completed = types('session.completed', sessionId)[0]!;
    expect(completed.data).toMatchObject({ meterStartWh: 1000, meterStopWh: 8500, energyWh: 7500, siteId: 'site-0001' });
    const list = await h.driver(d.driverId)('GET', `/v1/drivers/${d.driverId}/sessions`);
    expect(list.body.sessions.map((s: { sessionId: string }) => s.sessionId)).toEqual([sessionId]);
  });

  it('stop for an unknown transaction is accepted (ADR 0003) and an unknown connector is rejected', async () => {
    const r = await h.stopTx('st-0001-003', 2_000_000_000);
    expect(r.status).toBe(200);
    expect(r.body.sessionId).toBeNull();
    const bad = await h.startTx('st-0001-003', 9, 'x');
    expect(bad.status).toBe(422);
    // the failed start left no dedup row behind: a retry with the same id is processed, not replayed
    const id = randomUUID();
    expect((await h.startTx('st-0001-003', 9, 'x', id)).status).toBe(422);
    expect(await count('SELECT count(*) AS n FROM ocpp_messages WHERE message_key = $1', [`st-0001-003:${id}`])).toBe(0);
  });

  it('a start on a connector with a leftover session completes the old one as Superseded', async () => {
    const d = await h.registerDriver();
    const first = await h.startTx('st-0001-003', 1, d.idTag);
    const second = await h.startTx('st-0001-003', 1, d.idTag);
    expect(second.body.transactionId).not.toBe(first.body.transactionId);
    const { rows } = await h.db.pool.query('SELECT status, stop_reason FROM sessions WHERE session_id = $1', [first.body.sessionId]);
    expect(rows[0]).toMatchObject({ status: 'Completed', stop_reason: 'Superseded' });
  });

  it('a reservation is marked Used by the reserving driver start, atomically with the session', async () => {
    const d = await h.registerDriver();
    const r = await h.driver(d.driverId)('POST', '/v1/reservations', { key: randomUUID(), body: { stationId: 'st-0001-004', connectorId: 1 } });
    expect(r.status).toBe(201);
    const start = await h.startTx('st-0001-004', 1, d.idTag);
    expect(start.status).toBe(200);
    const res = await h.driver(d.driverId)('GET', `/v1/reservations/${r.body.reservationId}`);
    expect(res.body.status).toBe('Used');
    await waitFor(() => h.sessionEvents.some((e) => e.type === 'reservation.ended' && e.data.reservationId === r.body.reservationId && e.data.reason === 'Used'));
    expect((await h.db.pool.query('SELECT reservation_id FROM sessions WHERE session_id = $1', [start.body.sessionId])).rows[0].reservation_id).toBe(r.body.reservationId);
  });
});

describe('driver commands', () => {
  it('remote-start needs an online station, is idempotent and goes out through the outbox', async () => {
    const d = await h.registerDriver();
    const call = h.driver(d.driverId);
    const key = randomUUID();
    const offline = await call('POST', '/v1/sessions/remote-start', { key, body: { stationId: 'st-0001-004', connectorId: 2 } });
    expect(offline.status).toBe(409);
    expect(offline.body.code).toBe('station_offline');
    await h.publishStationEvent('station.connected', 'st-0001-004', { gatewayInstance: 'gw-1' });
    await waitFor(async () => (await h.db.pool.query("SELECT 1 FROM stations WHERE station_id = 'st-0001-004' AND online")).rowCount === 1);
    const k2 = randomUUID();
    const ok = await call('POST', '/v1/sessions/remote-start', { key: k2, body: { stationId: 'st-0001-004', connectorId: 2 } });
    expect(ok.status).toBe(202);
    const replay = await call('POST', '/v1/sessions/remote-start', { key: k2, body: { stationId: 'st-0001-004', connectorId: 2 } });
    expect(replay.body).toEqual(ok.body);
    await waitFor(() => h.commands.some((c) => c.data.commandId === ok.body.commandId));
    const cmds = h.commands.filter((c) => c.data.commandId === ok.body.commandId);
    expect(cmds).toHaveLength(1);
    expect(cmds[0]?.data).toMatchObject({ action: 'RemoteStartTransaction', stationId: 'st-0001-004', requestedBy: 'sessions', payload: { connectorId: 2, idTag: d.idTag } });
    expect((await call('POST', '/v1/sessions/remote-start', { body: { stationId: 'st-0001-004', connectorId: 2 } })).status).toBe(400);
  });

  it('stop sends one RemoteStopTransaction however often it is called; a rejection puts the session back to Active', async () => {
    const d = await h.registerDriver();
    const start = await h.startTx('st-0001-004', 2, d.idTag);
    const call = h.driver(d.driverId);
    const a = await call('POST', `/v1/sessions/${start.body.sessionId}/stop`);
    expect(a.status).toBe(202);
    expect(a.body.status).toBe('Stopping');
    await call('POST', `/v1/sessions/${start.body.sessionId}/stop`);
    const isStop = (c: (typeof h.commands)[number]) => c.data.action === 'RemoteStopTransaction' && c.data.payload['transactionId'] === start.body.transactionId;
    await waitFor(() => h.commands.some(isStop));
    await new Promise((r) => setTimeout(r, 300));
    const stops = h.commands.filter(isStop);
    expect(stops).toHaveLength(1);
    await h.publishCommandResult(stops[0]!.data.commandId, 'st-0001-004', 'RemoteStopTransaction', 'Rejected');
    await waitFor(async () => (await call('GET', `/v1/sessions/${start.body.sessionId}`)).body.status === 'Active');
    // another driver cannot stop it
    const other = await h.registerDriver();
    expect((await h.driver(other.driverId)('POST', `/v1/sessions/${start.body.sessionId}/stop`)).body).toMatchObject({ status: 404 });
  });
});

describe('station events consumer', () => {
  it('connector status is last-write-wins on the station timestamp; stale and replayed events are ignored', async () => {
    const status = () => h.db.pool.query<{ status: string }>("SELECT status FROM connectors WHERE station_id = 'st-0001-001' AND connector_id = 2").then((r) => r.rows[0]?.status);
    const t = Date.now();
    await h.publishStationEvent('station.connector-status-changed', 'st-0001-001', { connectorId: 2, status: 'Preparing', errorCode: 'NoError', stationTimestamp: new Date(t).toISOString() });
    await waitFor(async () => (await status()) === 'Preparing');
    await h.publishStationEvent('station.connector-status-changed', 'st-0001-001', { connectorId: 2, status: 'Faulted', errorCode: 'GroundFailure', stationTimestamp: new Date(t - 60_000).toISOString() });
    await h.publishStationEvent('station.connector-status-changed', 'st-0001-001', { connectorId: 2, status: 'Available', errorCode: 'NoError', stationTimestamp: new Date(t + 1000).toISOString() });
    await waitFor(async () => (await status()) === 'Available');
    expect(await status()).toBe('Available');
  });

  it('online flag follows connect/disconnect, but a replaced connection (4000) does not mark the station offline', async () => {
    const online = () => h.db.pool.query<{ online: boolean }>("SELECT online FROM stations WHERE station_id = 'st-0001-003'").then((r) => r.rows[0]?.online);
    const t = Date.now();
    await h.publishStationEvent('station.connected', 'st-0001-003', { gatewayInstance: 'gw-2' }, new Date(t).toISOString());
    await waitFor(async () => (await online()) === true);
    await h.publishStationEvent('station.disconnected', 'st-0001-003', { gatewayInstance: 'gw-1', code: 4000, reason: 'replaced' }, new Date(t + 10).toISOString());
    await h.publishStationEvent('station.disconnected', 'st-0001-003', { gatewayInstance: 'gw-2', code: 1006, reason: 'gone' }, new Date(t + 20).toISOString());
    await waitFor(async () => (await online()) === false);
    expect(await online()).toBe(false);
  });
});

describe('outbox', () => {
  it('every outbox row reaches Kafka and is marked published', async () => {
    await waitFor(async () => (await h.db.pool.query('SELECT 1 FROM outbox WHERE published_at IS NULL')).rowCount === 0);
    const total = await count('SELECT count(*) AS n FROM outbox');
    expect(total).toBeGreaterThan(5);
    const ids = new Set([...h.sessionEvents.map((e) => e.eventId), ...h.commands.map((e) => e.eventId)]);
    await waitFor(async () => (await h.db.pool.query<{ id: string }>("SELECT payload->>'eventId' AS id FROM outbox")).rows.every((r) => ids.has(r.id)));
    // keyed by siteId / stationId as ADR 0002 requires
    const { rows } = await h.db.pool.query<{ topic: string; key: string }>('SELECT DISTINCT topic, key FROM outbox');
    expect(rows.map((r) => (r.topic === h.topics.sessionEvents ? r.key.startsWith('site-') : r.key.startsWith('st-')))).not.toContain(false);
  });
});

describe('observability', () => {
  it('exposes health, readiness and the service metrics', async () => {
    expect((await h.http('GET', '/health/live', { token: null })).status).toBe(200);
    const ready = await h.http('GET', '/health/ready', { token: null });
    expect(ready.status).toBe(200);
    const metrics = (await h.http('GET', '/metrics', { token: null })).body as string;
    expect(metrics).toContain('sessions_ocpp_request_seconds_bucket');
    expect(metrics).toContain('sessions_reallocation_latency_seconds');
    expect(metrics).toContain('outbox_pending');
  });
});

describe('ad-hoc charging with unknown idTags (ACCEPT_UNKNOWN_IDTAGS)', () => {
  let adhoc: Harness;
  beforeAll(async () => {
    adhoc = await startHarness({ sites: 1, stationsPerSite: 1, env: { ACCEPT_UNKNOWN_IDTAGS: 'true' } });
  });
  afterAll(async () => {
    await adhoc.close();
  });

  it('accepts an unknown idTag and starts a driverless session', async () => {
    expect((await adhoc.http('POST', '/internal/ocpp/authorize', { body: { stationId: 'st-0001-001', idTag: 'ADHOC1' } })).body).toEqual({ idTagInfo: { status: 'Accepted' } });
    const start = await adhoc.http('POST', '/internal/ocpp/start-transaction', {
      headers: { 'idempotency-key': 'st-0001-001:adhoc-1' },
      body: { stationId: 'st-0001-001', ocppMessageId: 'adhoc-1', connectorId: 1, idTag: 'ADHOC1', meterStartWh: 0, timestamp: new Date().toISOString() },
    });
    expect(start.status).toBe(200);
    expect(start.body.idTagInfo.status).toBe('Accepted');
    const { rows } = await adhoc.db.pool.query('SELECT driver_id FROM sessions WHERE session_id = $1', [start.body.sessionId]);
    expect(rows[0].driver_id).toBeNull();
  });
});
