import { fleet } from '@evcharge/contracts';
/* oxlint-disable no-await-in-loop */
import { createHash, createHmac, randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { closeStaleSessions, revertUnconfirmedStops } from '../../src/workers/maintenance';
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

  it('flags a regressed meter and an implausible energy on session.completed', async () => {
    const d = await h.registerDriver();
    const s = await h.startTx('st-0001-003', 1, d.idTag); // harness starts with meterStartWh = 1000
    await h.stopTx('st-0001-003', s.body.transactionId, undefined, 99_999_999);
    const ev = await waitFor(() => types('session.completed', s.body.sessionId)[0]);
    expect((ev.data as { energyFlag?: string }).energyFlag).toBe('implausible');
    expect((await h.db.pool.query('SELECT energy_flag FROM sessions WHERE session_id = $1', [s.body.sessionId])).rows[0].energy_flag).toBe('implausible');
  });
});

describe('driver deletion hardening', () => {
  it('a start waits for a deletion in progress (driver row lock) instead of racing its active-session check', async () => {
    const d = await h.registerDriver();
    const c = await h.db.pool.connect();
    try {
      await c.query('BEGIN');
      await c.query('SELECT 1 FROM drivers WHERE driver_id = $1 FOR UPDATE', [d.driverId]); // what DELETE /internal/drivers/:id holds
      let done = false;
      const start = h.startTx('st-0001-003', 1, d.idTag).then((r) => {
        done = true;
        return r;
      });
      await new Promise((r) => setTimeout(r, 400));
      expect(done).toBe(false);
      await c.query('COMMIT');
      const r = await start;
      expect(r.status).toBe(200);
      await h.stopTx('st-0001-003', r.body.transactionId);
    } finally {
      c.release();
    }
  });
});

describe('energy flag regression', () => {
  it('flags a meter that went backwards as meter_regressed', async () => {
    const d = await h.registerDriver();
    const s = await h.startTx('st-0001-003', 1, d.idTag); // harness starts at meterStartWh = 1000
    await h.stopTx('st-0001-003', s.body.transactionId, undefined, 500);
    const ev = await waitFor(() => types('session.completed', s.body.sessionId)[0]);
    expect((ev.data as { energyFlag?: string }).energyFlag).toBe('meter_regressed');
  });
});

describe('driver deletion', () => {
  it('blocks the idTag, keeps driver_id, is idempotent, and is refused with a charging session', async () => {
    const d = await h.registerDriver();
    const s = await h.startTx('st-0001-003', 1, d.idTag);
    expect((await h.http('DELETE', `/internal/drivers/${d.driverId}`)).status).toBe(409);
    await h.stopTx('st-0001-003', s.body.transactionId);
    expect((await h.http('DELETE', `/internal/drivers/${d.driverId}`)).status).toBe(204);
    expect((await h.http('DELETE', `/internal/drivers/${d.driverId}`)).status).toBe(204);
    expect((await h.http('POST', '/internal/ocpp/authorize', { body: { stationId: 'st-0001-001', idTag: d.idTag } })).body.idTagInfo.status).toBe('Blocked');
    const row = (await h.db.pool.query('SELECT id_tag, blocked FROM drivers WHERE driver_id = $1', [d.driverId])).rows[0];
    expect(row.blocked).toBe(true);
    expect(row.id_tag).toMatch(/^deleted-[0-9a-f]{12}$/);
  });
});

describe('signed meter data', () => {
  it('stores SignedData from StopTransaction and returns it on the session', async () => {
    const d = await h.registerDriver();
    const s = await h.startTx('st-0001-003', 1, d.idTag);
    const signedMeterData = [{ format: 'SignedData', value: 'OCMF|{"FV":"1.0"}|{"SD":"abc"}', context: 'Transaction.End' }];
    const r = await h.stopTx('st-0001-003', s.body.transactionId, undefined, 6000, { signedMeterData });
    expect(r.status).toBe(200);
    const got = await h.driver(d.driverId)('GET', `/v1/sessions/${s.body.sessionId}`);
    expect(got.body.signedMeterData).toEqual(signedMeterData);
  });
});

describe('stuck Stopping sweeper', () => {
  it('reverts a Stopping session whose RemoteStop got no result; a late Accepted for it changes nothing and the stop still completes once', async () => {
    const d = await h.registerDriver();
    const s = await h.startTx('st-0001-001', 2, d.idTag);
    const api = h.driver(d.driverId);
    expect((await api('POST', `/v1/sessions/${s.body.sessionId}/stop`)).body.status).toBe('Stopping');
    const first = await waitFor(() => h.commands.find((c) => c.data.action === 'RemoteStopTransaction' && (c.data.payload as { transactionId?: number }).transactionId === s.body.transactionId));
    await h.db.pool.query("UPDATE commands SET expires_at = now() - interval '1 minute' WHERE command_id = $1", [first.data.commandId]);
    expect(await revertUnconfirmedStops(h.db.pool)).toBe(1);
    expect((await api('GET', `/v1/sessions/${s.body.sessionId}`)).body.status).toBe('Active');

    // Review Focus 5: the station's Accepted arrives after the sweeper gave up on that command
    await h.publishCommandResult(first.data.commandId, 'st-0001-001', 'RemoteStopTransaction', 'Accepted');
    await new Promise((r) => setTimeout(r, 500));
    expect((await api('GET', `/v1/sessions/${s.body.sessionId}`)).body.status).toBe('Active');
    expect((await h.db.pool.query('SELECT outcome FROM commands WHERE command_id = $1', [first.data.commandId])).rows[0].outcome).toBe('Expired');

    expect((await api('POST', `/v1/sessions/${s.body.sessionId}/stop`)).body.status).toBe('Stopping');
    expect((await h.stopTx('st-0001-001', s.body.transactionId)).status).toBe(200);
    await waitFor(() => types('session.completed', s.body.sessionId).length >= 1);
    await new Promise((r) => setTimeout(r, 300));
    expect(types('session.completed', s.body.sessionId)).toHaveLength(1);
    expect((await api('GET', `/v1/sessions/${s.body.sessionId}`)).body.status).toBe('Completed');
  });
});

describe('stale session reaper', () => {
  it('closes a stale session as StaleClosed, and a late StopTransaction is accepted without a second completion', async () => {
    const d = await h.registerDriver();
    const s = await h.startTx('st-0001-004', 1, d.idTag);
    const { transactionId, sessionId } = s.body;
    // Staleness is server time (the session_id's uuidv7 timestamp): a threshold of 0 makes every existing session stale.
    expect(await closeStaleSessions(h.db.pool, h.topics, 0)).toBeGreaterThanOrEqual(1);
    const ev = await waitFor(() => types('session.completed', sessionId)[0]);
    expect(ev.data).toMatchObject({ stopReason: 'StaleClosed', energyWh: 0, energyFlag: 'stale' });

    // Review Focus 2
    const r = await h.stopTx('st-0001-004', transactionId, undefined, 20_000);
    expect(r.status).toBe(200);
    await new Promise((res) => setTimeout(res, 500));
    expect(types('session.completed', sessionId)).toHaveLength(1);
  });

  it('does not reap a fresh session whose station clock is days behind', async () => {
    const d = await h.registerDriver();
    const s = await h.startTx('st-0001-002', 1, d.idTag);
    await h.db.pool.query("UPDATE active_sessions SET started_at = now() - interval '3 days' WHERE transaction_id = $1", [s.body.transactionId]);
    expect(await closeStaleSessions(h.db.pool, h.topics, 48 * 36e5)).toBe(0);
    expect((await h.db.pool.query('SELECT 1 FROM active_sessions WHERE transaction_id = $1', [s.body.transactionId])).rowCount).toBe(1);
    await h.stopTx('st-0001-002', s.body.transactionId);
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

  it('with ACCEPT_UNKNOWN_IDTAGS the idTag of a deleted account is Blocked, not ad-hoc Accepted', async () => {
    const d = await adhoc.registerDriver();
    expect((await adhoc.http('POST', '/internal/ocpp/authorize', { body: { stationId: 'st-0001-001', idTag: d.idTag } })).body.idTagInfo.status).toBe('Accepted');
    expect((await adhoc.http('DELETE', `/internal/drivers/${d.driverId}`)).status).toBe(204);
    expect((await adhoc.http('POST', '/internal/ocpp/authorize', { body: { stationId: 'st-0001-001', idTag: d.idTag } })).body.idTagInfo.status).toBe('Blocked');
    const start = await adhoc.startTx('st-0001-001', 1, d.idTag);
    expect(start.body.idTagInfo.status).toBe('Blocked');
    expect((await adhoc.http('POST', '/internal/ocpp/authorize', { body: { stationId: 'st-0001-001', idTag: 'SOMEONE-ELSE' } })).body.idTagInfo.status).toBe('Accepted');
  });
});

describe('concurrent StartTransactions on one connector', () => {
  let hc: Harness;
  beforeAll(async () => {
    hc = await startHarness({ sites: 1, stationsPerSite: 1, env: { ACCEPT_UNKNOWN_IDTAGS: 'true' } });
  });
  afterAll(async () => {
    await hc.close();
  });

  it('all succeed and exactly one stays active (no unique violation)', async () => {
    const starts = await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        hc.http('POST', '/internal/ocpp/start-transaction', {
          headers: { 'idempotency-key': `st-0001-001:race-${i}` },
          body: { stationId: 'st-0001-001', ocppMessageId: `race-${i}`, connectorId: 1, idTag: 'RACE', meterStartWh: 0, timestamp: new Date().toISOString() },
        }),
      ),
    );
    expect(starts.map((s) => s.status)).toEqual(Array(10).fill(200));
    const { rows } = await hc.db.pool.query("SELECT count(*)::int AS n FROM active_sessions WHERE station_id = 'st-0001-001' AND connector_id = 1");
    expect(rows[0].n).toBe(1);
  });
});

describe('uniqueId reuse after a firmware restart', () => {
  let hr: Harness;
  beforeAll(async () => {
    hr = await startHarness({ sites: 1, stationsPerSite: 1, env: { ACCEPT_UNKNOWN_IDTAGS: 'true' } });
  });
  afterAll(async () => {
    await hr.close();
  });
  const start = (uid: string, meterStartWh: number, timestamp: string) =>
    hr.http('POST', '/internal/ocpp/start-transaction', {
      headers: { 'idempotency-key': `st-0001-001:${uid}` },
      body: { stationId: 'st-0001-001', ocppMessageId: uid, connectorId: 1, idTag: 'REUSE', meterStartWh, timestamp },
    });

  it('a replay (same uniqueId, same payload) gets the stored transactionId; a reused uniqueId with a new payload starts a new transaction', async () => {
    const t1 = new Date(Date.now() - 60_000).toISOString();
    const a = await start('reused-1', 100, t1);
    const replay = await start('reused-1', 100, t1);
    expect(replay.body.transactionId).toBe(a.body.transactionId);
    // A StopTransaction that happens to reuse the same uniqueId must not be answered with the start's response.
    const stop = await hr.http('POST', '/internal/ocpp/stop-transaction', {
      headers: { 'idempotency-key': 'st-0001-001:reused-1' },
      body: { stationId: 'st-0001-001', ocppMessageId: 'reused-1', transactionId: a.body.transactionId, meterStopWh: 500, timestamp: new Date().toISOString() },
    });
    expect(stop.status).toBe(200);
    expect(stop.body.sessionId).toBe(a.body.sessionId);
    const b = await start('reused-1', 900, new Date().toISOString());
    expect(b.status).toBe(200);
    expect(typeof b.body.transactionId).toBe('number');
    expect(b.body.transactionId).not.toBe(a.body.transactionId);
  });
});
