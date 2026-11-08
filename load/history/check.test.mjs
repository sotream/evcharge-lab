// Unit tests for check.mjs (node --test load/history/).
import test from 'node:test';
import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { checkHistory, FOOTER, formatReport } from './check.mjs';

const T0 = Date.parse('2026-01-01T00:00:00Z');
const iso = (ms) => new Date(ms).toISOString();
const CLI = join(dirname(fileURLToPath(import.meta.url)), 'check.mjs');
let n = 0;
const reserve = (rid, at, o = {}) => ({ opId: `r${++n}`, driverId: 'd', type: 'reserve', stationId: 'S1', connectorId: 1, invokeMs: at - 5, completeMs: at + 5, outcome: 'ok', status: 201, reservationId: rid, startsAt: iso(at), expiresAt: iso(at + 900_000), ...o });
const cancel = (rid, at, o = {}) => ({ opId: `c${++n}`, driverId: 'd', type: 'cancel', stationId: 'S1', connectorId: 1, reservationId: rid, invokeMs: at, completeMs: at + 5, outcome: 'ok', status: 200, ...o });
const row = (rid, at, status = 'Pending', o = {}) => ({ reservation_id: rid, station_id: 'S1', connector_id: 1, starts_at: iso(at), expires_at: iso(at + 900_000), status, ...o });
const unknownOp = (at) => reserve(0, at, { outcome: 'error', status: 0, reservationId: undefined, startsAt: undefined, expiresAt: undefined });
const failed = (r, kind) => !r.ok && r.violations.some((v) => v.kind.includes(kind));

test('clean history passes', () => {
  const r = checkHistory([reserve(1, T0), cancel(1, T0 + 1000), reserve(2, T0 + 2000)], [row(1, T0, 'Cancelled'), row(2, T0 + 2000)]);
  assert.ok(r.ok, JSON.stringify(r.violations));
  assert.equal(r.totals.acknowledged, 2);
});

test('overlapping accepted reservations on one connector fail', () => {
  const r = checkHistory([reserve(1, T0), reserve(2, T0 + 60_000)], [row(1, T0), row(2, T0 + 60_000)]);
  assert.ok(failed(r, 'acknowledged overlap'));
  assert.equal(r.violations[0].opIds.length, 2);
});

test('overlap vanishes when the first was cancelled before the second started', () => {
  const r = checkHistory([reserve(1, T0), cancel(1, T0 + 1000), reserve(2, T0 + 60_000)], [row(1, T0, 'Cancelled'), row(2, T0 + 60_000)]);
  assert.ok(r.ok);
});

test('overlap on different connectors is fine', () => {
  const r = checkHistory([reserve(1, T0), reserve(2, T0, { connectorId: 2 })], [row(1, T0), row(2, T0, 'Pending', { connector_id: 2 })]);
  assert.ok(r.ok);
});

test('acknowledged reservation missing from DB fails', () => {
  const r = checkHistory([reserve(1, T0), reserve(2, T0 + 1_000_000)], [row(2, T0 + 1_000_000)]);
  assert.ok(failed(r, 'acknowledged reservation lost'));
  assert.match(r.violations[0].detail, /reservationId 1/);
});

test('acknowledged reservation on another connector in DB fails', () => {
  const r = checkHistory([reserve(1, T0)], [row(1, T0, 'Pending', { connector_id: 9 })]);
  assert.ok(failed(r, 'acknowledged reservation lost'));
});

test('error reserve present in DB is counted and not a violation by itself', () => {
  const r = checkHistory([unknownOp(T0)], [row(70, T0 + 2)]);
  assert.ok(r.ok);
  assert.equal(r.totals.indeterminate, 1);
  assert.equal(r.totals.indeterminateInDb, 1);
});

test('error reserve absent from DB is indeterminate and passes', () => {
  const r = checkHistory([unknownOp(T0)], []);
  assert.ok(r.ok);
  assert.equal(r.totals.indeterminateInDb, 0);
});

test('two unknown ops never produce a definite violation', () => {
  const r = checkHistory([unknownOp(T0), unknownOp(T0 + 10)], [row(1, T0 + 1, 'Cancelled'), row(2, T0 + 11, 'Expired')]);
  assert.ok(r.ok);
  assert.ok(r.informational.length > 0);
});

test('unknown row overlapping an acknowledged one is informational unless still open', () => {
  const u = unknownOp(T0 + 10);
  const quiet = checkHistory([reserve(1, T0), u], [row(1, T0), row(2, T0 + 11, 'Cancelled')]);
  assert.ok(!quiet.violations.some((v) => v.opIds.includes(u.opId)));
  assert.ok(quiet.informational.length > 0);
  const bad = checkHistory([reserve(1, T0), u], [row(1, T0), row(2, T0 + 11, 'Active')]);
  assert.ok(!bad.ok);
});

test('mutated history (protection disabled: two ok at the same time) goes red', () => {
  const r = checkHistory([reserve(1, T0), reserve(2, T0)], [row(1, T0), row(2, T0)]);
  assert.ok(!r.ok);
  assert.ok(r.violations.length >= 2); // history overlap and DB-row overlap
});

test('rejected reserves are ignored', () => {
  const r = checkHistory([reserve(1, T0), reserve(0, T0 + 10, { outcome: 'rejected', status: 409, reservationId: undefined })], [row(1, T0)]);
  assert.ok(r.ok);
  assert.equal(r.totals.rejected, 1);
});

test('report carries the honesty footer', () => {
  assert.ok(formatReport(checkHistory([], [])).endsWith(FOOTER));
});

test('bad input: pure function throws, CLI exits 2', () => {
  assert.throws(() => checkHistory('x', []));
  assert.throws(() => checkHistory([{ nope: 1 }], []));
  const dir = mkdtempSync(join(tmpdir(), 'hist-'));
  writeFileSync(join(dir, 'h.jsonl'), 'not json\n');
  writeFileSync(join(dir, 'db.json'), '[]');
  assert.equal(spawnSync('node', [CLI, '--history', join(dir, 'h.jsonl'), '--db', join(dir, 'db.json')]).status, 2);
  assert.equal(spawnSync('node', [CLI]).status, 2);
});

test('CLI exit codes: 0 pass, 1 violation', () => {
  const dir = mkdtempSync(join(tmpdir(), 'hist-'));
  const run = (ops, db) => {
    writeFileSync(join(dir, 'h.jsonl'), ops.map((o) => JSON.stringify(o)).join('\n'));
    writeFileSync(join(dir, 'db.json'), JSON.stringify(db));
    return spawnSync('node', [CLI, '--history', join(dir, 'h.jsonl'), '--db', join(dir, 'db.json')]).status;
  };
  assert.equal(run([reserve(1, T0)], [row(1, T0)]), 0);
  assert.equal(run([reserve(1, T0)], []), 1);
});

test('a reservation acknowledged as Pending and rejected by the station moments later does not hold the connector', () => {
  const rejected = row(1, T0, 'Rejected', { ended_at: iso(T0 + 50) });
  const r = checkHistory([reserve(1, T0), reserve(2, T0 + 120)], [rejected, row(2, T0 + 120, 'Active', { ended_at: null })]);
  assert.ok(r.ok, JSON.stringify(r.violations));
});

test('with the protection disabled two overlapping rows stay Active and the checker goes red', () => {
  const r = checkHistory([reserve(1, T0), reserve(2, T0 + 120)], [row(1, T0, 'Active', { ended_at: null }), row(2, T0 + 120, 'Active', { ended_at: null })]);
  assert.ok(failed(r, 'overlap'));
});

test('ended_at beats the client view: the DB says the first reservation ended after the second began', () => {
  const r = checkHistory([reserve(1, T0), cancel(1, T0 + 100), reserve(2, T0 + 200)], [row(1, T0, 'Cancelled', { ended_at: iso(T0 + 500) }), row(2, T0 + 200, 'Active', { ended_at: null })]);
  assert.ok(failed(r, 'overlap'));
});

test('an acknowledged reservation whose id was reissued to a different reservation counts as lost', () => {
  // the promoted node lost reservation 7 (acknowledged at T0) and later issued id 7 again, for another driver, 5 s later
  const r = checkHistory([reserve(7, T0)], [row(7, T0 + 5000)]);
  assert.ok(failed(r, 'reservation lost'));
  assert.match(r.violations[0].detail, /id reused/);
});
