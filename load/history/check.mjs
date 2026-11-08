// Offline reservation-history checker.
//   node load/history/check.mjs --history history.jsonl --db reservations.json
// Dump the DB after recovery (compose Postgres, host port 25432):
//   PGPASSWORD=evcharge psql -h 127.0.0.1 -p 25432 -U evcharge -d sessions -At -c \
//     "SELECT COALESCE(json_agg(json_build_object('reservation_id', r.reservation_id, 'station_id', c.station_id, 'connector_id', c.connector_id, 'starts_at', r.starts_at, 'expires_at', r.expires_at, 'status', r.status, 'ended_at', r.ended_at)), '[]') FROM reservations r JOIN connectors c ON c.connector_pk = r.connector_pk" > reservations.json
// (or COPY (SELECT json_agg(...) ...) TO STDOUT). Exit: 0 pass, 1 violation, 2 bad input.
// Proves only (a) no overlap of acknowledged intervals per connector, (b) every acknowledged reserve is in the DB.
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const OPEN = new Set(['Pending', 'Active']);
const KNOWN = new Set(['Pending', 'Active', 'Used', 'Cancelled', 'Expired']);
const STARTS_AT_SLACK_MS = 1; // the API and the database both carry millisecond timestamps of the same instant
const MATCH_SLACK_MS = 1000; // error-reserve -> DB row match window around [invokeMs, completeMs]
const key = (o) => `${o.stationId ?? o.station_id}#${o.connectorId ?? o.connector_id}`;
const overlap = (a, b) => b.start < a.end && a.start < b.end;

function ts(v, what) {
  const t = Date.parse(v);
  if (Number.isNaN(t)) throw new TypeError(`bad timestamp in ${what}: ${v}`);
  return t;
}

function validate(ops, dbRows) {
  if (!Array.isArray(ops) || !Array.isArray(dbRows)) throw new TypeError('ops and dbRows must be arrays');
  for (const o of ops) {
    if (!o || typeof o.opId !== 'string' || !['reserve', 'cancel'].includes(o.type) || !['ok', 'rejected', 'error'].includes(o.outcome)) throw new TypeError(`bad op: ${JSON.stringify(o)}`);
    if (o.type === 'reserve' && o.outcome === 'ok') {
      ts(o.startsAt, o.opId);
      ts(o.expiresAt, o.opId);
    }
  }
  for (const r of dbRows) if (!r || r.reservation_id === undefined) throw new TypeError(`bad db row: ${JSON.stringify(r)}`);
}

/** End of the period a row held its connector: the earlier of expires_at and ended_at. */
const heldEnd = (row) => Math.min(ts(row.expires_at, 'db'), row.ended_at ? ts(row.ended_at, 'db') : Infinity);

export function checkHistory(ops, dbRows) {
  validate(ops, dbRows);
  const violations = [];
  const info = [];
  const okCancel = new Map(); // reservationId -> earliest accepted cancel invoke
  for (const o of ops)
    if (o.type === 'cancel' && o.outcome === 'ok' && o.reservationId !== undefined) okCancel.set(o.reservationId, Math.min(okCancel.get(o.reservationId) ?? Infinity, o.invokeMs));

  const reserves = ops.filter((o) => o.type === 'reserve');
  const acked = reserves.filter((o) => o.outcome === 'ok');
  const unknown = reserves.filter((o) => o.outcome === 'error');
  const dbById = new Map(dbRows.map((r) => [r.reservation_id, r]));

  // (b) durability of acknowledged reservations
  for (const o of acked) {
    const row = dbById.get(o.reservationId);
    // After a promotion the database can hand out a lost id again: the id then exists, but it is a different reservation.
    const sameRow = row && key(row) === key(o) && Math.abs(ts(row.starts_at, 'db') - ts(o.startsAt, o.opId)) <= STARTS_AT_SLACK_MS;
    if (!sameRow) violations.push({ kind: 'acknowledged reservation lost', opIds: [o.opId], detail: `reservationId ${o.reservationId} on ${key(o)} ${!row ? 'missing from DB' : key(row) !== key(o) ? 'found on another connector' : 'id reused by a different reservation (starts_at differs)'}` });
  }

  // (a) overlap of acknowledged intervals; definite end honours an accepted cancel
  // A 201 only means "Pending": the station may reject the reservation moments later (status Rejected, ended_at), and a driver
  // cancel sets ended_at too. When the DB row carries ended_at (the dump has the key), the DB decides when the connector stopped
  // being held: [starts_at, min(expires_at, ended_at)). Without it the client's accepted cancel is the best available end.
  const items = acked.map((o) => {
    const row = dbById.get(o.reservationId);
    if (row && 'ended_at' in row && key(row) === key(o) && Math.abs(ts(row.starts_at, 'db') - ts(o.startsAt, o.opId)) <= STARTS_AT_SLACK_MS) return { k: key(o), id: o.opId, start: ts(row.starts_at, 'db'), end: heldEnd(row), definite: true };
    const start = ts(o.startsAt, o.opId);
    const end = Math.min(ts(o.expiresAt, o.opId), okCancel.get(o.reservationId) ?? Infinity);
    return { k: key(o), id: o.opId, start, end, definite: true };
  });

  // unknown reserves: matched to unclaimed DB rows by connector and start time; they only ever enter the check via the DB row
  const ackedIds = new Set(acked.map((o) => o.reservationId));
  const claimed = new Set();
  let materialised = 0;
  for (const o of unknown) {
    const row = dbRows.find((r) => !ackedIds.has(r.reservation_id) && !claimed.has(r.reservation_id) && KNOWN.has(r.status) && key(r) === key(o) && ts(r.starts_at, 'db') >= o.invokeMs - MATCH_SLACK_MS && ts(r.starts_at, 'db') <= o.completeMs + MATCH_SLACK_MS);
    if (!row) continue;
    claimed.add(row.reservation_id);
    materialised++;
    items.push({ k: key(o), id: o.opId, start: ts(row.starts_at, 'db'), end: ts(row.expires_at, 'db'), definite: OPEN.has(row.status), fromUnknown: true });
  }

  const pair = (list, label) => {
    const byKey = new Map();
    for (const it of list) byKey.set(it.k, [...(byKey.get(it.k) ?? []), it]);
    for (const [k, arr] of byKey)
      for (let i = 0; i < arr.length; i++)
        for (let j = i + 1; j < arr.length; j++) {
          const a = arr[i];
          const b = arr[j];
          if (a.fromUnknown && b.fromUnknown) continue; // covered by the DB-row pass
          if (!overlap(a, b)) continue;
          const v = { kind: `${label} overlap`, opIds: [a.id, b.id], detail: `${k}: [${a.start},${a.end}) vs [${b.start},${b.end})` };
          (a.definite && b.definite ? violations : info).push(v);
        }
  };
  pair(items, 'acknowledged');

  // DB rows themselves: overlapping Pending/Active rows are definite, anything else informational
  const rowItems = dbRows.filter((r) => KNOWN.has(r.status)).map((r) => ({ k: key(r), id: `reservation:${r.reservation_id}`, start: ts(r.starts_at, 'db'), end: 'ended_at' in r ? heldEnd(r) : ts(r.expires_at, 'db'), definite: 'ended_at' in r || OPEN.has(r.status) }));
  pair(rowItems, 'db-row');

  return {
    ok: violations.length === 0,
    totals: { ops: ops.length, reserves: reserves.length, acknowledged: acked.length, rejected: reserves.filter((o) => o.outcome === 'rejected').length, indeterminate: unknown.length, indeterminateInDb: materialised, cancelsOk: okCancel.size, dbRows: dbRows.length },
    violations,
    informational: info,
  };
}

export const FOOTER = 'This is not a full linearizability check: it proves non-overlap of acknowledged intervals and durability of acknowledged reservations only.';

export function formatReport(r) {
  const t = r.totals;
  const lines = [
    `ops ${t.ops}, reserves ${t.reserves} (acknowledged ${t.acknowledged}, rejected ${t.rejected}, indeterminate ${t.indeterminate}, of which found in DB ${t.indeterminateInDb}), accepted cancels ${t.cancelsOk}, DB rows ${t.dbRows}`,
    `informational overlaps (not violations): ${r.informational.length}`,
    r.ok ? 'RESULT: PASS' : `RESULT: FAIL, ${r.violations.length} violation(s)`,
    ...r.violations.map((v) => `  VIOLATION ${v.kind} [${v.opIds.join(', ')}] ${v.detail}`),
    '',
    FOOTER,
  ];
  return lines.join('\n');
}

function cli(argv) {
  const arg = (n) => argv[argv.indexOf(n) + 1];
  try {
    if (!argv.includes('--history') || !argv.includes('--db')) throw new TypeError('usage: check.mjs --history history.jsonl --db reservations.json');
    const ops = readFileSync(arg('--history'), 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
    const db = JSON.parse(readFileSync(arg('--db'), 'utf8').trim() || '[]');
    const report = checkHistory(ops, db ?? []);
    console.log(formatReport(report));
    return report.ok ? 0 : 1;
  } catch (e) {
    console.error(`bad input: ${e.message}`);
    return 2;
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) process.exit(cli(process.argv.slice(2)));
