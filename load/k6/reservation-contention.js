// N drivers reserve the SAME connector at the same instant (ADR 0005): exactly one 201, the rest 409.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';
import { internalHeaders, uuid, summary } from './lib.js';

const URL = __ENV.URL || 'http://localhost:28081';
const N = Number(__ENV.N || 100);
const won = new Counter('reservation_won');
const lost = new Counter('reservation_conflict');
export const options = {
  scenarios: { burst: { executor: 'per-vu-iterations', vus: N, iterations: 1, maxDuration: '60s' } },
  thresholds: { reservation_won: ['count==1'], reservation_conflict: [`count==${N - 1}`] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};
export function setup() {
  // Every VU is a registered driver (sessions rejects reservations from drivers without an idTag).
  const drivers = [];
  for (let i = 0; i < N; i++) {
    const driverId = uuid();
    const idTag = `K6R${driverId.replace(/-/g, '').slice(0, 17)}`;
    const r = http.post(`${URL}/internal/drivers`, JSON.stringify({ driverId, idTag }), { headers: { ...internalHeaders, 'idempotency-key': driverId } });
    if (r.status < 300) drivers.push(driverId);
  }
  if (drivers.length < N) throw new Error(`registered ${drivers.length} of ${N} drivers: the burst would not be N concurrent drivers`);
  // A free connector: probe candidates with the first driver, release the probe at once. The simulator also uses these
  // connectors, so a fixed or time-derived pick lands on a held one and the run proves nothing (0 x 201, N x 409).
  const probe = (stationId, connectorId) => http.post(`${URL}/v1/reservations`, JSON.stringify({ stationId, connectorId }),
    { headers: { ...internalHeaders, 'x-driver-id': drivers[0], 'idempotency-key': uuid() }, responseCallback: http.expectedStatuses(201, 409) });
  for (let s = 1; s <= 10; s++) {
    for (let c = 1; c <= 2; c++) {
      const stationId = `st-0020-${String(s).padStart(3, '0')}`;
      const r = probe(stationId, c);
      if (r.status !== 201) continue;
      http.del(`${URL}/v1/reservations/${r.json('reservationId')}`, null, { headers: { ...internalHeaders, 'x-driver-id': drivers[0] } });
      return { drivers, stationId, connectorId: c };
    }
  }
  throw new Error('no free connector among st-0020-001..010 connectors 1-2');
}
export default function (d) {
  const res = http.post(`${URL}/v1/reservations`, JSON.stringify({ stationId: d.stationId, connectorId: d.connectorId }),
    { headers: { ...internalHeaders, 'x-driver-id': d.drivers[(__VU - 1) % d.drivers.length], 'idempotency-key': uuid() }, responseCallback: http.expectedStatuses(201, 409) });
  if (res.status === 201) {
    won.add(1);
    // The winner releases the connector so the next run can use it, but only after the whole burst is over: releasing at once
    // frees the connector in the middle of the burst and lets a later request win.
    sleep(3);
    http.del(`${URL}/v1/reservations/${res.json('reservationId')}`, null, { headers: { ...internalHeaders, 'x-driver-id': d.drivers[(__VU - 1) % d.drivers.length] } });
  }
  else if (res.status === 409) lost.add(1);
  check(res, { '201 or 409': (r) => r.status === 201 || r.status === 409 });
}
export const handleSummary = summary('reservation-contention');
