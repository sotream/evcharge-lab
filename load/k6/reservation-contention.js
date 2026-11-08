// N drivers reserve the SAME connector at the same instant (ADR 0005): exactly one 201, the rest 409.
import http from 'k6/http';
import { check } from 'k6';
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
  // A distinct free connector per run: station st-0020-0xx connector 1, picked by the run timestamp.
  return { stationId: `st-0020-${String(1 + (Math.floor(Date.now() / 1000) % 10)).padStart(3, '0')}`, connectorId: 1 };
}
export default function (d) {
  const res = http.post(`${URL}/v1/reservations`, JSON.stringify({ stationId: d.stationId, connectorId: d.connectorId }),
    { headers: { ...internalHeaders, 'x-driver-id': uuid(), 'idempotency-key': uuid() }, responseCallback: http.expectedStatuses(201, 409) });
  if (res.status === 201) won.add(1);
  else if (res.status === 409) lost.add(1);
  check(res, { '201 or 409': (r) => r.status === 201 || r.status === 409 });
}
export const handleSummary = summary('reservation-contention');
