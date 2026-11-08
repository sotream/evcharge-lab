// sessions OCPP hot path: StartTransaction + StopTransaction pairs through the internal API, as the gateway calls it.
// ADR 0000: sessions >= 200 commands/s per instance; CALLRESULT p99 < 500 ms (this is the sessions share of it).
import http from 'k6/http';
import { check } from 'k6';
import { TARGETS, internalHeaders, stationId, uuid, summary } from './lib.js';

const T = TARGETS.perInstance.sessions;
const URL = __ENV.URL || 'http://localhost:28081';
const SITES = Number(__ENV.SITES || 20);
const PER_SITE = Number(__ENV.STATIONS_PER_SITE || 10);
export const options = {
  scenarios: { open: { executor: 'constant-arrival-rate', rate: T.commandsPerSec / 2, timeUnit: '1s', duration: __ENV.DURATION || '60s', preAllocatedVUs: 50, maxVUs: 300 } },
  thresholds: { 'http_req_duration{op:start}': ['p(99)<500'], 'http_req_duration{op:stop}': ['p(99)<500'], http_req_failed: ['rate<0.001'], dropped_iterations: ['count<1'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};
export default function () {
  const g = __ITER % (SITES * PER_SITE);
  const st = stationId(1 + Math.floor(g / PER_SITE), 1 + (g % PER_SITE));
  const connectorId = 1 + (Math.floor(__ITER / (SITES * PER_SITE)) % 2);
  const m1 = uuid();
  const start = http.post(`${URL}/internal/ocpp/start-transaction`, JSON.stringify({ stationId: st, ocppMessageId: m1, connectorId, idTag: 'K6LOAD', meterStartWh: 0, timestamp: new Date().toISOString() }),
    { headers: { ...internalHeaders, 'idempotency-key': `${st}:${m1}` }, tags: { op: 'start' } });
  if (!check(start, { 'start 200': (r) => r.status === 200 })) return;
  const m2 = uuid();
  const stop = http.post(`${URL}/internal/ocpp/stop-transaction`, JSON.stringify({ stationId: st, ocppMessageId: m2, transactionId: start.json('transactionId'), meterStopWh: 5000, timestamp: new Date().toISOString(), reason: 'Local' }),
    { headers: { ...internalHeaders, 'idempotency-key': `${st}:${m2}` }, tags: { op: 'stop' } });
  check(stop, { 'stop 200': (r) => r.status === 200 });
}
export const handleSummary = summary('sessions-ocpp');
