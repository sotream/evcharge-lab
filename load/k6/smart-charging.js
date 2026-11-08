// Open-loop 1,000 allocations/s against smart-charging (ADR 0000: >= 1,000/s, p99 < 20 ms, one instance).
import http from 'k6/http';
import { check } from 'k6';
import { TARGETS, internalHeaders, uuid, summary } from './lib.js';

const T = TARGETS.perInstance['smart-charging'];
const URL = __ENV.URL || 'http://localhost:28084';
export const options = {
  scenarios: { open: { executor: 'constant-arrival-rate', rate: T.allocationsPerSec, timeUnit: '1s', duration: __ENV.DURATION || '60s', preAllocatedVUs: 100, maxVUs: 400 } },
  thresholds: { http_req_duration: [`p(99)<${T.p99Ms}`], http_req_failed: ['rate<0.001'], dropped_iterations: ['count<1'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

function site(i) {
  const connectors = 10 + (i % 71);
  const active = Math.max(1, Math.floor(connectors * (0.1 + 0.3 * ((i * 7919) % 100) / 100)));
  const now = Date.now();
  const sessions = [];
  for (let k = 0; k < active; k++) {
    const dc = k % 5 === 4;
    sessions.push({ sessionId: uuid(), stationId: `st-${i}-${k}`, connectorId: 1 + (k % 2), maxW: dc ? 150000 : 22000, minW: dc ? 5000 : 4140,
      measuredW: k % 3 === 0 ? 7000 : undefined, priority: 0, startedAt: new Date(now - (k + 1) * 120000).toISOString() });
  }
  const capW = Math.floor(connectors * 11000 * 0.5);
  return { siteId: `site-${i}`, siteCapW: capW, idleConnectors: connectors - active, startW: Math.min(4140, Math.floor(capW / connectors)), sessions };
}

export default function () {
  const res = http.post(`${URL}/v1/allocate`, JSON.stringify(site(__ITER % 2000)), { headers: internalHeaders });
  check(res, { '200': (r) => r.status === 200 });
}
export const handleSummary = summary('smart-charging');
