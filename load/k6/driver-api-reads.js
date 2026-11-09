// driver-api reads: 1,000 RPS of GET /v1/sites with real JWTs (ADR 0000: >= 1,000 read RPS, p99 < 300 ms per instance).
import http from 'k6/http';
import { check } from 'k6';
import { TARGETS, uuid, summary } from './lib.js';

const T = TARGETS.perInstance['driver-api'];
const URL = __ENV.URL || 'http://localhost:28082';
export const options = {
  scenarios: { open: { executor: 'constant-arrival-rate', rate: T.readRps, timeUnit: '1s', duration: __ENV.DURATION || '60s', preAllocatedVUs: 300, maxVUs: 800 } },
  // Thresholds on the read scenario only: setup() registers and logs in (scrypt) and must not count as reads.
  thresholds: { 'http_req_duration{scenario:open}': [`p(99)<${T.readP99Ms}`], 'http_req_failed{scenario:open}': ['rate==0'], dropped_iterations: ['count==0'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};
export function setup() {
  const tokens = [];
  for (let i = 0; i < 20; i++) {
    const email = `k6-${uuid()}@load.test`;
    const password = `pw-${uuid()}`;
    const h = { headers: { 'content-type': 'application/json', 'idempotency-key': uuid() } };
    http.post(`${URL}/v1/auth/register`, JSON.stringify({ email, password }), h);
    const login = http.post(`${URL}/v1/auth/login`, JSON.stringify({ email, password }), { headers: { 'content-type': 'application/json' } });
    tokens.push(login.json('accessToken'));
  }
  return { tokens };
}
export default function (d) {
  const lat = (52.4 + (__ITER % 30) * 0.01).toFixed(3);
  const res = http.get(`${URL}/v1/sites?lat=${lat}&lng=13.4&radiusKm=10`, { headers: { authorization: `Bearer ${d.tokens[__ITER % d.tokens.length]}` } });
  check(res, { '200': (r) => r.status === 200 });
}
export const handleSummary = summary('driver-api-reads');
