// Open-loop 2,000 quotes/s against tariff (ADR 0000: >= 2,000/s, p99 < 20 ms, one instance).
import http from 'k6/http';
import { check } from 'k6';
import { TARGETS, internalHeaders, summary } from './lib.js';

const T = TARGETS.perInstance.tariff;
const URL = __ENV.URL || 'http://localhost:28085';
const IDS = ['std', 'tou', 'fleet'];
export const options = {
  scenarios: { open: { executor: 'constant-arrival-rate', rate: T.calculationsPerSec, timeUnit: '1s', duration: __ENV.DURATION || '60s', preAllocatedVUs: 300, maxVUs: 600 } },
  thresholds: { http_req_duration: [`p(99)<${T.p99Ms}`], http_req_failed: ['rate<0.001'], dropped_iterations: [`rate<${T.calculationsPerSec * 0.01}`] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};
export default function () {
  const start = Date.now() - (30 + (__ITER % 600)) * 60000;
  const body = { tariffId: IDS[__ITER % 3], startedAt: new Date(start).toISOString(), stoppedAt: new Date(start + (20 + (__ITER % 120)) * 60000).toISOString(), energyWh: 1000 + (__ITER % 60000) };
  const res = http.post(`${URL}/v1/quote`, JSON.stringify(body), { headers: internalHeaders });
  check(res, { '200': (r) => r.status === 200 });
}
export const handleSummary = summary('tariff');
