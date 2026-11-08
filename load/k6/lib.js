// Shared helpers for k6 scripts. Targets come from load/targets.json (transcribed from ADR 0000).
import crypto from 'k6/crypto';

export const TARGETS = JSON.parse(open('../targets.json'));
export const TOKEN = __ENV.INTERNAL_TOKEN || 'dev-internal-token-change-me';
export const SEED = __ENV.STATION_SECRET_SEED || 'dev-station-seed-change-me';
export const internalHeaders = { 'content-type': 'application/json', 'x-internal-token': TOKEN };

export const pad = (n, w) => String(n).padStart(w, '0');
export const stationId = (site, n) => `st-${pad(site, 4)}-${pad(n, 3)}`;
/** ADR 0003 lab secret: HMAC-SHA256(seed, stationId) as hex, first 32 chars. */
export const stationSecret = (id) => crypto.hmac('sha256', SEED, id, 'hex').slice(0, 32);

export function uuid() {
  const b = crypto.randomBytes(16);
  const v = new Uint8Array(b);
  v[6] = (v[6] & 0x0f) | 0x40;
  v[8] = (v[8] & 0x3f) | 0x80;
  const h = Array.from(v, (x) => x.toString(16).padStart(2, '0')).join('');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

/** Writes the k6 summary to docs/load-output/k6-<name>.json (raw, used by the chart script). */
export function summary(name) {
  return (data) => ({ [`../../docs/load-output/k6-${name}.json`]: JSON.stringify(data, null, 2), stdout: textLine(data) });
}
function textLine(data) {
  const d = data.metrics.http_req_duration?.values ?? {};
  const reqs = data.metrics.http_reqs?.values ?? data.metrics.iterations?.values ?? {};
  return `rate=${(reqs.rate ?? 0).toFixed(1)}/s p95=${(d['p(95)'] ?? 0).toFixed(2)}ms p99=${(d['p(99)'] ?? 0).toFixed(2)}ms\n`;
}
