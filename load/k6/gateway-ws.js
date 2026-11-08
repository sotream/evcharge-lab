// OCPP over WebSocket against ONE gateway instance (port 28087): connect stations, then send Heartbeat and
// MeterValues and measure CALL -> CALLRESULT latency. ADR 0000: >= 2,000 msg/s per instance, CALLRESULT p99 < 500 ms.
import ws from 'k6/ws';
import encoding from 'k6/encoding';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';
import { TARGETS, stationId, stationSecret, uuid, summary } from './lib.js';

const T = TARGETS.perInstance['station-gateway'];
const URL = __ENV.URL || 'ws://localhost:28087/ocpp';
const SITES = Number(__ENV.SITES || 200);
const PER_SITE = Number(__ENV.STATIONS_PER_SITE || 10);
const MSG_PER_S = Number(__ENV.MSG_PER_S_PER_STATION || 1);
const DURATION_S = Number(__ENV.DURATION_S || 60);
const rtt = new Trend('ocpp_callresult_ms', true);
const results = new Counter('ocpp_callresults');
const errors = new Counter('ocpp_callerrors');
const skipped = new Counter('ocpp_ticks_skipped_outstanding_call');
export const options = {
  scenarios: { stations: { executor: 'per-vu-iterations', vus: SITES * PER_SITE, iterations: 1, maxDuration: `${DURATION_S + 60}s` } },
  thresholds: { ocpp_callresult_ms: [`p(99)<${T.callResultP99Ms}`], ocpp_callresults: [`rate>=${T.msgPerSec}`], ocpp_callerrors: ['count<1'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};
export default function () {
  const g = __VU - 1;
  // Ramp connections at ~250/s, under the gateway's upgrade admission limit (GATEWAY_MAX_UPGRADES_PER_S, ADR 0003).
  sleep(g / 250);
  const id = stationId(1 + Math.floor(g / PER_SITE), 1 + (g % PER_SITE));
  const auth = encoding.b64encode(`${id}:${stationSecret(id)}`);
  const pending = {};
  const res = ws.connect(`${URL}/${id}`, { headers: { Authorization: `Basic ${auth}`, 'Sec-WebSocket-Protocol': 'ocpp1.6' } }, (socket) => {
    const call = (action, payload) => {
      const mid = uuid().slice(0, 36);
      pending[mid] = Date.now();
      socket.send(JSON.stringify([2, mid, action, payload]));
    };
    socket.on('open', () => {
      call('BootNotification', { chargePointVendor: 'k6', chargePointModel: 'load' });
      socket.setInterval(() => {
        // OCPP: one outstanding CALL per direction. A station waits for the CALLRESULT before its next CALL.
        if (Object.keys(pending).length > 0) { skipped.add(1); return; }
        if (Math.random() < 0.5) call('Heartbeat', {});
        else call('MeterValues', { connectorId: 1, meterValue: [{ timestamp: new Date().toISOString(), sampledValue: [{ value: '7000', measurand: 'Power.Active.Import', unit: 'W' }] }] });
      }, 1000 / MSG_PER_S);
      socket.setTimeout(() => socket.close(), DURATION_S * 1000);
    });
    socket.on('message', (raw) => {
      const f = JSON.parse(raw);
      if (f[0] === 3 || f[0] === 4) {
        const t = pending[f[1]];
        if (t !== undefined) { rtt.add(Date.now() - t); delete pending[f[1]]; }
        if (f[0] === 3) results.add(1); else errors.add(1);
      } else if (f[0] === 2) socket.send(JSON.stringify([3, f[1], { status: 'Accepted' }])); // answer CSMS calls
    });
  });
  if (!check(res, { 'upgrade 101': (r) => r && r.status === 101 })) console.warn(`upgrade ${id}: ${res && res.status} ${res && res.error}`);
}
export const handleSummary = summary(`gateway-ws-${SITES * PER_SITE}st-${MSG_PER_S}hz`);
