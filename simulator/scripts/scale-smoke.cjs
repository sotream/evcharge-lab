// Scale smoke: 10,000+ simulated stations in ONE simulator process against an accept-everything OCPP stub
// that runs in a SEPARATE process (so the stub does not steal the simulator's CPU).
// Usage (after pnpm build): node simulator/scripts/scale-smoke.cjs <stations=10000> <seconds=60> <timeScale=60>
// Output: one JSON line per second on stdout (true per-second values), then a final summary line.
const path = require('node:path').resolve(__dirname, '..');
const { fork } = require('node:child_process');
const { monitorEventLoopDelay } = require('node:perf_hooks');

if (process.argv[2] === '--stub') {
  const { WebSocketServer } = require(`${path}/node_modules/ws`);
  const wss = new WebSocketServer({ port: 0, perMessageDeflate: false, handleProtocols: (p) => (p.has('ocpp1.6') ? 'ocpp1.6' : false) });
  let tx = 1000;
  let received = 0;
  wss.on('connection', (ws) => {
    ws.on('message', (d) => {
      received++;
      const m = JSON.parse(d.toString());
      if (m[0] !== 2) return;
      const [, id, action] = m;
      let res = {};
      if (action === 'BootNotification') res = { status: 'Accepted', currentTime: new Date().toISOString(), interval: 300 };
      else if (action === 'StartTransaction') res = { transactionId: tx++, idTagInfo: { status: 'Accepted' } };
      else if (action === 'Authorize') res = { idTagInfo: { status: 'Accepted' } };
      else if (action === 'Heartbeat') res = { currentTime: new Date().toISOString() };
      ws.send(JSON.stringify([3, id, res]));
    });
  });
  process.send({ port: wss.address().port });
  setInterval(() => process.send({ received }), 1000);
} else {
  const { Simulator, loadSimConfig } = require(`${path}/dist/index.js`);
  const { createLogger } = require(`${path}/node_modules/@evcharge/platform`);
  const stations = Number(process.argv[2] ?? 10000);
  const seconds = Number(process.argv[3] ?? 60);
  const scale = Number(process.argv[4] ?? 60);
  const stub = fork(__filename, ['--stub']);
  let stubReceived = 0;
  stub.on('message', (m) => {
    if (m.received !== undefined) stubReceived = m.received;
    if (m.port !== undefined) run(m.port);
  });

  function run(port) {
    const sim = new Simulator(
      loadSimConfig({
        LOG_LEVEL: 'warn',
        CSMS_URL: `ws://127.0.0.1:${port}/ocpp`,
        STATION_SECRET_SEED: 'scale-smoke-seed',
        SIM_SITES: String(stations / 10),
        SIM_STATIONS_PER_SITE: '10',
        SIM_TIME_SCALE: String(scale),
        SIM_AUTOSTART: 'true',
        SIM_CONNECT_RATE: '2000',
        SIM_UTC_OFFSET_H: '0',
        SIM_START: '2026-11-08T17:00:00Z',
      }),
      createLogger({ service: 'smoke', level: 'warn' }),
    );
    const t0 = Date.now();
    sim.start();
    const lag = monitorEventLoopDelay({ resolution: 10 });
    lag.enable();
    let lastRecv = 0;
    let lastCpu = process.cpuUsage();
    const peak = { msgPerS: 0, rssMB: 0, lagMaxMs: 0 };
    const tick = setInterval(() => {
      const s = sim.status();
      const mem = process.memoryUsage();
      const cpu = process.cpuUsage(lastCpu);
      lastCpu = process.cpuUsage();
      const line = {
        t: Math.round((Date.now() - t0) / 1000),
        connected: s.connected,
        activeSessions: s.activeSessions,
        queue: s.replayQueueDepth,
        msgPerS: stubReceived - lastRecv, // messages the stub received during the last second
        rssMB: Math.round(mem.rss / 1048576),
        heapMB: Math.round(mem.heapUsed / 1048576),
        simCpuPct: Math.round(((cpu.user + cpu.system) / 1e6) * 100),
        loopLagMeanMs: +(lag.mean / 1e6).toFixed(2),
        loopLagP99Ms: +(lag.percentile(99) / 1e6).toFixed(2),
        loopLagMaxMs: +(lag.max / 1e6).toFixed(2),
        simTime: s.simTime,
      };
      lastRecv = stubReceived;
      lag.reset();
      peak.msgPerS = Math.max(peak.msgPerS, line.msgPerS);
      peak.rssMB = Math.max(peak.rssMB, line.rssMB);
      peak.lagMaxMs = Math.max(peak.lagMaxMs, line.loopLagMaxMs);
      process.stdout.write(`${JSON.stringify(line)}\n`);
    }, 1000);
    setTimeout(async () => {
      clearInterval(tick);
      process.stdout.write(`${JSON.stringify({ final: true, stations, scale, seconds, peak, totalMessagesReceivedByStub: stubReceived, stubInSeparateProcess: true })}\n`);
      await sim.stop();
      stub.kill();
      process.exit(0);
    }, seconds * 1000);
  }
}
