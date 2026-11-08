// Gateway-side load run: N simulated stations against ONE real station-gateway process (native, real Redis and Kafka,
// stub sessions HTTP server). Four processes: this runner (stub sessions, sampling), the gateway, the simulator.
// Usage (after pnpm build, infra up): node simulator/scripts/gateway-10k.cjs <stations=10000> <seconds=150> <timeScale=20> <out.json>
// Needs TEST_REDIS_URL / TEST_KAFKA_BROKERS (defaults: the compose host ports).
const path = require('node:path').resolve(__dirname, '..');
const { fork } = require('node:child_process');
const { randomBytes, randomUUID } = require('node:crypto');
const { createServer } = require('node:http');
const { writeFileSync } = require('node:fs');
const { monitorEventLoopDelay } = require('node:perf_hooks');
const req = require('node:module').createRequire(`${path}/package.json`);

const REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://localhost:26379';
const BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:29091,localhost:29092,localhost:29093').split(',');
const SEED = 'gateway-10k-seed';
const TOKEN = 'gateway-10k-internal-token';

const mode = process.argv[2];

if (mode === '--gateway') {
  // ---- child 1: the station-gateway under test ----
  const { StationGateway, loadGatewayConfig } = req('@evcharge/station-gateway');
  const { createLogger, registry } = req('@evcharge/platform');
  const topics = JSON.parse(process.env.GW_TOPICS);
  const hist = monitorEventLoopDelay({ resolution: 10 });
  hist.enable();
  (async () => {
    const gw = await StationGateway.start(
      loadGatewayConfig({
        SERVICE_NAME: 'station-gateway',
        LOG_LEVEL: 'warn',
        INTERNAL_TOKEN: TOKEN,
        SESSIONS_URL: process.env.GW_SESSIONS_URL,
        KAFKA_BROKERS: BROKERS.join(','),
        REDIS_URL,
        GATEWAY_INSTANCE_ID: 'gw-load',
        // The default 300 upgrades/s is sized for 167 reconnects/s per instance (ADR 0003); a 10,000 station ramp needs more.
        GATEWAY_MAX_UPGRADES_PER_S: '3000',
      }),
      { port: 0, host: '127.0.0.1', topics, dispatcherGroupId: `${topics.stationEvents}.dispatcher`, dispatcherStartFrom: 'latest', logger: createLogger({ service: 'gw', level: 'warn' }), handleSignals: false, drainDelayMs: 0 },
    );
    process.send({ ready: true, port: new URL(gw.url).port });
    let last = { in: 0, out: 0 };
    let lastCpu = process.cpuUsage();
    setInterval(async () => {
      const json = await registry.getMetricsAsJSON();
      const msgs = json.find((m) => m.name === 'ocpp_messages_total')?.values ?? [];
      const sum = (dir, type) => msgs.filter((v) => v.labels.direction === dir && (!type || v.labels.type === type)).reduce((a, v) => a + v.value, 0);
      const cur = { in: sum('in'), out: sum('out'), callsIn: sum('in', 'call') };
      const mem = process.memoryUsage();
      const cpu = process.cpuUsage(lastCpu);
      lastCpu = process.cpuUsage();
      process.send({
        sample: {
          connections: gw.connections.size,
          rssMB: Math.round(mem.rss / 1048576),
          heapMB: Math.round(mem.heapUsed / 1048576),
          msgInPerS: cur.in - last.in,
          msgOutPerS: cur.out - last.out,
          cpuPct: Math.round(((cpu.user + cpu.system) / 1e6) * 100),
          loopLagMeanMs: +(hist.mean / 1e6).toFixed(2),
          loopLagP99Ms: +(hist.percentile(99) / 1e6).toFixed(2),
          loopLagMaxMs: +(hist.max / 1e6).toFixed(2),
          produceBufferDepth: gw.stats().produceBufferDepth,
        },
      });
      last = cur;
      hist.reset();
    }, 1000);
    process.on('message', async (m) => {
      if (m === 'stop') {
        await gw.shutdown('load run done').catch(() => {});
        process.exit(0);
      }
    });
  })().catch((e) => {
    process.stderr.write(`${e.stack}\n`);
    process.exit(1);
  });
} else if (mode === '--sim') {
  // ---- child 2: the simulator ----
  const { Simulator, loadSimConfig } = require(`${path}/dist/index.js`);
  const { createLogger } = req('@evcharge/platform');
  const sim = new Simulator(
    loadSimConfig({
      LOG_LEVEL: 'warn',
      CSMS_URL: `ws://127.0.0.1:${process.env.GW_PORT}/ocpp`,
      STATION_SECRET_SEED: SEED,
      SIM_SITES: String(Number(process.env.SIM_STATIONS) / 10),
      SIM_STATIONS_PER_SITE: '10',
      SIM_TIME_SCALE: process.env.SIM_SCALE,
      SIM_AUTOSTART: 'true',
      SIM_CONNECT_RATE: '1000',
      SIM_UTC_OFFSET_H: '0',
      SIM_START: '2026-11-08T18:00:00Z', // evening peak
    }),
    createLogger({ service: 'sim', level: 'warn' }),
  );
  sim.start();
  setInterval(() => process.send({ sim: sim.status() }), 1000);
  process.on('message', async (m) => {
    if (m === 'stop') {
      await sim.stop();
      process.exit(0);
    }
  });
} else {
  // ---- runner ----
  const stations = Number(process.argv[2] ?? 10000);
  const seconds = Number(process.argv[3] ?? 150);
  const scale = Number(process.argv[4] ?? 20);
  const out = process.argv[5] ?? 'gateway-10k-connections.json';
  const { Admin } = req('@platformatic/kafka');
  const { fleet } = req('@evcharge/contracts');

  const run = `g10k-${Date.now().toString(36)}-${randomBytes(2).toString('hex')}`;
  const topics = { stationEvents: `${run}.station-events`, meterValues: `${run}.meter-values`, commands: `${run}.commands`, commandResults: `${run}.command-results`, rawLog: `${run}.raw-log` };

  function sendJson(rs, status, body) {
    rs.writeHead(status, { 'content-type': 'application/json' });
    rs.end(JSON.stringify(body));
  }
  const sessionsServer = createServer((rq, rs) => {
    rq.resume();
    rq.on('end', () => {
      const m = /^\/internal\/stations\/([^/]+)\/credential$/.exec(rq.url);
      if (m) {
        const id = decodeURIComponent(m[1]);
        const mm = /^st-(\d{4})-\d{3}$/.exec(id);
        if (!mm) return sendJson(rs, 404, {});
        return sendJson(rs, 200, { stationId: id, siteId: `site-${mm[1]}`, secretSha256: fleet.stationSecretSha256(SEED, id), enabled: true, chargingDefaults: { startW: 4140, safeShareW: 11000 } });
      }
      if (rq.url === '/internal/ocpp/authorize') return sendJson(rs, 200, { idTagInfo: { status: 'Accepted' } });
      if (rq.url === '/internal/ocpp/start-transaction') return sendJson(rs, 200, { transactionId: ++txSeq, sessionId: randomUUID(), idTagInfo: { status: 'Accepted' } });
      if (rq.url === '/internal/ocpp/stop-transaction') return sendJson(rs, 200, { sessionId: null, idTagInfo: { status: 'Accepted' } });
      sendJson(rs, 404, {});
    });
  });
  let txSeq = 1000;

  (async () => {
    await new Promise((r) => sessionsServer.listen(0, '127.0.0.1', r));
    const admin = new Admin({ clientId: run, bootstrapBrokers: BROKERS });
    const names = [...Object.values(topics), `${topics.commands}.dlq`];
    await admin.createTopics({ topics: names, partitions: 12, replicas: 3 });

    const gwProc = fork(__filename, ['--gateway'], { env: { ...process.env, GW_TOPICS: JSON.stringify(topics), GW_SESSIONS_URL: `http://127.0.0.1:${sessionsServer.address().port}` } });
    const gwPort = await new Promise((resolve, reject) => {
      gwProc.once('exit', (c) => reject(new Error(`gateway exited ${c}`)));
      gwProc.on('message', (m) => m.ready && resolve(m.port));
    });
    const simProc = fork(__filename, ['--sim'], { env: { ...process.env, GW_PORT: gwPort, SIM_STATIONS: String(stations), SIM_SCALE: String(scale) } });

    const samples = [];
    let gwSample;
    let simStatus;
    gwProc.on('message', (m) => m.sample && (gwSample = m.sample));
    simProc.on('message', (m) => m.sim && (simStatus = m.sim));
    const t0 = Date.now();
    const tick = setInterval(() => {
      if (!gwSample) return;
      const line = { t: Math.round((Date.now() - t0) / 1000), ...gwSample, simConnected: simStatus?.connected ?? 0, simActiveSessions: simStatus?.activeSessions ?? 0 };
      samples.push(line);
      process.stdout.write(`${JSON.stringify(line)}\n`);
    }, 1000);

    await new Promise((r) => setTimeout(r, seconds * 1000));
    clearInterval(tick);
    simProc.send('stop');
    gwProc.send('stop');
    await new Promise((r) => setTimeout(r, 4000));
    await admin.deleteTopics({ topics: names }).catch(() => {});
    await admin.close();
    sessionsServer.close();

    const peak = (k) => Math.max(...samples.map((s) => s[k]));
    const tail = samples.slice(-Math.min(30, samples.length));
    const avg = (k) => Math.round(tail.reduce((a, s) => a + s[k], 0) / tail.length);
    const doc = {
      label: 'measured',
      what: 'One real station-gateway process, driven by simulated stations. Raw per-second samples are in `samples`.',
      measuredAt: new Date().toISOString(),
      setup: {
        gateway: 'ONE native node process (no docker), real Redis and Kafka (3 brokers, RF 3, acks=all) from compose host ports, raw-log publishing ON, GATEWAY_MAX_UPGRADES_PER_S=3000 (default 300 is sized for 167 reconnects/s), other settings default',
        sessions: 'stub HTTP server in the runner process (instant answers, no database)',
        simulator: `separate process: ${stations} stations (${stations / 10} sites x 10), SIM_TIME_SCALE=${scale}, SIM_START 18:00 UTC (evening peak), connect ramp 1000/s`,
        machine: `${require('node:os').cpus().length} logical CPUs, ${require('node:os').platform()} ${require('node:os').arch()}; gateway, simulator, runner, Redis and Kafka all share this one machine`,
        durationS: seconds,
        topics: 'unique per run, created with 12 partitions RF 3, deleted afterwards',
      },
      targets: {
        'ADR 0000 connections per instance': '>= 10,000',
        'ADR 0000 inbound msg/s per instance': '>= 2,000',
        'ADR 0004 RSS at 10,000 connections': '< 1.5 GB',
      },
      summary: {
        peakConnections: peak('connections'),
        peakRssMB: peak('rssMB'),
        peakMsgInPerS: peak('msgInPerS'),
        peakMsgOutPerS: peak('msgOutPerS'),
        last30sAvgMsgInPerS: avg('msgInPerS'),
        last30sAvgMsgOutPerS: avg('msgOutPerS'),
        last30sAvgCpuPct: avg('cpuPct'),
        peakLoopLagP99Ms: peak('loopLagP99Ms'),
        peakLoopLagMaxMs: peak('loopLagMaxMs'),
        peakProduceBufferDepth: peak('produceBufferDepth'),
      },
      notes: 'loopLag* come from perf_hooks monitorEventLoopDelay with a 10 ms resolution, so an idle loop reads about 10 ms. msgIn/msgOut count OCPP frames (all types) from the gateway ocpp_messages_total counter.',
      samples,
    };
    writeFileSync(out, `${JSON.stringify(doc, null, 2)}\n`);
    process.stdout.write(`${JSON.stringify({ final: true, summary: doc.summary })}\n`);
    process.exit(0);
  })().catch((e) => {
    process.stderr.write(`${e.stack}\n`);
    process.exit(1);
  });
}
