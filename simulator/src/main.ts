import { ConfigError, createLogger, initMetrics } from '@evcharge/platform';
import { createApi } from './api';
import { loadSimConfig, type SimConfig } from './config';
import { Simulator } from './simulator';

async function main(): Promise<void> {
  let config: SimConfig;
  try {
    config = loadSimConfig();
  } catch (err) {
    if (err instanceof ConfigError) {
      process.stderr.write(`${err.message}\n`);
      process.exit(2);
    }
    throw err;
  }
  initMetrics({ service: 'simulator' });
  const log = createLogger({ service: 'simulator', level: config.LOG_LEVEL });
  const sim = new Simulator(config, log);
  const state = { shuttingDown: false };
  const server = createApi(sim, state);
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(config.SIM_HTTP_PORT, '0.0.0.0', () => resolve());
  });
  log.info({ port: config.SIM_HTTP_PORT }, 'control API listening');
  sim.start();

  const shutdown = (signal: string) => {
    if (state.shuttingDown) process.exit(1);
    state.shuttingDown = true;
    log.info({ signal }, 'shutting down');
    const timer = setTimeout(() => process.exit(1), 10_000);
    timer.unref();
    void sim
      .stop()
      .then(() => new Promise<void>((r) => server.close(() => r())))
      .then(() => process.exit(0));
    server.closeAllConnections();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

void main().catch((err: unknown) => {
  process.stderr.write(`simulator failed to start: ${err instanceof Error ? (err.stack ?? err.message) : String(err)}\n`);
  process.exit(1);
});
