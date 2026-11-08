import { telemetry } from './instrument';
// oxlint-disable-next-line import/no-unassigned-import -- Nest decorators need the polyfill loaded first
import 'reflect-metadata';
import { ConfigError, createLogger } from '@evcharge/platform';
import { loadGatewayConfig } from './config';
import { StationGateway } from './gateway';

async function main(): Promise<void> {
  let config;
  try {
    config = loadGatewayConfig();
  } catch (err) {
    if (err instanceof ConfigError) {
      process.stderr.write(`${err.message}\n`);
      process.exit(2);
    }
    throw err;
  }
  const logger = createLogger({ service: config.SERVICE_NAME, level: config.LOG_LEVEL });
  try {
    await StationGateway.start(config, { logger, telemetry });
  } catch (err) {
    logger.fatal({ err: err instanceof Error ? err.message : String(err) }, 'station gateway failed to start');
    logger.flush?.();
    process.exit(1);
  }
}

void main();
