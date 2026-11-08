import { telemetry } from './instrument';
import { ConfigError } from '@evcharge/platform';
import { createDriverApi } from './app';
import { readConfig } from './config';

async function main(): Promise<void> {
  let config;
  try {
    config = readConfig();
  } catch (err) {
    if (err instanceof ConfigError) {
      process.stderr.write(`invalid configuration:\n  ${err.issues.join('\n  ')}\n`);
      process.exit(2);
    }
    throw err;
  }
  const app = await createDriverApi({ config, telemetry });
  app.logger.info('driver-api started');
}

main().catch((err: unknown) => {
  process.stderr.write(`${err instanceof Error ? (err.stack ?? err.message) : String(err)}\n`);
  process.exit(1);
});
