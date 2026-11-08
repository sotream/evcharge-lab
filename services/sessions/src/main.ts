import { telemetry } from './instrument';
import { ConfigError } from '@evcharge/platform';
import { createSessionsApp } from './app';
import { readConfig } from './config';

async function main(): Promise<void> {
  const config = readConfig();
  await createSessionsApp({ config, telemetry });
}

main().catch((err: unknown) => {
  // Fail fast: a bad configuration or an unreachable dependency at startup must not leave a half-started process.
  process.stderr.write(`${JSON.stringify({ level: 'fatal', service: 'sessions', msg: 'startup failed', err: err instanceof ConfigError ? err.issues : String(err) })}\n`);
  process.exit(1);
});
