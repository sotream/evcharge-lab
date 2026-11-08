import { telemetry } from './instrument';
import { bootstrapService, ConfigError, createLogger, createPgPool, loadConfig, pgReadinessCheck, registerReadinessCheck, runMigrations } from '@evcharge/platform';
import { AppModule } from './app.module';
import { billingConfigSchema } from './config';
import { createBillingRuntime } from './runtime';

async function main(): Promise<void> {
  let config;
  try {
    config = loadConfig(billingConfigSchema);
  } catch (err) {
    if (err instanceof ConfigError) {
      process.stderr.write(`invalid configuration:\n  ${err.issues.join('\n  ')}\n`);
      process.exit(2);
    }
    throw err;
  }
  const logger = createLogger({ service: config.SERVICE_NAME, level: config.LOG_LEVEL });
  const pool = createPgPool(config.DATABASE_URL, { max: config.DB_POOL_MAX, applicationName: config.SERVICE_NAME, logger });
  await runMigrations(pool, `${__dirname}/../migrations`, { logger });
  registerReadinessCheck('postgres', pgReadinessCheck(pool));

  const rt = createBillingRuntime(config, pool, logger);
  registerReadinessCheck('kafka-consumer', rt.consumer.readinessCheck());
  registerReadinessCheck('kafka-producer', rt.producer.readinessCheck());

  // Hooks run in reverse order: runtime (intake, workers, relay, producer) stops before the pool closes.
  await bootstrapService({
    module: AppModule.register(rt.deps),
    config,
    logger,
    telemetry,
    onShutdown: [
      { name: 'postgres', run: () => pool.end() },
      { name: 'billing-runtime', run: () => rt.stop() },
    ],
  });
  await rt.start();
  logger.info('billing started');
}

main().catch((err: unknown) => {
  process.stderr.write(`${err instanceof Error ? (err.stack ?? err.message) : String(err)}\n`);
  process.exit(1);
});
