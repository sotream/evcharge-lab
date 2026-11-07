import { Controller, type DynamicModule, Get, Inject, Module, ServiceUnavailableException } from '@nestjs/common';
import { errorMessage, withTimeout } from '../util';

/** A readiness check passes when it resolves (to anything but `false`) within its timeout. */
export type ReadinessCheck = () => unknown;

export interface CheckResult {
  status: 'ok' | 'fail';
  durationMs: number;
  error?: string;
}

export interface ReadinessReport {
  status: 'ok' | 'fail';
  draining: boolean;
  checks: Record<string, CheckResult>;
}

export class HealthRegistry {
  private readonly checks = new Map<string, { fn: ReadinessCheck; timeoutMs: number }>();
  private drainingFlag = false;

  /** Registers (or replaces) a named check. Returns a function that unregisters it. */
  register(name: string, fn: ReadinessCheck, opts: { timeoutMs?: number } = {}): () => void {
    const entry = { fn, timeoutMs: opts.timeoutMs ?? 2000 };
    this.checks.set(name, entry);
    return () => {
      if (this.checks.get(name) === entry) this.checks.delete(name);
    };
  }

  /** Once draining, readiness fails regardless of checks (load balancers stop sending traffic). */
  setDraining(draining = true): void {
    this.drainingFlag = draining;
  }

  get draining(): boolean {
    return this.drainingFlag;
  }

  names(): string[] {
    return [...this.checks.keys()];
  }

  /** Runs every check in parallel, each bounded by its own timeout. */
  async check(): Promise<ReadinessReport> {
    const entries = await Promise.all(
      [...this.checks].map(async ([name, { fn, timeoutMs }]): Promise<[string, CheckResult]> => {
        const t0 = performance.now();
        try {
          const r = await withTimeout(Promise.resolve().then(fn), timeoutMs, `check ${name}`);
          const durationMs = Math.round(performance.now() - t0);
          return [name, r === false ? { status: 'fail', durationMs, error: 'check returned false' } : { status: 'ok', durationMs }];
        } catch (err) {
          return [name, { status: 'fail', durationMs: Math.round(performance.now() - t0), error: errorMessage(err) }];
        }
      }),
    );
    const checks = Object.fromEntries(entries);
    const ok = !this.drainingFlag && entries.every(([, r]) => r.status === 'ok');
    return { status: ok ? 'ok' : 'fail', draining: this.drainingFlag, checks };
  }
}

/** Process-wide default registry used by bootstrapService and registerReadinessCheck. */
export const defaultHealth = new HealthRegistry();

/** Registers a readiness check on the default registry. Returns an unregister function. */
export function registerReadinessCheck(name: string, fn: ReadinessCheck, opts: { timeoutMs?: number } = {}): () => void {
  return defaultHealth.register(name, fn, opts);
}

export const HEALTH_REGISTRY = Symbol('HEALTH_REGISTRY');

@Controller('health')
export class HealthController {
  constructor(@Inject(HEALTH_REGISTRY) private readonly registry: HealthRegistry) {}

  /** Liveness: the process is up and the event loop answers. */
  @Get('live')
  live() {
    return { status: 'ok' };
  }

  /** Readiness: 200 when all checks pass and not draining, else 503 with per-check detail. */
  @Get('ready')
  async ready(): Promise<ReadinessReport> {
    const report = await this.registry.check();
    if (report.status !== 'ok') throw new ServiceUnavailableException(report);
    return report;
  }
}

@Module({})
export class HealthModule {
  static forRoot(registry: HealthRegistry = defaultHealth): DynamicModule {
    return {
      module: HealthModule,
      controllers: [HealthController],
      providers: [{ provide: HEALTH_REGISTRY, useValue: registry }],
      exports: [HEALTH_REGISTRY],
    };
  }
}
