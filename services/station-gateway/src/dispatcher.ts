/* oxlint-disable no-await-in-loop */ // the PUBLISH retry loop is sequential on purpose (ADR 0004)
import { events, RedisKeys } from '@evcharge/contracts';
import { currentTraceparent, makeEnvelope, sleep, type Logger, type Redis } from '@evcharge/platform';
import type { CommandOutcome } from './command-channel';
import type { GatewayMetrics } from './metrics';
import type { EventPublisher, GatewayTopics } from './publisher';
import type { RouteRegistry } from './registry';
import { PRODUCER } from './station-connection';
import { errorMessage } from './util';

export type CommandRequested = typeof events.StationCommandRequested._output;

/** Messages on gw:cmd:{instance}. */
export type ChannelMessage = { type: 'command'; event: CommandRequested; traceparent?: string } | { type: 'evict'; stationId: string; epoch: number };

/** Publishes StationCommandCompleted (key stationId) to station.command-results.v1, retrying a few times. */
export async function publishCommandResult(
  publisher: EventPublisher,
  topics: GatewayTopics,
  logger: Logger,
  cmd: CommandRequested['data'],
  outcome: CommandOutcome,
  response?: Record<string, unknown>,
): Promise<void> {
  const env = makeEnvelope(
    'station.command-completed',
    cmd.stationId,
    { commandId: cmd.commandId, stationId: cmd.stationId, action: cmd.action, outcome, ...(response ? { response } : {}), requestedBy: cmd.requestedBy },
    PRODUCER,
  );
  let lastErr: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      // Same eventId on every attempt: consumers dedupe on it.
      await publisher.publish(topics.commandResults, env);
      return;
    } catch (err) {
      lastErr = err;
      await sleep(200 * 2 ** attempt);
    }
  }
  logger.error({ commandId: cmd.commandId, stationId: cmd.stationId, outcome, err: errorMessage(lastErr) }, 'command result not published');
  throw lastErr;
}

export interface DispatcherOptions {
  /** ADR 0004: retry 3x over 1.5 s when the PUBLISH reached no subscriber. */
  publishRetries?: number;
  publishRetryDelayMs?: number;
}

/**
 * The dispatcher side of ADR 0004: a member of the `station-gateway.command-dispatcher` group
 * on station.commands.v1 looks up the route and forwards the command to the owning instance
 * over Redis pub/sub. Outcomes decided here: Expired, StationOffline.
 */
export class CommandDispatcher {
  constructor(
    private readonly redis: Redis,
    private readonly registry: RouteRegistry,
    private readonly publisher: EventPublisher,
    private readonly topics: GatewayTopics,
    private readonly logger: Logger,
    private readonly metrics: GatewayMetrics,
    private readonly opts: DispatcherOptions = {},
  ) {}

  /** Kafka handler. Throws only when the result itself cannot be published (consumer retries). */
  async handle(event: CommandRequested): Promise<void> {
    const cmd = event.data;
    if (Date.parse(cmd.expiresAt) <= Date.now()) return this.finish(cmd, 'Expired', 'expired');
    const retries = this.opts.publishRetries ?? 3;
    const delay = this.opts.publishRetryDelayMs ?? 500;
    const traceparent = currentTraceparent();
    const message = JSON.stringify({ type: 'command', event, ...(traceparent ? { traceparent } : {}) } satisfies ChannelMessage);
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) await sleep(delay);
      let route;
      try {
        route = await this.registry.lookup(cmd.stationId);
        if (!route) return this.finish(cmd, 'StationOffline', 'no_route');
        if (!(await this.registry.isInstanceAlive(route.instance))) return this.finish(cmd, 'StationOffline', 'dead_instance');
        const receivers = await this.redis.publish(RedisKeys.gatewayCommandChannel(route.instance), message);
        if (receivers > 0) {
          this.metrics.dispatch.inc({ result: 'forwarded' });
          return;
        }
      } catch (err) {
        // ADR 0004: with Redis unavailable, new commands fail as StationOffline.
        this.logger.warn({ commandId: cmd.commandId, stationId: cmd.stationId, err: errorMessage(err) }, 'route lookup failed');
        return this.finish(cmd, 'StationOffline', 'redis_error');
      }
    }
    return this.finish(cmd, 'StationOffline', 'no_subscriber');
  }

  private async finish(cmd: CommandRequested['data'], outcome: CommandOutcome, reason: string): Promise<void> {
    this.metrics.dispatch.inc({ result: reason });
    this.metrics.commands.inc({ action: cmd.action, outcome });
    await publishCommandResult(this.publisher, this.topics, this.logger, cmd, outcome);
  }
}
