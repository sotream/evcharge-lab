import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import { context as otelContext, SpanKind } from '@opentelemetry/api';
import { ConsumerGroups, events, ocpp, RedisKeys } from '@evcharge/contracts';
import {
  bootstrapService,
  closeRedis,
  createConsumer,
  createLogger,
  createProducer,
  extractTraceContext,
  HealthRegistry,
  inSpan,
  makeEnvelope,
  type KafkaConsumer,
  type KafkaProducer,
  type Logger,
  type Redis,
  type RunningService,
  type Telemetry,
  createRedis,
} from '@evcharge/platform';
import { WebSocketServer, type WebSocket } from 'ws';
import { GatewayModule, type GatewayIntrospection } from './app.module';
import { AllocationTracker, safeShareProfile } from './charging';
import type { GatewayConfig } from './config';
import { CredentialStore, parseBasicAuth, verifySecret } from './credentials';
import { CommandDispatcher, publishCommandResult, type ChannelMessage, type DispatcherOptions } from './dispatcher';
import { gatewayMetrics, type GatewayMetrics } from './metrics';
import { defaultTopics, EventPublisher, type GatewayTopics } from './publisher';
import { RouteRegistry } from './registry';
import { SessionsClient, type StationCredential } from './sessions-client';
import { PRODUCER, StationConnection, type ConnectionContext } from './station-connection';
import { errorMessage, randInt, TokenBucket } from './util';

export interface GatewayOptions {
  /** Default config.HTTP_PORT; 0 picks a free port (tests). */
  port?: number;
  host?: string;
  topics?: Partial<GatewayTopics>;
  dispatcherGroupId?: string;
  /** Where a fresh dispatcher group starts. Default 'latest' (stale commands would only expire). */
  dispatcherStartFrom?: 'earliest' | 'latest';
  dispatcher?: DispatcherOptions;
  logger?: Logger;
  telemetry?: Telemetry;
  handleSignals?: boolean;
  drainDelayMs?: number;
  /** Allocation-expiry sweep period. Default 1000 ms. */
  allocationSweepMs?: number;
}

const STATION_ID = /^[A-Za-z0-9._-]{1,48}$/;
const PATH = /^\/ocpp\/([^/?#]+)\/?$/;

const STATUS_TEXT: Record<number, string> = { 400: 'Bad Request', 401: 'Unauthorized', 404: 'Not Found', 503: 'Service Unavailable' };

/**
 * The station gateway: one instance owns a set of station WebSockets, publishes their messages,
 * answers their CALLs, and routes commands to them (ADR 0003, 0004, 0006, 0012).
 */
export class StationGateway implements GatewayIntrospection {
  readonly instance: string;
  readonly metrics: GatewayMetrics;
  readonly topics: GatewayTopics;
  readonly connections = new Map<string, StationConnection>();
  readonly allocations = new AllocationTracker();
  readonly health = new HealthRegistry();
  service!: RunningService;

  private readonly logger: Logger;
  private readonly redis: Redis;
  private readonly sub: Redis;
  private readonly producer: KafkaProducer;
  private readonly rawProducer: KafkaProducer | undefined;
  private readonly publisher: EventPublisher;
  private readonly sessions: SessionsClient;
  private readonly credentials: CredentialStore;
  private readonly registry: RouteRegistry;
  private readonly dispatcher: CommandDispatcher;
  private readonly consumer: KafkaConsumer;
  private readonly bucket: TokenBucket;
  private readonly badAuthBucket: TokenBucket;
  private readonly wss: WebSocketServer;
  private readonly ctx: ConnectionContext;
  private readonly timers: NodeJS.Timeout[] = [];
  private pendingUpgrades = 0;
  private stopping = false;

  private constructor(
    readonly config: GatewayConfig,
    private readonly opts: GatewayOptions,
  ) {
    this.instance = config.GATEWAY_INSTANCE_ID;
    this.logger = (opts.logger ?? createLogger({ service: config.SERVICE_NAME, level: config.LOG_LEVEL })).child({ gateway: this.instance });
    this.metrics = gatewayMetrics();
    this.topics = { ...defaultTopics(), ...opts.topics };
    this.redis = createRedis(config.REDIS_URL, { logger: this.logger });
    this.sub = createRedis(config.REDIS_URL, { logger: this.logger, commandTimeoutMs: 5000 });
    const clientId = `${config.SERVICE_NAME}-${this.instance}`;
    this.producer = createProducer({ brokers: config.KAFKA_BROKERS, clientId, lingerMs: config.KAFKA_LINGER_MS, sendTimeoutMs: config.KAFKA_SEND_TIMEOUT_MS });
    this.rawProducer = config.GATEWAY_RAW_LOG_ENABLED
      ? createProducer({ brokers: config.KAFKA_BROKERS, clientId: `${clientId}-raw`, lingerMs: 20, maxBatchSize: 1000, sendTimeoutMs: 10_000 })
      : undefined;
    this.publisher = new EventPublisher(this.producer, this.rawProducer, {
      instance: this.instance,
      capacity: config.GATEWAY_PRODUCE_BUFFER,
      rawCapacity: config.GATEWAY_RAW_LOG_BUFFER,
      rawEnabled: config.GATEWAY_RAW_LOG_ENABLED,
      topics: this.topics,
      logger: this.logger,
      metrics: this.metrics,
    });
    this.sessions = new SessionsClient({ baseUrl: config.SESSIONS_URL, internalToken: config.INTERNAL_TOKEN, timeoutMs: config.SESSIONS_TIMEOUT_MS });
    this.credentials = new CredentialStore(
      this.redis,
      this.sessions,
      { lruTtlMs: config.CREDENTIAL_LRU_TTL_S * 1000, lruMax: config.CREDENTIAL_LRU_MAX, redisTtlS: config.CREDENTIAL_REDIS_TTL_S },
      this.logger,
      this.metrics,
    );
    this.registry = new RouteRegistry(this.redis, this.instance, { ttlS: config.ROUTE_TTL_S, deadAfterS: config.INSTANCE_DEAD_AFTER_S });
    this.dispatcher = new CommandDispatcher(this.redis, this.registry, this.publisher, this.topics, this.logger, this.metrics, opts.dispatcher);
    this.consumer = createConsumer({
      brokers: config.KAFKA_BROKERS,
      groupId: opts.dispatcherGroupId ?? ConsumerGroups.GatewayCommandDispatcher,
      clientId: `${clientId}-dispatcher`,
      topics: [this.topics.commands],
      schema: events.StationCommandRequested,
      handler: (event) => this.dispatcher.handle(event),
      concurrency: 12,
      startFrom: opts.dispatcherStartFrom ?? 'latest',
      dlqProducer: this.producer,
      logger: this.logger,
    });
    this.bucket = new TokenBucket(config.GATEWAY_MAX_UPGRADES_PER_S);
    this.badAuthBucket = new TokenBucket(config.GATEWAY_MAX_BAD_AUTH_PER_S);
    this.wss = new WebSocketServer({
      noServer: true,
      clientTracking: false,
      perMessageDeflate: false,
      maxPayload: config.WS_MAX_PAYLOAD_BYTES,
      handleProtocols: (protocols) => (protocols.has(ocpp.OCPP_SUBPROTOCOL) ? ocpp.OCPP_SUBPROTOCOL : false),
    });
    this.ctx = {
      instance: this.instance,
      publisher: this.publisher,
      sessions: this.sessions,
      topics: this.topics,
      metrics: this.metrics,
      logger: this.logger,
      allocations: this.allocations,
      commandQueue: config.GATEWAY_COMMAND_QUEUE,
      callTimeoutMs: config.GATEWAY_CALL_TIMEOUT_S * 1000,
      replayThresholdS: config.REPLAY_THRESHOLD_S,
    };
  }

  static async start(config: GatewayConfig, opts: GatewayOptions = {}): Promise<StationGateway> {
    const gw = new StationGateway(config, opts);
    await gw.init();
    return gw;
  }

  get url(): string {
    return this.service.url;
  }

  get wsUrl(): string {
    return `${this.service.url.replace(/^http/, 'ws')}/ocpp`;
  }

  shutdown(reason = 'shutdown') {
    return this.service.shutdown(reason);
  }

  // ------------------------------------------------------------------ introspection

  connectionInfo(stationId: string) {
    const c = this.connections.get(stationId);
    return c ? { stationId, epoch: c.epoch, connectedAt: c.connectedAt, pendingCommands: c.channel.pending } : undefined;
  }

  stats() {
    return {
      instance: this.instance,
      connections: this.connections.size,
      produceBufferDepth: this.publisher.depth,
      trackedAllocations: this.allocations.size,
      draining: this.health.draining,
    };
  }

  // ------------------------------------------------------------------ startup / shutdown

  private async init(): Promise<void> {
    const { config, logger } = this;
    // Readiness reflects only draining and capacity: with Kafka or Redis down the gateway keeps
    // its sockets and degrades per ADR 0004/0012, so HAProxy must not drop every backend at once.
    this.health.register('capacity', () => this.connections.size < config.GATEWAY_MAX_CONNECTIONS);

    await this.registry.heartbeat().catch((err: unknown) => logger.warn({ err: errorMessage(err) }, 'initial gateway heartbeat failed'));
    const channel = RedisKeys.gatewayCommandChannel(this.instance);
    this.sub.on('message', (_ch: string, msg: string) => this.onChannelMessage(msg));
    await this.sub.subscribe(channel);
    await this.consumer.start();

    this.service = await bootstrapService({
      module: GatewayModule.forRoot(this),
      config,
      logger,
      health: this.health,
      ...(this.opts.port !== undefined ? { port: this.opts.port } : {}),
      ...(this.opts.host !== undefined ? { host: this.opts.host } : {}),
      ...(this.opts.telemetry ? { telemetry: this.opts.telemetry } : {}),
      ...(this.opts.handleSignals !== undefined ? { handleSignals: this.opts.handleSignals } : {}),
      ...(this.opts.drainDelayMs !== undefined ? { drainDelayMs: this.opts.drainDelayMs } : {}),
      // Registered order; they run in reverse: timers, consumer, instance, subscriber, producers, redis.
      onShutdown: [
        { name: 'redis', run: () => closeRedis(this.redis) },
        { name: 'kafka-producer', run: () => this.producer.close() },
        { name: 'kafka-raw-producer', run: () => this.rawProducer?.close() },
        { name: 'redis-subscriber', run: () => closeRedis(this.sub) },
        { name: 'registry', run: () => this.registry.removeInstance().catch(() => {}) },
        { name: 'dispatcher', run: () => this.consumer.stop() },
        { name: 'timers', run: () => this.timers.forEach((t) => clearInterval(t)) },
      ],
    });
    const svc = this.service;
    // Sockets must close (1012) before the HTTP server: upgraded sockets keep server.close() waiting.
    svc.lifecycle.setHttpCloser(async () => {
      this.stopping = true;
      await this.closeAllSockets();
      await svc.app.close();
    });
    svc.fastify.server.on('upgrade', (req: IncomingMessage, socket: Duplex, head: Buffer) => this.onUpgrade(req, socket, head));

    this.every(config.INSTANCE_HEARTBEAT_S * 1000, () => this.registry.heartbeat());
    this.every(config.ROUTE_REFRESH_S * 1000, () => this.refreshRoutes());
    this.every(config.WS_PING_INTERVAL_S * 1000, () => this.pingSweep());
    this.every(this.opts.allocationSweepMs ?? 1000, () => this.allocationSweep());
    logger.info({ instance: this.instance, url: svc.url }, 'station gateway ready');
  }

  private every(ms: number, fn: () => unknown): void {
    const t = setInterval(() => {
      Promise.resolve()
        .then(fn)
        .catch((err: unknown) => this.logger.warn({ err: errorMessage(err) }, 'periodic task failed'));
    }, ms);
    t.unref();
    this.timers.push(t);
  }

  private async closeAllSockets(): Promise<void> {
    const conns = [...this.connections.values()];
    this.logger.info({ connections: conns.length }, 'closing station sockets with 1012');
    for (const c of conns) c.close(1012, 'service restart');
    const deadline = Date.now() + 5000;
    // oxlint-disable-next-line no-await-in-loop -- polling until the close handshakes finish
    while (this.connections.size > 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 25));
    for (const c of this.connections.values()) c.ws.terminate();
    await new Promise((r) => setTimeout(r, 25));
  }

  // ------------------------------------------------------------------ upgrade / admission

  private onUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void {
    socket.on('error', () => {});
    const reject = (status: number, reason: string, headers: Record<string, string> = {}) => {
      this.metrics.upgrades.inc({ outcome: 'rejected', reason });
      writeHttpError(socket, status, headers);
    };
    if (this.stopping || this.health.draining) return reject(503, 'draining', retryAfter());

    const m = PATH.exec((req.url ?? '').split('?')[0] ?? '');
    let stationId = '';
    try {
      stationId = m?.[1] ? decodeURIComponent(m[1]) : '';
    } catch {
      stationId = '';
    }
    if (!STATION_ID.test(stationId)) return reject(404, 'bad_path');

    const protocols = String(req.headers['sec-websocket-protocol'] ?? '')
      .split(',')
      .map((p) => p.trim());
    if (!protocols.includes(ocpp.OCPP_SUBPROTOCOL)) return reject(400, 'no_subprotocol');

    if (this.connections.size + this.pendingUpgrades >= this.config.GATEWAY_MAX_CONNECTIONS) return reject(503, 'at_capacity', retryAfter());

    const auth = parseBasicAuth(req.headers.authorization);
    if (!auth || auth.username !== stationId) return this.rejectUnauthorized(reject);

    this.pendingUpgrades++;
    void this.authenticate(req, socket, head, stationId, auth.password, reject, retryAfter).finally(() => this.pendingUpgrades--);
  }

  /** 401 draws on its own, smaller bucket; when that is empty the attacker gets 503 and the good-station budget is untouched. */
  private rejectUnauthorized(reject: (status: number, reason: string, headers?: Record<string, string>) => void): void {
    if (!this.badAuthBucket.tryTake()) return reject(503, 'bad_auth_limited', retryAfter());
    reject(401, 'unauthorized', { 'WWW-Authenticate': 'Basic realm="ocpp"' });
  }

  private async authenticate(
    req: IncomingMessage,
    socket: Duplex,
    head: Buffer,
    stationId: string,
    password: string,
    reject: (status: number, reason: string, headers?: Record<string, string>) => void,
    retryAfter: () => Record<string, string>,
  ): Promise<void> {
    let cred: StationCredential | null;
    try {
      cred = await this.credentials.get(stationId);
    } catch (err) {
      this.logger.warn({ stationId, err: errorMessage(err) }, 'credential lookup failed');
      return reject(503, 'backend_unavailable', retryAfter());
    }
    if (!cred || !cred.enabled || !verifySecret(password, cred.secretSha256)) return this.rejectUnauthorized(reject);
    // The main budget is charged only for stations that authenticated, so bad credentials cannot drain it.
    if (!this.bucket.tryTake()) return reject(503, 'rate_limited', retryAfter());
    if (socket.destroyed) {
      this.metrics.upgrades.inc({ outcome: 'rejected', reason: 'client_gone' });
      return;
    }
    if (this.stopping) return reject(503, 'draining', retryAfter());
    const accepted = cred;
    this.wss.handleUpgrade(req, socket, head, (ws) => this.onConnection(ws, stationId, accepted, req));
  }

  private onConnection(ws: WebSocket, stationId: string, cred: StationCredential, req: IncomingMessage): void {
    this.metrics.upgrades.inc({ outcome: 'accepted', reason: 'ok' });
    const conn = new StationConnection(stationId, ws, cred, this.ctx);
    const previous = this.connections.get(stationId);
    this.connections.set(stationId, conn);
    this.metrics.connected.set({ gateway: this.instance }, this.connections.size);
    if (previous) {
      this.metrics.replaced.inc({ where: 'local' });
      previous.close(4000, 'replaced by a newer connection');
    }

    ws.on('message', (data, isBinary) => {
      conn.alive = true;
      conn.onMessage(data, isBinary);
    });
    ws.on('pong', () => (conn.alive = true));
    ws.on('error', (err) => this.logger.debug({ stationId, err: err.message }, 'socket error'));
    ws.on('close', (code, reason) => this.onClose(conn, code, reason.toString('utf8')));

    const remoteAddr = req.socket.remoteAddress;
    this.publisher.publishQuietly(
      this.topics.stationEvents,
      makeEnvelope('station.connected', stationId, { stationId, gatewayInstance: this.instance, ...(remoteAddr ? { remoteAddr } : {}) }, PRODUCER),
    );
    void this.registerRoute(stationId, conn);
    this.logger.debug({ stationId }, 'station connected');
  }

  private async registerRoute(stationId: string, conn: StationConnection): Promise<void> {
    try {
      const { epoch, previousInstance } = await this.registry.register(stationId, conn.connectedAt);
      if (conn.closed) {
        await this.registry.unregister(stationId, epoch);
        return;
      }
      conn.epoch = epoch;
      if (previousInstance && previousInstance !== this.instance) {
        const msg: ChannelMessage = { type: 'evict', stationId, epoch };
        await this.redis.publish(RedisKeys.gatewayCommandChannel(previousInstance), JSON.stringify(msg));
      }
    } catch (err) {
      this.logger.warn({ stationId, err: errorMessage(err) }, 'route registration failed; the refresh loop retries');
    }
  }

  private onClose(conn: StationConnection, code: number, reason: string): void {
    conn.onClosed();
    const { stationId } = conn;
    if (this.connections.get(stationId) === conn) this.connections.delete(stationId);
    this.metrics.connected.set({ gateway: this.instance }, this.connections.size);
    if (conn.epoch > 0) this.registry.unregister(stationId, conn.epoch).catch((err: unknown) => this.logger.warn({ stationId, err: errorMessage(err) }, 'route unregister failed'));
    this.publisher.publishQuietly(this.topics.stationEvents, makeEnvelope('station.disconnected', stationId, { stationId, gatewayInstance: this.instance, code, reason }, PRODUCER));
    this.logger.debug({ stationId, code, reason }, 'station disconnected');
  }

  // ------------------------------------------------------------------ command routing (owning side)

  private onChannelMessage(text: string): void {
    let msg: ChannelMessage;
    try {
      msg = JSON.parse(text) as ChannelMessage;
    } catch {
      this.logger.warn('unparseable command channel message');
      return;
    }
    if (msg.type === 'evict') {
      const c = this.connections.get(msg.stationId);
      if (c && c.epoch > 0 && c.epoch < msg.epoch) {
        this.metrics.replaced.inc({ where: 'remote' });
        c.close(4000, 'replaced by a newer connection');
      }
      return;
    }
    const parsed = events.StationCommandRequested.safeParse(msg.event);
    if (!parsed.success) {
      this.logger.warn({ err: parsed.error.issues[0]?.message }, 'invalid command on channel');
      return;
    }
    const cmd = parsed.data.data;
    const parent = msg.traceparent ? extractTraceContext({ traceparent: msg.traceparent }) : otelContext.active();
    void inSpan(`command ${cmd.action}`, { kind: SpanKind.CONSUMER, parent, attributes: { 'ocpp.station_id': cmd.stationId, 'command.id': cmd.commandId } }, async () => {
      const finish = (outcome: Parameters<typeof publishCommandResult>[4], response?: Record<string, unknown>) =>
        publishCommandResult(this.publisher, this.topics, this.logger, cmd, outcome, response).catch(() => {});
      const conn = this.connections.get(cmd.stationId);
      if (!conn) {
        this.metrics.commands.inc({ action: cmd.action, outcome: 'StationOffline' });
        return finish('StationOffline');
      }
      const valid = ocpp.CsmsToStation[cmd.action].req.safeParse(cmd.payload);
      if (!valid.success) {
        this.metrics.commands.inc({ action: cmd.action, outcome: 'Error' });
        return finish('Error', { reason: `invalid ${cmd.action} payload: ${valid.error.issues[0]?.message ?? ''}` });
      }
      await new Promise<void>((resolve) => conn.command(cmd.action, cmd.payload, (outcome, response) => void finish(outcome, response).then(resolve), Date.parse(cmd.expiresAt)));
    });
  }

  // ------------------------------------------------------------------ periodic tasks

  private async refreshRoutes(): Promise<void> {
    const owned = [...this.connections.values()].map((c) => ({ stationId: c.stationId, epoch: c.epoch, connectedAt: c.connectedAt }));
    if (owned.length === 0) return;
    const r = await this.registry.refresh(owned);
    this.metrics.routeRefresh.inc({ result: 'refreshed' }, r.refreshed);
    this.metrics.routeRefresh.inc({ result: 'recreated' }, r.recreated);
    this.metrics.routeRefresh.inc({ result: 'failed' }, r.failed);
    this.metrics.routeRefresh.inc({ result: 'lost' }, r.lost.length);
    for (const stationId of r.lost) {
      this.metrics.replaced.inc({ where: 'refresh' });
      this.connections.get(stationId)?.close(4000, 'route owned by another connection');
    }
  }

  private pingSweep(): void {
    for (const c of this.connections.values()) {
      if (!c.alive) {
        c.ws.terminate();
        continue;
      }
      c.alive = false;
      c.ws.ping();
    }
  }

  /**
   * ADR 0006 safe share: raise a connector to safeShareW only when the WHOLE site is stale, i.e. now is
   * past the site's newest allocation validTo plus the grace. A missing key or a failed Redis read means
   * "do not raise": the station stays at startW, which is always safe.
   */
  private async allocationSweep(): Promise<void> {
    const graceMs = this.config.ALLOCATION_GRACE_S * 1000;
    const now = Date.now();
    const staleBySite = new Map<string, boolean>();
    for (const { stationId, connectorId } of this.allocations.expired(now, graceMs, SAFE_SHARE_RETRY_MS)) {
      const c = this.connections.get(stationId);
      if (!c) continue;
      const siteId = c.credential.siteId;
      let stale = staleBySite.get(siteId);
      if (stale === undefined) {
        // oxlint-disable-next-line no-await-in-loop -- one Redis read per site, cached for the rest of the sweep
        stale = await this.siteIsStale(siteId, now, graceMs);
        staleBySite.set(siteId, stale);
      }
      if (!stale) continue;
      this.allocations.attempted(stationId, connectorId, now);
      const req = safeShareProfile(connectorId, c.credential.chargingDefaults.safeShareW);
      this.logger.info({ stationId, connectorId, siteId, safeShareW: c.credential.chargingDefaults.safeShareW }, 'site allocation stale; applying safe share');
      c.command('SetChargingProfile', req as unknown as Record<string, unknown>, (outcome) => this.metrics.safeShare.inc({ outcome }));
    }
  }

  private async siteIsStale(siteId: string, nowMs: number, graceMs: number): Promise<boolean> {
    try {
      const raw = await this.redis.get(RedisKeys.siteAllocationValidTo(siteId));
      const validTo = raw === null ? Number.NaN : Number(raw);
      return Number.isFinite(validTo) && nowMs > validTo + graceMs;
    } catch (err) {
      this.logger.warn({ siteId, err: errorMessage(err) }, 'site allocation key unreadable; not raising to safe share');
      return false;
    }
  }
}

/** A safe share the station did not accept is retried at this period. */
const SAFE_SHARE_RETRY_MS = 30_000;

const retryAfter = (): Record<string, string> => ({ 'Retry-After': String(randInt(1, 5)) });

/** Raw HTTP error on a not-yet-upgraded socket, then close it. */
function writeHttpError(socket: Duplex, status: number, headers: Record<string, string>): void {
  if (socket.destroyed) return;
  const lines = [`HTTP/1.1 ${status} ${STATUS_TEXT[status] ?? 'Error'}`, 'Connection: close', 'Content-Length: 0', ...Object.entries(headers).map(([k, v]) => `${k}: ${v}`)];
  socket.end(`${lines.join('\r\n')}\r\n\r\n`);
  setTimeout(() => socket.destroy(), 1000).unref();
}

