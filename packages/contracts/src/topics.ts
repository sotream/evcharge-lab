/** Kafka topics (ADR 0002). Partition counts are set by the compose topic-init job. */
export const Topics = {
  /** key = stationId; producer: station-gateway; 12 partitions, 7d retention. */
  StationEvents: 'station.events.v1',
  /** key = stationId; producer: station-gateway; 24 partitions, 24h retention (Timescale is the store). */
  MeterValues: 'telemetry.meter-values.v1',
  /** key = stationId; producers: sessions; 12 partitions, 1h retention (commands expire). */
  StationCommands: 'station.commands.v1',
  /** key = stationId; producer: station-gateway; 12 partitions, 24h retention. */
  StationCommandResults: 'station.command-results.v1',
  /** key = siteId; producer: sessions (outbox); 12 partitions, 7d retention. */
  SessionEvents: 'session.events.v1',
  /** key = sessionId; producer: billing (outbox); 6 partitions, 7d retention. */
  BillingEvents: 'billing.events.v1',
  /** key = stationId; producer: station-gateway; raw OCPP frames both directions (ADR 0003); 12 partitions, 14d retention. */
  OcppRawLog: 'ocpp.raw-log.v1',
} as const;
export type Topic = (typeof Topics)[keyof typeof Topics];

export const TOPIC_SPECS: ReadonlyArray<{ name: Topic; partitions: number; retentionMs: number }> = [
  { name: Topics.StationEvents, partitions: 12, retentionMs: 7 * 864e5 },
  { name: Topics.MeterValues, partitions: 24, retentionMs: 864e5 },
  { name: Topics.StationCommands, partitions: 12, retentionMs: 36e5 },
  { name: Topics.StationCommandResults, partitions: 12, retentionMs: 864e5 },
  { name: Topics.SessionEvents, partitions: 12, retentionMs: 7 * 864e5 },
  { name: Topics.BillingEvents, partitions: 6, retentionMs: 7 * 864e5 },
  { name: Topics.OcppRawLog, partitions: 12, retentionMs: 14 * 864e5 },
];

/** Consumer group ids, one per (service, purpose). */
export const ConsumerGroups = {
  GatewayCommandDispatcher: 'station-gateway.command-dispatcher',
  SessionsStationEvents: 'sessions.station-events',
  SessionsCommandResults: 'sessions.command-results',
  BillingSessionEvents: 'billing.session-events',
  TelemetryIngest: 'telemetry-ingest.meter-values',
  DriverApiBillingEvents: 'driver-api.billing-events',
} as const;

/** Redis key conventions shared across services (ADR 0004). */
export const RedisKeys = {
  /** HASH {instance, connectedAt, epoch}; TTL 90s, refreshed by owning gateway every 30s. */
  stationRoute: (stationId: string) => `route:station:${stationId}`,
  /** Pub/Sub channel a gateway instance listens on for commands to its stations. */
  gatewayCommandChannel: (instance: string) => `gw:cmd:${instance}`,
  /** SET of live gateway instances with heartbeat ZSET score. */
  gatewayInstances: 'gw:instances',
  /** HASH latest power per connector: {w, ts}; written by telemetry-ingest. */
  connectorPower: (stationId: string, connectorId: number) => `power:${stationId}:${connectorId}`,
  /**
   * STRING epoch ms: latest validTo of any allocation profile that may be in force at the site (ADR 0006).
   * Written by sessions before a round commits (only raised); the gateway applies the safe share only
   * when now > value + ALLOCATION_GRACE_S. Missing key: never raise (stations stay at startW).
   */
  siteAllocationValidTo: (siteId: string) => `alloc:site:${siteId}`,
  /** STRING JSON of SessionsInternal.stationCredential; TTL 5m. */
  stationCredential: (stationId: string) => `cred:station:${stationId}`,
} as const;
