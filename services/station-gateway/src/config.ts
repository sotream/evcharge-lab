import { hostname } from 'node:os';
import { ocpp } from '@evcharge/contracts';
import { baseConfigSchema, csv, envBool, loadConfig, type Env } from '@evcharge/platform';
import { z } from 'zod';

const int = (def: number, min = 0) => z.coerce.number().int().min(min).default(def);

/** Every variable the gateway reads (ADR 0003, 0004, 0006, 0012). Fails fast listing every bad one. */
export const gatewayConfigSchema = baseConfigSchema.extend({
  SERVICE_NAME: z.string().min(1).default('station-gateway'),
  /** Registry identity: route:station:{id}.instance and channel gw:cmd:{instance}. */
  GATEWAY_INSTANCE_ID: z
    .string()
    .regex(/^[A-Za-z0-9._-]{1,64}$/)
    .default(hostname().replace(/[^A-Za-z0-9._-]/g, '-').slice(0, 64)),
  SESSIONS_URL: z.url(),
  KAFKA_BROKERS: csv(),
  REDIS_URL: z.string().min(1),
  /** Token bucket on WebSocket upgrades per second (ADR 0003). */
  GATEWAY_MAX_UPGRADES_PER_S: int(300, 1),
  /** Separate, smaller bucket charged for 401 upgrades so bad credentials cannot drain the budget of good stations. */
  GATEWAY_MAX_BAD_AUTH_PER_S: int(30, 1),
  /** Hard cap of open station sockets per instance (ADR 0003). */
  GATEWAY_MAX_CONNECTIONS: int(15_000, 1),
  /** Records waiting for a broker ack before new CALLs get InternalError (ADR 0012). */
  GATEWAY_PRODUCE_BUFFER: int(20_000, 1),
  /** Raw-log records in flight before they are dropped (ADR 0003). */
  GATEWAY_RAW_LOG_BUFFER: int(5_000, 0),
  GATEWAY_RAW_LOG_ENABLED: envBool().default(true),
  /** Per-station queue of CSMS -> station commands (ADR 0003). */
  GATEWAY_COMMAND_QUEUE: int(32, 1),
  GATEWAY_CALL_TIMEOUT_S: int(ocpp.Timing.CALL_TIMEOUT_S, 1),
  /** Hot-path timeout to sessions; no retry in the gateway (ADR 0003). */
  SESSIONS_TIMEOUT_MS: int(1000, 50),
  CREDENTIAL_LRU_TTL_S: int(30, 0),
  CREDENTIAL_LRU_MAX: int(50_000, 1),
  CREDENTIAL_REDIS_TTL_S: int(300, 1),
  ROUTE_TTL_S: int(90, 3),
  ROUTE_REFRESH_S: int(30, 1),
  INSTANCE_HEARTBEAT_S: int(5, 1),
  /** A route whose instance heartbeat is older than this is dead (ADR 0004). */
  INSTANCE_DEAD_AFTER_S: int(15, 1),
  ALLOCATION_GRACE_S: int(ocpp.ChargingProfiles.ALLOCATION_GRACE_S, 0),
  /** Station timestamps older than this mark MeterValues as replayed (ADR 0003). */
  REPLAY_THRESHOLD_S: int(60, 1),
  /** WebSocket ping sweep; a socket that missed one pong is terminated. */
  WS_PING_INTERVAL_S: int(60, 1),
  /** Max frame size; OCPP payloads stay under 4 KB (ADR 0004), allow headroom for replays. */
  WS_MAX_PAYLOAD_BYTES: int(64 * 1024, 1024),
  KAFKA_LINGER_MS: int(5, 0),
  KAFKA_SEND_TIMEOUT_MS: int(30_000, 1000),
});
export type GatewayConfig = z.infer<typeof gatewayConfigSchema>;

export function loadGatewayConfig(env: Env = process.env): GatewayConfig {
  return loadConfig(gatewayConfigSchema, env);
}
