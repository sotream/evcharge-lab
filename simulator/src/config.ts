import { envBool, LOG_LEVELS, loadConfig, type Env } from '@evcharge/platform';
import { z } from 'zod';

const int = (def: number, min = 0) => z.coerce.number().int().min(min).default(def);

/** Every variable the simulator reads (ADR 0013). Fails fast listing every bad one. */
export const simConfigSchema = z.object({
  LOG_LEVEL: z.enum(LOG_LEVELS).default('info'),
  /** Station endpoint base, e.g. ws://station-lb:8080/ocpp; the station id is appended. */
  CSMS_URL: z.string().regex(/^wss?:\/\//, 'must start with ws:// or wss://'),
  STATION_SECRET_SEED: z.string().min(8),
  SIM_SITES: int(20, 1),
  SIM_STATIONS_PER_SITE: int(10, 1),
  SIM_DC_SHARE: z.coerce.number().min(0).max(1).default(0.2),
  /** Simulated seconds per real second (1 = real time, 60 = a simulated hour per minute). */
  SIM_TIME_SCALE: z.coerce.number().min(0.01).max(3600).default(1),
  SIM_HTTP_PORT: z.coerce.number().int().min(0).max(65535).default(8080),
  /** Connect the whole fleet at start-up (ramped at SIM_CONNECT_RATE per second). */
  SIM_AUTOSTART: envBool().default(true),
  SIM_CONNECT_RATE: int(200, 1),
  /** After the fleet connected, plug in vehicles at the current utilisation at once, so traffic does not take ~45 min to reach steady state. */
  SIM_WARM_START: envBool().default(true),
  DRIVER_API_URL: z.url().optional(),
  SIM_DRIVERS: int(0),
  SIM_DRIVERS_AUTOSTART: envBool().default(true),
  /** Mean real seconds a driver waits between actions. */
  SIM_DRIVER_THINK_S: z.coerce.number().min(0.1).default(20),
  /** Probe stations: charge continuously and log every reading to SIM_PROBE_DIR (drills). */
  SIM_PROBE_STATIONS: int(0),
  SIM_PROBE_DIR: z.string().min(1).default('/tmp/sim-probes'),
  SIM_SEED: int(1),
  /** Local time offset used by the daily utilisation profile. */
  SIM_UTC_OFFSET_H: z.coerce.number().min(-12).max(14).default(1),
  SIM_START: z.iso.datetime({ offset: true }).optional(),
  /** Offline queue capacity per station (oldest MeterValues dropped beyond it). */
  SIM_QUEUE_MAX: int(30_000, 10),
  /** MeterValues period of a charging connector in simulated seconds (ADR 0000: 30). */
  SIM_METER_INTERVAL_S: z.coerce.number().min(0.1).default(30),
  SIM_CALL_TIMEOUT_S: int(30, 1),
  /** Override the heartbeat period (default: BootNotification.conf.interval = 300 s). */
  SIM_HEARTBEAT_S: z.coerce.number().min(1).optional(),
  /** Optional: read the real site caps from sessions (GET /v1/sites with the internal token); otherwise the cap comes from the shared fleet convention. */
  SESSIONS_URL: z.url().optional(),
  INTERNAL_TOKEN: z.string().optional(),
});
export type SimConfig = z.infer<typeof simConfigSchema>;

export const loadSimConfig = (env: Env = process.env): SimConfig => loadConfig(simConfigSchema, env);
