import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { registry } from '@evcharge/platform';
import { z } from 'zod';
import type { Simulator } from './simulator';
import { errorMessage } from './util';

const count = z.coerce.number().int().min(0);
const bodies = {
  fleetStart: z.object({ stations: count.optional(), ratePerS: z.coerce.number().min(1).max(100_000).optional() }),
  fleetStop: z.object({ stations: count.optional() }),
  offline: z.object({ stations: count.optional(), ids: z.array(z.string()).optional(), minutes: z.coerce.number().min(0.01).max(1440) }),
  storm: z.object({ stations: count.optional() }),
  flood: z.object({ stations: count.min(1), concurrency: z.coerce.number().int().min(1).max(20_000).optional(), wait: z.boolean().optional() }),
  drivers: z.object({ count: count.min(1).optional(), ratePerS: z.coerce.number().min(0.1).max(1000).optional() }),
  probes: z.object({ count: count.min(0) }),
};

function send(res: ServerResponse, status: number, body: unknown, type = 'application/json'): void {
  res.writeHead(status, { 'content-type': type });
  res.end(type.includes('json') ? JSON.stringify(body) : String(body));
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const c of req) {
    size += (c as Buffer).length;
    if (size > 64 * 1024) throw new HttpError(413, 'body too large');
    chunks.push(c as Buffer);
  }
  if (size === 0) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new HttpError(400, 'invalid JSON');
  }
}

function parse<S extends z.ZodType>(schema: S, body: unknown): z.infer<S> {
  const r = schema.safeParse(body);
  if (!r.success) throw new HttpError(400, r.error.issues.map((i) => `${i.path.join('.') || '(body)'}: ${i.message}`).join('; '));
  return r.data;
}

/**
 * Control and observability API (ADR 0013). Unauthenticated on purpose: it only exists in the lab
 * compose stack on a host port. Modes: offline, storm, flood, drivers, probes.
 */
export function createApi(sim: Simulator, state: { shuttingDown: boolean }): Server {
  const route = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const key = `${req.method} ${url.pathname}`;
    switch (key) {
      case 'GET /health/live':
        return send(res, 200, { status: 'ok' });
      case 'GET /health/ready':
        return state.shuttingDown ? send(res, 503, { status: 'draining' }) : send(res, 200, { status: 'ok' });
      case 'GET /metrics':
        sim.updateGauges();
        return send(res, 200, await registry.metrics(), registry.contentType);
      case 'GET /status':
        return send(res, 200, sim.status());
      case 'POST /fleet/start': {
        const b = parse(bodies.fleetStart, await readJson(req));
        const n = sim.connectFleet(b.stations ?? sim.stations.length, b.ratePerS ?? sim.config.SIM_CONNECT_RATE);
        return send(res, 202, { starting: n });
      }
      case 'POST /fleet/stop': {
        const b = parse(bodies.fleetStop, await readJson(req));
        return send(res, 200, { stopped: sim.disconnectFleet(b.stations ?? sim.stations.length) });
      }
      case 'POST /offline': {
        const b = parse(bodies.offline, await readJson(req));
        const r = sim.offline({ minutes: b.minutes, ...(b.ids ? { ids: b.ids } : {}), ...(b.stations !== undefined ? { stations: b.stations } : {}) });
        return send(res, 202, { stations: r.stations.length, ids: r.stations.slice(0, 50), realSeconds: r.realSeconds, until: new Date(r.untilRealMs).toISOString() });
      }
      case 'POST /storm': {
        const b = parse(bodies.storm, await readJson(req));
        return send(res, 202, { disconnected: sim.storm(b.stations) });
      }
      case 'POST /flood': {
        const b = parse(bodies.flood, await readJson(req));
        const job = sim.flood(b.stations, b.concurrency);
        if (b.wait) {
          // oxlint-disable-next-line no-await-in-loop -- polling a job until it finishes
          while (!job.finishedAt && !state.shuttingDown) await new Promise((r) => setTimeout(r, 100));
          return send(res, 200, job);
        }
        return send(res, 202, job);
      }
      case 'POST /drivers/start': {
        const b = parse(bodies.drivers, await readJson(req));
        if (!sim.startDrivers(b.count ?? Math.max(1, sim.config.SIM_DRIVERS), b.ratePerS)) throw new HttpError(409, 'DRIVER_API_URL is not configured');
        return send(res, 202, { drivers: sim.status().drivers });
      }
      case 'POST /drivers/stop':
        await sim.drivers?.stop();
        return send(res, 200, { drivers: sim.status().drivers });
      case 'POST /probes': {
        const b = parse(bodies.probes, await readJson(req));
        const ids = sim.setProbes(b.count);
        return send(res, 200, { stations: ids, file: sim.probeLog?.path ?? null });
      }
      case 'GET /probes':
        return send(res, 200, { stations: sim.probeStations(), file: sim.probeLog?.path ?? null });
      default:
        throw new HttpError(404, `no route ${key}`);
    }
  };

  return createServer((req, res) => {
    route(req, res).catch((err: unknown) => {
      const status = err instanceof HttpError ? err.status : 500;
      if (!res.headersSent) send(res, status, { type: 'about:blank', title: status === 500 ? 'Internal Server Error' : 'Bad Request', status, detail: errorMessage(err) }, 'application/problem+json');
      else res.end();
    });
  });
}
