import { api } from '@evcharge/contracts';
import type { HttpResponse } from '@evcharge/platform';
import { Body, Controller, Delete, Get, Headers, Inject, Param, Post, Put, Query, Req, Res, UseFilters, UseGuards } from '@nestjs/common';
import type { FastifyReply } from 'fastify';
import { z } from 'zod';
import { AuthGuard, driverIdOf } from './auth.guard';
import { AuthService } from './auth.service';
import { APP_CONFIG, DOWNSTREAM } from './tokens';
import type { Config } from './config';
import { asDriver, COMMIT_LSN_HEADER, type Downstream, MIN_LSN_HEADER, readOpts } from './downstream';
import { LsnTracker } from './lsn-tracker';
import { metrics } from './metrics';
import { badRequest, parse, ProblemError, ProblemFilter } from './problem';
import { TtlCache } from './sites-cache';

const S = api.SessionsPublic;
/** sessions enforces the same maximum; checking here saves a round trip. */
const MAX_RADIUS_KM = 100;
/** Radii are rounded up to these steps so the cache key space stays small. */
const RADIUS_STEPS_KM = [1, 2, 5, 10, 25, 50, 100] as const;
const snapRadius = (km: number): number => RADIUS_STEPS_KM.find((r) => r >= km) ?? MAX_RADIUS_KM;
const sitesQuery = z.object({
  lat: z.coerce.number().min(-90).max(90),
  lng: z.coerce.number().min(-180).max(180),
  radiusKm: z.coerce.number().positive().max(MAX_RADIUS_KM).default(5),
  limit: z.coerce.number().int().min(1).max(100).default(20),
});
const pageQuery = z.object({ limit: z.coerce.number().int().min(1).max(100).optional(), before: z.iso.datetime({ offset: true }).optional() });
const powerQuery = z.object({ step: z.string().regex(/^\d{1,4}m$/, 'step must be whole minutes, e.g. 5m').optional() });
const intId = z.coerce.number().int().min(1).max(2_147_483_647);
const uuid = z.uuid();

type Req = { driverId?: string };

function idempotencyKey(raw: string | undefined): string {
  if (!raw) throw new ProblemError(400, 'idempotency_key_required', 'Idempotency key required', 'idempotency-key header is required on mutating calls');
  if (raw.length > 200) throw badRequest('idempotency-key is too long');
  return raw;
}

/** Relays a downstream success: status, body and the replay marker. */
function relay(reply: FastifyReply, res: HttpResponse<unknown>): unknown {
  reply.status(res.status);
  const replayed = res.headers.get('idempotent-replayed');
  if (replayed) reply.header('idempotent-replayed', replayed);
  return res.body;
}

@Controller('v1')
@UseGuards(AuthGuard)
@UseFilters(ProblemFilter)
export class DriverController {
  private readonly sitesCache: TtlCache<unknown>;
  private readonly read: { timeoutMs: number; retries: number };
  private readonly lsns: LsnTracker;

  constructor(
    @Inject(AuthService) private readonly auth: AuthService,
    @Inject(DOWNSTREAM) private readonly ds: Downstream,
    @Inject(APP_CONFIG) config: Config,
  ) {
    this.read = readOpts(config);
    this.lsns = new LsnTracker(config.MIN_LSN_TTL_MS);
    this.sitesCache = new TtlCache(config.SITES_CACHE_TTL_MS, Date.now, (hit) => metrics.sitesCache.inc({ result: hit ? 'hit' : 'miss' }));
  }

  /** Remembers the commit LSN of a successful write for the driver (and one resource) and relays the response. */
  private written(reply: FastifyReply, driverId: string, res: HttpResponse<unknown>, resource?: string): unknown {
    this.lsns.record(driverId, res.headers.get(COMMIT_LSN_HEADER), resource);
    return relay(reply, res);
  }

  /** Headers for a read of the driver's own data: identity plus x-min-lsn while their last write may still be replicating. */
  private readHeaders(driverId: string, resource?: string): Record<string, string> {
    const min = this.lsns.minLsn(driverId, resource);
    return min ? { ...asDriver(driverId), [MIN_LSN_HEADER]: min } : asDriver(driverId);
  }

  @Get('me')
  async me(@Req() req: Req) {
    return (await this.auth.me(driverIdOf(req))) ?? Promise.reject(new ProblemError(404, 'not_found', 'Not found', 'driver not found'));
  }

  /** GDPR erasure of the account; invoices and sessions are kept for the legal retention period (see README). */
  @Delete('me')
  async deleteMe(@Req() req: Req, @Headers('idempotency-key') key: string | undefined, @Res({ passthrough: true }) reply: FastifyReply): Promise<void> {
    idempotencyKey(key);
    await this.auth.deleteAccount(driverIdOf(req));
    reply.status(204);
  }

  @Put('me/payment-method')
  async setPaymentMethod(@Req() req: Req, @Headers('idempotency-key') key: string | undefined, @Body() body: unknown, @Res({ passthrough: true }) reply: FastifyReply) {
    const k = idempotencyKey(key);
    const driverId = driverIdOf(req);
    await this.auth.assertAccountExists(driverId);
    const payload = parse(api.BillingPublic.paymentMethodReq, body);
    return relay(reply, await this.ds.billing.put(`/v1/drivers/${driverId}/payment-method`, payload, { idempotencyKey: k, headers: asDriver(driverId) }));
  }

  /** Map data is not driver specific, so it is cached briefly and shared. lat/lng are rounded to ~110 m and the radius up to a fixed step, for the key and the query. */
  @Get('sites')
  async sites(@Query() q: unknown) {
    const { lat, lng, radiusKm: requested, limit } = parse(sitesQuery, q);
    const radiusKm = snapRadius(requested);
    const query = { lat: lat.toFixed(3), lng: lng.toFixed(3), radiusKm, limit };
    return this.sitesCache.get(`${query.lat}|${query.lng}|${radiusKm}|${limit}`, async () => (await this.ds.sessions.get('/v1/sites', { ...this.read, query, headers: {} })).body);
  }

  @Post('reservations')
  async reserve(@Req() req: Req, @Headers('idempotency-key') key: string | undefined, @Body() body: unknown, @Res({ passthrough: true }) reply: FastifyReply) {
    const k = idempotencyKey(key);
    const driverId = driverIdOf(req);
    await this.auth.assertAccountExists(driverId);
    const payload = parse(S.createReservationReq, body);
    const res = await this.ds.sessions.post<{ reservationId?: number }>('/v1/reservations', payload, { idempotencyKey: k, headers: asDriver(driverId) });
    return this.written(reply, driverId, res, res.body?.reservationId === undefined ? undefined : `reservation:${res.body.reservationId}`);
  }

  @Delete('reservations/:id')
  async cancelReservation(@Req() req: Req, @Headers('idempotency-key') key: string | undefined, @Param('id') id: string, @Res({ passthrough: true }) reply: FastifyReply) {
    const k = idempotencyKey(key);
    const driverId = driverIdOf(req);
    const reservationId = parse(intId, id);
    return this.written(reply, driverId, await this.ds.sessions.delete(`/v1/reservations/${reservationId}`, { idempotencyKey: k, headers: asDriver(driverId) }), `reservation:${reservationId}`);
  }

  @Post('sessions')
  async remoteStart(@Req() req: Req, @Headers('idempotency-key') key: string | undefined, @Body() body: unknown, @Res({ passthrough: true }) reply: FastifyReply) {
    const k = idempotencyKey(key);
    const driverId = driverIdOf(req);
    await this.auth.assertAccountExists(driverId);
    const payload = parse(S.remoteStartReq, body);
    return this.written(reply, driverId, await this.ds.sessions.post('/v1/sessions/remote-start', payload, { idempotencyKey: k, headers: asDriver(driverId) }));
  }

  @Get('sessions')
  async listSessions(@Req() req: Req, @Query() q: unknown, @Res({ passthrough: true }) reply: FastifyReply) {
    const driverId = driverIdOf(req);
    return relay(reply, await this.ds.sessions.get(`/v1/drivers/${driverId}/sessions`, { ...this.read, query: parse(pageQuery, q), headers: this.readHeaders(driverId) }));
  }

  @Get('sessions/:id')
  async getSession(@Req() req: Req, @Param('id') id: string, @Res({ passthrough: true }) reply: FastifyReply) {
    const driverId = driverIdOf(req);
    const sessionId = parse(uuid, id);
    return relay(reply, await this.ds.sessions.get(`/v1/sessions/${sessionId}`, { ...this.read, headers: this.readHeaders(driverId, `session:${sessionId}`) }));
  }

  @Post('sessions/:id/stop')
  async stopSession(@Req() req: Req, @Headers('idempotency-key') key: string | undefined, @Param('id') id: string, @Res({ passthrough: true }) reply: FastifyReply) {
    const k = idempotencyKey(key);
    const driverId = driverIdOf(req);
    const sessionId = parse(uuid, id);
    return this.written(reply, driverId, await this.ds.sessions.post(`/v1/sessions/${sessionId}/stop`, undefined, { idempotencyKey: k, headers: asDriver(driverId) }), `session:${sessionId}`);
  }

  /** Live power of the session's connector plus the 1-minute series since the session started. Ownership is enforced by sessions. */
  @Get('sessions/:id/power')
  async power(@Req() req: Req, @Param('id') id: string, @Query() q: unknown) {
    const driverId = driverIdOf(req);
    const { step } = parse(powerQuery, q);
    const sessionId = parse(uuid, id);
    const { body: s } = await this.ds.sessions.get<z.infer<typeof S.session>>(`/v1/sessions/${sessionId}`, { ...this.read, headers: this.readHeaders(driverId, `session:${sessionId}`) });
    const path = `/v1/stations/${encodeURIComponent(s.stationId)}/connectors/${s.connectorId}`;
    const [latest, series] = await Promise.all([
      this.ds.telemetry
        .get(`${path}/latest`, this.read)
        .then((r) => r.body)
        .catch((err: unknown) => {
          if ((err as { status?: number }).status === 404) return null; // no reading yet
          throw err;
        }),
      this.ds.telemetry.get<z.infer<typeof api.PowerSeriesRes>>(`${path}/power`, { ...this.read, query: { from: s.startedAt, to: s.stoppedAt ?? new Date().toISOString(), step } }).then((r) => r.body),
    ]);
    return { sessionId: s.sessionId, latest, series };
  }

  @Get('invoices')
  async invoices(@Req() req: Req, @Query() q: unknown, @Res({ passthrough: true }) reply: FastifyReply) {
    const driverId = driverIdOf(req);
    // A client-sent driverId query parameter is dropped: only limit/before are forwarded, the driver is the JWT's.
    return relay(reply, await this.ds.billing.get('/v1/invoices', { ...this.read, query: { ...parse(pageQuery, q), driverId }, headers: asDriver(driverId) }));
  }

  @Get('invoices/:id')
  async invoice(@Req() req: Req, @Param('id') id: string, @Res({ passthrough: true }) reply: FastifyReply) {
    const driverId = driverIdOf(req);
    return relay(reply, await this.ds.billing.get(`/v1/invoices/${parse(uuid, id)}`, { ...this.read, headers: asDriver(driverId) }));
  }
}
