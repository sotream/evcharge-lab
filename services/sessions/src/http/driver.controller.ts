import { Body, Controller, Delete, Get, Headers, HttpCode, Inject, Param, Post, Query, Req, Res, UseFilters, UseGuards } from '@nestjs/common';
import { api } from '@evcharge/contracts';
import { InternalTokenGuard, IDEMPOTENT_REPLAYED_HEADER, type StoredResponse } from '@evcharge/platform';
import type { FastifyReply } from 'fastify';
import { z } from 'zod';
import { CatalogService, MAX_RADIUS_KM } from '../driver/catalog.service';
import { DriverService } from '../driver/driver.service';
import { DriverGuard, driverIdOf } from './driver.guard';
import { badRequest, parse, ProblemError, ProblemFilter } from './problem';

const P = api.SessionsPublic;
const sitesQuery = z.object({
  lat: z.coerce.number().min(-90).max(90),
  lng: z.coerce.number().min(-180).max(180),
  radiusKm: z.coerce.number().positive().max(MAX_RADIUS_KM).default(5),
  limit: z.coerce.number().int().min(1).max(100).default(20),
});
const listQuery = z.object({ limit: z.coerce.number().int().min(1).max(100).default(20), before: z.iso.datetime({ offset: true }).optional() });
const intId = z.coerce.number().int().min(1).max(2_147_483_647);
const uuid = z.uuid();

type Req = { driverId?: string };

function idempotencyKey(raw: string | undefined): string {
  if (!raw) throw new ProblemError(400, 'idempotency_key_required', 'Idempotency key required', 'idempotency-key header is required');
  if (raw.length > 200) throw badRequest('idempotency-key is too long');
  return raw;
}

function send(reply: FastifyReply, r: { replayed: boolean; response: StoredResponse }): unknown {
  reply.status(r.response.status);
  if (r.replayed) reply.header(IDEMPOTENT_REPLAYED_HEADER, 'true');
  return r.response.body;
}

/**
 * Driver-facing API, reached only through driver-api (ADR 0010): every route needs x-internal-token; routes
 * acting for a driver also need x-driver-id, trusted only after the token was verified.
 */
@Controller('v1')
@UseGuards(InternalTokenGuard)
@UseFilters(ProblemFilter)
export class DriverController {
  constructor(
    @Inject(CatalogService) private readonly catalog: CatalogService,
    @Inject(DriverService) private readonly drivers: DriverService,
  ) {}

  @Get('sites')
  sites(@Query() q: unknown) {
    const { lat, lng, radiusKm, limit } = parse(sitesQuery, q);
    return this.catalog.listSites(lat, lng, radiusKm, limit);
  }

  @Post('reservations')
  @UseGuards(DriverGuard)
  async reserve(@Req() req: Req, @Headers('idempotency-key') key: string | undefined, @Body() body: unknown, @Res({ passthrough: true }) reply: FastifyReply) {
    const k = idempotencyKey(key);
    return send(reply, await this.drivers.createReservation(driverIdOf(req), k, parse(P.createReservationReq, body)));
  }

  @Get('reservations/:id')
  @UseGuards(DriverGuard)
  getReservation(@Req() req: Req, @Param('id') id: string) {
    return this.drivers.getReservation(driverIdOf(req), parse(intId, id));
  }

  @Delete('reservations/:id')
  @UseGuards(DriverGuard)
  cancelReservation(@Req() req: Req, @Param('id') id: string) {
    return this.drivers.cancelReservation(driverIdOf(req), parse(intId, id));
  }

  @Post('sessions/remote-start')
  @UseGuards(DriverGuard)
  async remoteStart(@Req() req: Req, @Headers('idempotency-key') key: string | undefined, @Body() body: unknown, @Res({ passthrough: true }) reply: FastifyReply) {
    const k = idempotencyKey(key);
    return send(reply, await this.drivers.remoteStart(driverIdOf(req), k, parse(P.remoteStartReq, body)));
  }

  @Get('sessions/:id')
  @UseGuards(DriverGuard)
  getSession(@Req() req: Req, @Param('id') id: string) {
    return this.drivers.getSession(driverIdOf(req), parse(uuid, id));
  }

  @Post('sessions/:id/stop')
  @HttpCode(202)
  @UseGuards(DriverGuard)
  stopSession(@Req() req: Req, @Param('id') id: string) {
    return this.drivers.stopSession(driverIdOf(req), parse(uuid, id));
  }

  @Get('drivers/:id/sessions')
  @UseGuards(DriverGuard)
  driverSessions(@Req() req: Req, @Param('id') id: string, @Query() q: unknown) {
    const driverId = driverIdOf(req);
    if (parse(uuid, id).toLowerCase() !== driverId) throw new ProblemError(403, 'forbidden', 'Forbidden', 'x-driver-id does not match the requested driver');
    const { limit, before } = parse(listQuery, q);
    return this.drivers.listDriverSessions(driverId, limit, before);
  }
}
