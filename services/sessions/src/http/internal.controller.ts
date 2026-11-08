import { Body, Controller, Get, HttpCode, Inject, Param, Post, Res, UseFilters, UseGuards } from '@nestjs/common';
import { api } from '@evcharge/contracts';
import { InternalTokenGuard } from '@evcharge/platform';
import type { FastifyReply } from 'fastify';
import { OcppService } from '../ocpp/ocpp.service';
import { parse, ProblemFilter } from './problem';

const S = api.SessionsInternal;

/** OCPP-facing internal API called by station-gateway (contracts `SessionsInternal`). */
@Controller('internal')
@UseGuards(InternalTokenGuard)
@UseFilters(ProblemFilter)
export class InternalController {
  constructor(@Inject(OcppService) private readonly ocpp: OcppService) {}

  @Get('stations/:stationId/credential')
  credential(@Param('stationId') stationId: string) {
    return this.ocpp.credential(stationId);
  }

  @Post('ocpp/authorize')
  @HttpCode(200)
  authorize(@Body() body: unknown) {
    return this.ocpp.authorize(parse(S.authorizeReq, body));
  }

  @Post('ocpp/start-transaction')
  @HttpCode(200)
  startTransaction(@Body() body: unknown) {
    return this.ocpp.startTransaction(parse(S.startTxReq, body));
  }

  @Post('ocpp/stop-transaction')
  @HttpCode(200)
  stopTransaction(@Body() body: unknown) {
    return this.ocpp.stopTransaction(parse(S.stopTxReq, body));
  }

  @Post('drivers')
  async registerDriver(@Body() body: unknown, @Res({ passthrough: true }) reply: FastifyReply) {
    const r = await this.ocpp.registerDriver(parse(S.registerDriverReq, body));
    reply.status(r.status);
    return r.body;
  }
}
