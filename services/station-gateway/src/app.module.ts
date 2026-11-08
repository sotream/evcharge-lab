import { Controller, type DynamicModule, Get, Inject, Module, NotFoundException, Param, UseGuards } from '@nestjs/common';
import { InternalTokenGuard } from '@evcharge/platform';

/** What the internal controller needs from the running gateway. */
export interface GatewayIntrospection {
  readonly instance: string;
  connectionInfo(stationId: string): { stationId: string; epoch: number; connectedAt: string; pendingCommands: number } | undefined;
  stats(): { instance: string; connections: number; produceBufferDepth: number; trackedAllocations: number; draining: boolean };
}

export const GATEWAY = Symbol('GATEWAY');

/** Internal, x-internal-token protected introspection (ADR 0010); used by drills and tests. */
@Controller('internal')
@UseGuards(InternalTokenGuard)
export class InternalController {
  constructor(@Inject(GATEWAY) private readonly gw: GatewayIntrospection) {}

  @Get('stats')
  stats() {
    return this.gw.stats();
  }

  @Get('stations/:stationId/connection')
  connection(@Param('stationId') stationId: string) {
    const info = this.gw.connectionInfo(stationId);
    if (!info) throw new NotFoundException(`station ${stationId} is not connected to ${this.gw.instance}`);
    return { instance: this.gw.instance, ...info };
  }
}

@Module({})
export class GatewayModule {
  static forRoot(gw: GatewayIntrospection): DynamicModule {
    return { module: GatewayModule, controllers: [InternalController], providers: [{ provide: GATEWAY, useValue: gw }] };
  }
}
