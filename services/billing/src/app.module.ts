import { type DynamicModule, Module } from '@nestjs/common';
import { BILLING_DEPS, BillingController, type BillingDeps } from './api/billing.controller';
import { InternalController } from './api/internal.controller';

@Module({})
export class AppModule {
  static register(deps: BillingDeps): DynamicModule {
    return { module: AppModule, controllers: [BillingController, InternalController], providers: [{ provide: BILLING_DEPS, useValue: deps }] };
  }
}
