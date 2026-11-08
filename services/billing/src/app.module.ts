import { type DynamicModule, Module } from '@nestjs/common';
import { BILLING_DEPS, BillingController, type BillingDeps } from './api/billing.controller';

@Module({})
export class AppModule {
  static register(deps: BillingDeps): DynamicModule {
    return { module: AppModule, controllers: [BillingController], providers: [{ provide: BILLING_DEPS, useValue: deps }] };
  }
}
