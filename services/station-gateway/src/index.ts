/** Library entry point (the process entry is main.ts): lets tests and the simulator run a gateway in-process. */
// oxlint-disable-next-line import/no-unassigned-import -- Nest decorators need the polyfill loaded first
import 'reflect-metadata';

export { gatewayConfigSchema, loadGatewayConfig, type GatewayConfig } from './config';
export { StationGateway, type GatewayOptions } from './gateway';
export { defaultTopics, type GatewayTopics } from './publisher';
export { verifySecret, parseBasicAuth } from './credentials';
export type { StationCredential } from './sessions-client';
