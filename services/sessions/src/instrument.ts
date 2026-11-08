// Must be the first import of main.ts: instrumentations patch pg/ioredis/http when they are first required.
import { startTelemetry } from '@evcharge/platform/telemetry';

export const telemetry = startTelemetry({ serviceName: 'sessions' });
