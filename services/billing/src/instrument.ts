// Must be the first import of main.ts: instrumentations patch pg/http on first require.
import { startTelemetry } from '@evcharge/platform/telemetry';

export const telemetry = startTelemetry({ serviceName: process.env['SERVICE_NAME'] ?? 'billing' });
