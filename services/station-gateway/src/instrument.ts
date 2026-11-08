// Imports nothing but the telemetry entry point: instrumentations patch http/undici/ioredis on first require.
import { startTelemetry } from '@evcharge/platform/telemetry';

export const telemetry = startTelemetry({ serviceName: process.env['SERVICE_NAME'] ?? 'station-gateway' });
