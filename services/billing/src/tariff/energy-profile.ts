import type { api } from '@evcharge/contracts';
import type { z } from 'zod';

type EnergySample = z.infer<typeof api.EnergySample>;

/**
 * Cumulative energy register from per-bucket energy: the first sample is the session start at 0 Wh, then one sample at
 * each bucket END time. Buckets not strictly after the previous sample are dropped (the tariff requires increasing times).
 */
export function profileFromBuckets(startedAt: Date, buckets: { bucket: string; energyWhDelta: number }[]): EnergySample[] {
  const out: EnergySample[] = [{ at: startedAt.toISOString(), energyWh: 0 }];
  let last = startedAt.getTime();
  let sum = 0;
  for (const b of buckets) {
    const at = Date.parse(b.bucket);
    if (!(at > last)) continue;
    sum += Math.max(0, Math.round(b.energyWhDelta));
    out.push({ at: new Date(at).toISOString(), energyWh: sum });
    last = at;
  }
  return out;
}

/**
 * The session's energy shape from telemetry-ingest's power series (avg W per bucket x bucket length = Wh). Only used to
 * split time-of-use energy between price bands, so the estimate need not match the meter exactly (the tariff rescales
 * to the metered total). Any error or a 2 s timeout gives undefined: pricing never waits on telemetry, the tariff then
 * falls back to time weights.
 */
export async function fetchEnergyProfile(
  baseUrl: string,
  token: string,
  stationId: string,
  connectorId: number,
  startedAt: Date,
  stoppedAt: Date,
): Promise<EnergySample[] | undefined> {
  try {
    const minutes = Math.ceil((stoppedAt.getTime() - startedAt.getTime()) / 60_000);
    if (minutes < 2) return undefined;
    const stepMin = Math.max(1, Math.ceil(minutes / 1440)); // the series is capped at 1440 points
    const from = new Date(Math.floor(startedAt.getTime() / 60_000) * 60_000);
    const qs = new URLSearchParams({ from: from.toISOString(), to: stoppedAt.toISOString(), step: `${stepMin}m` });
    const res = await fetch(`${baseUrl}/v1/stations/${encodeURIComponent(stationId)}/connectors/${connectorId}/power?${qs}`, {
      headers: { 'x-internal-token': token },
      signal: AbortSignal.timeout(2000),
    });
    if (!res.ok) return undefined;
    const body = (await res.json()) as { points?: { t: string; avgW: number }[] };
    const buckets = (body.points ?? []).map((p) => ({
      bucket: new Date(Math.min(Date.parse(p.t) + stepMin * 60_000, stoppedAt.getTime())).toISOString(),
      energyWhDelta: (p.avgW * stepMin) / 60,
    }));
    const profile = profileFromBuckets(startedAt, buckets);
    return profile.length >= 2 ? profile : undefined;
  } catch {
    return undefined;
  }
}
