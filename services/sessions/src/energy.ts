export type EnergyFlag = 'ok' | 'meter_regressed' | 'implausible' | 'stale';

/** Plausibility of a session's metered energy; 'stale' is set by the reaper (Task 4), never here. */
export function energyFlag(meterStartWh: number, meterStopWh: number, startedAt: Date, stoppedAt: Date, maxPowerW: number): EnergyFlag {
  if (meterStopWh < meterStartWh) return 'meter_regressed';
  const durationMs = Math.max(0, stoppedAt.getTime() - startedAt.getTime());
  // bound = maxPowerW * duration(h) * 1.1, in integers
  return (meterStopWh - meterStartWh) * 36_000_000 > maxPowerW * durationMs * 11 ? 'implausible' : 'ok';
}
