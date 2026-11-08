import type { ocpp } from '@evcharge/contracts';

/** OCPP 1.6 defaults when a sampledValue omits them. */
const DEFAULT_MEASURAND = 'Energy.Active.Import.Register';
const DEFAULT_UNIT: Record<string, string> = {
  'Energy.Active.Import.Register': 'Wh',
  'Power.Active.Import': 'W',
  'Current.Import': 'A',
  Voltage: 'V',
  SoC: 'Percent',
};

export interface Reading {
  timestamp: string;
  samples: { measurand: string; value: number; unit: string; context?: string; phase?: string }[];
}

export type MeterReq = ocpp.StationReq<'MeterValues'>;

/** Converts OCPP meterValue[] to MeterValuesReceived readings. Throws on a non-numeric value. */
export function toReadings(meterValue: MeterReq['meterValue']): Reading[] {
  return meterValue.map((mv) => ({
    timestamp: new Date(mv.timestamp).toISOString(),
    samples: mv.sampledValue.map((sv) => {
      const value = Number(sv.value);
      if (sv.value.trim() === '' || !Number.isFinite(value)) throw new TypeError(`sampledValue.value "${sv.value}" is not numeric`);
      const measurand = sv.measurand ?? DEFAULT_MEASURAND;
      return {
        measurand,
        value,
        unit: sv.unit ?? DEFAULT_UNIT[measurand] ?? '',
        ...(sv.context ? { context: sv.context } : {}),
        ...(sv.phase ? { phase: sv.phase } : {}),
      };
    }),
  }));
}

/** ADR 0003: a reading whose newest station timestamp is older than the threshold is a replay. */
export function isReplayed(readings: Reading[], nowMs: number, thresholdS: number): boolean {
  let newest = Number.NEGATIVE_INFINITY;
  for (const r of readings) newest = Math.max(newest, Date.parse(r.timestamp));
  return nowMs - newest > thresholdS * 1000;
}
