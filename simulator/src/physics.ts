/**
 * Charging physics (ADR 0013). Power is a function of the connector, the vehicle, the state of
 * charge and the active OCPP limit; the energy register is the integral of that power, so energy
 * and power samples are always consistent.
 */
import { fleet } from '@evcharge/contracts';

export const AC_VOLTAGE = 230;
export const DC_VOLTAGE = 400;
/** DC: full power up to this SoC, then a linear taper... */
export const DC_TAPER_START_SOC = 0.8;
/** ...down to this fraction of max power at 100% SoC. */
export const DC_TAPER_END_FRACTION = 0.1;
/** Integration step in simulated milliseconds. */
export const STEP_MS = 10_000;

export type Kind = 'AC' | 'DC';

export interface Vehicle {
  capacityWh: number;
  /** Highest power the vehicle accepts (AC: its on-board charger; DC: its peak). */
  maxW: number;
}

export interface ChargeState {
  /** Simulated time the state is valid at. */
  tMs: number;
  /** Energy register (Wh), monotonic. */
  energyWh: number;
  soc: number;
  /** Power at the end of the last integration step. */
  powerW: number;
}

/** Minimum charge power of the vehicle side: 6 A per phase for AC, shared with the seed via contracts `fleet`. */
export const minChargeW = (kind: Kind): number => (kind === 'AC' ? fleet.AC_MIN_W : fleet.DC_MIN_W);

/** DC taper factor in (0.1..1] by SoC. */
export function dcTaper(soc: number): number {
  if (soc <= DC_TAPER_START_SOC) return 1;
  if (soc >= 1) return DC_TAPER_END_FRACTION;
  return 1 - (1 - DC_TAPER_END_FRACTION) * ((soc - DC_TAPER_START_SOC) / (1 - DC_TAPER_START_SOC));
}

/** The charge curve before any OCPP limit: AC flat at min(connector, vehicle); DC tapering. */
export function curveW(kind: Kind, connectorMaxW: number, vehicle: Vehicle, soc: number): number {
  const flat = Math.min(connectorMaxW, vehicle.maxW);
  return kind === 'AC' ? flat : flat * dcTaper(soc);
}

/** min(curve, limit); below the vehicle's minimum charge power the EVSE is effectively suspended (0 W). */
export function drawnW(curve: number, limitW: number, minW: number): number {
  const p = Math.min(curve, limitW);
  return p < minW ? 0 : p;
}

/**
 * Integrates the charge from `state.tMs` to `toMs` in steps of at most STEP_MS. `limitAt(t)` is the
 * OCPP limit in W at simulated time t (Infinity when none). Mutates and returns `state`.
 */
export function advanceCharge(
  state: ChargeState,
  toMs: number,
  p: { kind: Kind; connectorMaxW: number; phases: number; vehicle: Vehicle; limitAt: (tMs: number) => number },
): ChargeState {
  const minW = minChargeW(p.kind);
  while (state.tMs < toMs) {
    const dt = Math.min(STEP_MS, toMs - state.tMs);
    const w = state.soc >= 1 ? 0 : drawnW(curveW(p.kind, p.connectorMaxW, p.vehicle, state.soc), p.limitAt(state.tMs), minW);
    // never add more energy than the battery has room for
    const wh = Math.min((w * dt) / 3_600_000, (1 - state.soc) * p.vehicle.capacityWh);
    state.energyWh += wh;
    state.soc = Math.min(1, state.soc + wh / p.vehicle.capacityWh);
    state.tMs += dt;
    state.powerW = w;
  }
  return state;
}

export function sampleElectrics(kind: Kind, phases: number, powerW: number): { voltageV: number; currentA: number } {
  if (kind === 'DC') return { voltageV: DC_VOLTAGE, currentA: powerW / DC_VOLTAGE };
  return { voltageV: AC_VOLTAGE, currentA: powerW / (AC_VOLTAGE * phases) };
}
