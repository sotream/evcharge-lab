import { describe, expect, it } from 'vitest';
import { ocpp } from '@evcharge/contracts';
import { ProfileStack, type Profile } from '../../src/profiles';

const { txProfileId } = ocpp;
const NOW = Date.parse('2026-11-08T12:00:00Z');

const profile = (o: Partial<Profile> & { limit: number; id?: number }): Profile => ({
  chargingProfileId: o.id ?? 1,
  stackLevel: o.stackLevel ?? 0,
  chargingProfilePurpose: o.chargingProfilePurpose ?? 'TxDefaultProfile',
  chargingProfileKind: o.chargingProfileKind ?? 'Relative',
  ...(o.validTo ? { validTo: o.validTo } : {}),
  chargingSchedule: o.chargingSchedule ?? { chargingRateUnit: 'W', chargingSchedulePeriod: [{ startPeriod: 0, limit: o.limit }] },
});

const limit = (st: ProfileStack, cid: number, t = NOW, tx = true) => st.limitW(cid, t, tx, NOW - 60_000, 3);

describe('ProfileStack (ADR 0006)', () => {
  it('one profile id per connector: connector 2 profiles never touch connector 1, and the safe share replaces only its own allocation', () => {
    const st = new ProfileStack();
    const validTo = new Date(NOW + 180_000).toISOString();
    st.set(0, profile({ id: 1, limit: 4_140 }), NOW, false);
    st.set(1, profile({ id: txProfileId(1), stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 9_000, validTo }), NOW, true);
    st.set(2, profile({ id: txProfileId(2), stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 7_000, validTo }), NOW, true);
    expect(st.size).toBe(3);
    expect(limit(st, 1)).toBe(9_000);
    expect(limit(st, 2)).toBe(7_000);
    // a new allocation for connector 2 leaves connector 1 alone
    st.set(2, profile({ id: txProfileId(2), stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 6_000, validTo }), NOW, true);
    expect(limit(st, 1)).toBe(9_000);
    expect(limit(st, 2)).toBe(6_000);
    // the gateway safe share for connector 1 (same id, no validTo) replaces connector 1's allocation only
    st.set(1, profile({ id: txProfileId(1), stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 5_500 }), NOW, true);
    expect(st.size).toBe(3);
    expect(limit(st, 1, NOW + 3_600_000)).toBe(5_500);
    expect(limit(st, 2)).toBe(6_000);
    // ending connector 1's transaction removes only its TxProfile
    st.clearTx(1);
    expect(limit(st, 1)).toBe(4_140);
    expect(limit(st, 2)).toBe(6_000);
  });

  it('no profile means no limit; a TxDefaultProfile on connector 0 applies to every connector during a transaction', () => {
    const st = new ProfileStack();
    expect(limit(st, 1)).toBe(Number.POSITIVE_INFINITY);
    expect(st.set(0, profile({ limit: 4_140 }), NOW, false)).toBe('Accepted');
    expect(limit(st, 1)).toBe(4_140);
    expect(limit(st, 2)).toBe(4_140);
    expect(limit(st, 1, NOW, false)).toBe(Number.POSITIVE_INFINITY);
  });

  it('a TxProfile (level 1) beats the default (level 0) until its validTo, then the default applies again', () => {
    const st = new ProfileStack();
    st.set(0, profile({ limit: 4_140 }), NOW, false);
    const validTo = new Date(NOW + 180_000).toISOString();
    expect(st.set(1, profile({ id: 2, stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 9_000, validTo }), NOW, true)).toBe('Accepted');
    expect(limit(st, 1)).toBe(9_000);
    expect(limit(st, 2)).toBe(4_140); // other connector keeps the default
    expect(limit(st, 1, NOW + 179_000)).toBe(9_000);
    expect(limit(st, 1, NOW + 180_000)).toBe(4_140);
  });

  it('the safe-share TxProfile (same stack level, no validTo) replaces an expired or live allocation', () => {
    const st = new ProfileStack();
    st.set(1, profile({ id: 2, stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 9_000, validTo: new Date(NOW + 1000).toISOString() }), NOW, true);
    st.set(1, profile({ id: 3, stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 5_500 }), NOW, true);
    expect(st.size).toBe(1);
    expect(limit(st, 1, NOW + 3_600_000)).toBe(5_500);
  });

  it('a profile with the same id replaces the old one; a higher stack level wins within a purpose', () => {
    const st = new ProfileStack();
    st.set(0, profile({ id: 1, limit: 4_000 }), NOW, false);
    st.set(0, profile({ id: 1, limit: 3_000 }), NOW, false);
    expect(st.size).toBe(1);
    st.set(0, profile({ id: 7, stackLevel: 2, limit: 8_000 }), NOW, false);
    expect(limit(st, 1)).toBe(8_000);
  });

  it('rejects a TxProfile without a running transaction and clears TxProfiles when it ends', () => {
    const st = new ProfileStack();
    expect(st.set(1, profile({ id: 2, stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 1 }), NOW, false)).toBe('Rejected');
    expect(st.set(0, profile({ id: 2, stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 1 }), NOW, true)).toBe('Rejected');
    st.set(0, profile({ limit: 4_000 }), NOW, false);
    st.set(1, profile({ id: 2, stackLevel: 1, chargingProfilePurpose: 'TxProfile', limit: 9_000 }), NOW, true);
    st.clearTx(1);
    expect(limit(st, 1)).toBe(4_000);
  });

  it('honours periods of a schedule relative to the transaction start, and A units', () => {
    const st = new ProfileStack();
    st.set(
      0,
      profile({ limit: 0, chargingSchedule: { chargingRateUnit: 'W', chargingSchedulePeriod: [{ startPeriod: 0, limit: 3_000 }, { startPeriod: 120, limit: 7_000 }] } }),
      NOW,
      false,
    );
    expect(st.limitW(1, NOW, true, NOW - 60_000, 3)).toBe(3_000); // 60 s into the transaction
    expect(st.limitW(1, NOW, true, NOW - 130_000, 3)).toBe(7_000);
    const amps = new ProfileStack();
    amps.set(0, profile({ limit: 0, chargingSchedule: { chargingRateUnit: 'A', chargingSchedulePeriod: [{ startPeriod: 0, limit: 16 }] } }), NOW, false);
    expect(amps.limitW(1, NOW, true, NOW, 3)).toBe(16 * 230 * 3);
  });

  it('ChargePointMaxProfile caps everything', () => {
    const st = new ProfileStack();
    st.set(0, profile({ id: 5, chargingProfilePurpose: 'ChargePointMaxProfile', limit: 6_000 }), NOW, false);
    st.set(0, profile({ limit: 11_000 }), NOW, false);
    expect(limit(st, 1)).toBe(6_000);
    expect(st.set(1, profile({ id: 6, chargingProfilePurpose: 'ChargePointMaxProfile', limit: 1 }), NOW, true)).toBe('Rejected');
  });
});
