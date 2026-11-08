import { createHash } from 'node:crypto';
import { ocpp } from '@evcharge/contracts';
import { describe, expect, it } from 'vitest';
import { AllocationTracker, allocationValidTo, replacesAllocationWithoutExpiry, safeShareProfile, startDefaultProfile } from '../../src/charging';
import { parseBasicAuth, verifySecret } from '../../src/credentials';
import { TokenBucket, TtlLru } from '../../src/util';

const sha = (s: string) => createHash('sha256').update(s).digest('hex');
const basic = (s: string) => `Basic ${Buffer.from(s).toString('base64')}`;

describe('credentials', () => {
  it('verifies SHA-256(password) in constant time', () => {
    expect(verifySecret('secret', sha('secret'))).toBe(true);
    expect(verifySecret('Secret', sha('secret'))).toBe(false);
    expect(verifySecret('secret', 'abcd')).toBe(false);
  });
  it('parses Basic auth', () => {
    expect(parseBasicAuth(basic('st-1:p:w'))).toEqual({ username: 'st-1', password: 'p:w' });
    expect(parseBasicAuth(undefined)).toBeUndefined();
    expect(parseBasicAuth('Bearer x')).toBeUndefined();
    expect(parseBasicAuth(basic('nocolon'))).toBeUndefined();
  });
});

describe('TokenBucket', () => {
  it('admits a burst then refills at the rate', () => {
    let t = 0;
    const b = new TokenBucket(10, 10, () => t);
    for (let i = 0; i < 10; i++) expect(b.tryTake()).toBe(true);
    expect(b.tryTake()).toBe(false);
    t += 200;
    expect(b.tryTake()).toBe(true);
    expect(b.tryTake()).toBe(true);
    expect(b.tryTake()).toBe(false);
  });
});

describe('TtlLru', () => {
  it('expires entries and evicts the least recently used', () => {
    let t = 0;
    const l = new TtlLru<number>(2, () => t);
    l.set('a', 1, 1000);
    l.set('b', 2, 1000);
    l.get('a');
    l.set('c', 3, 1000);
    expect(l.get('b')).toBeUndefined();
    expect(l.get('a')).toBe(1);
    t = 1001;
    expect(l.get('a')).toBeUndefined();
  });
});

const { txProfileId } = ocpp;

describe('charging profiles (ADR 0006)', () => {
  it('builds the start default and the safe share', () => {
    const d = startDefaultProfile(4140);
    expect(d.connectorId).toBe(0);
    expect(d.csChargingProfiles).toMatchObject({ stackLevel: 0, chargingProfilePurpose: 'TxDefaultProfile', chargingSchedule: { chargingRateUnit: 'W', chargingSchedulePeriod: [{ limit: 4140 }] } });
    expect(d.csChargingProfiles.validTo).toBeUndefined();
    const s = safeShareProfile(2, 11000);
    expect(s.csChargingProfiles).toMatchObject({ stackLevel: 1, chargingProfilePurpose: 'TxProfile', chargingProfileId: 102 });
    expect(allocationValidTo(s)).toBeUndefined();
    expect(replacesAllocationWithoutExpiry(s)).toBe(true);
  });

  it('uses one profile id per connector, so a safe share replaces only its own connector allocation', () => {
    expect(safeShareProfile(1, 5000).csChargingProfiles.chargingProfileId).toBe(txProfileId(1));
    expect(safeShareProfile(2, 5000).csChargingProfiles.chargingProfileId).toBe(txProfileId(2));
    expect(txProfileId(1)).not.toBe(txProfileId(2));
    expect(startDefaultProfile(4140).csChargingProfiles.chargingProfileId).toBe(ocpp.ChargingProfiles.DEFAULT_PROFILE_ID);
    expect(startDefaultProfile(4140).connectorId).toBe(0);
  });

  it('lists expired allocations after the grace and keeps them until replaced or cleared', () => {
    const tr = new AllocationTracker();
    tr.record('st', 1, 1000);
    tr.record('st', 2, 9000);
    expect(tr.expired(5999, 5000)).toEqual([]);
    expect(tr.expired(6000, 5000)).toEqual([{ stationId: 'st', connectorId: 1 }]);
    expect(tr.expired(6000, 5000)).toEqual([{ stationId: 'st', connectorId: 1 }]); // still listed: the site check may not have passed yet
    tr.attempted('st', 1, 6000);
    expect(tr.expired(7000, 5000, 30_000)).toEqual([]); // throttled after an attempt
    expect(tr.expired(40_000, 5000, 30_000)).toHaveLength(2);
    tr.record('st', 2, 99_000); // a newer allocation moves the deadline
    expect(tr.expired(40_000, 5000, 30_000)).toEqual([{ stationId: 'st', connectorId: 1 }]);
    tr.clear('st', 1);
    tr.clear('st', 2);
    expect(tr.size).toBe(0);
  });
});
