import { describe, expect, it } from 'vitest';
import { encodeCall, encodeError, encodeResult, parseFrame, validateStationCall } from '../../src/ocpp/framing';
import { isReplayed, toReadings } from '../../src/ocpp/meter';

describe('parseFrame', () => {
  it('parses CALL, CALLRESULT and CALLERROR', () => {
    expect(parseFrame('[2,"a","Heartbeat",{}]')).toEqual({ kind: 'call', uniqueId: 'a', action: 'Heartbeat', payload: {} });
    expect(parseFrame('[3,"a",{"x":1}]')).toEqual({ kind: 'result', uniqueId: 'a', payload: { x: 1 } });
    expect(parseFrame('[4,"a","InternalError","boom",{}]')).toMatchObject({ kind: 'error', code: 'InternalError' });
  });
  it('rejects garbage with FormationViolation and keeps the uniqueId when readable', () => {
    expect(parseFrame('not json')).toMatchObject({ kind: 'invalid', code: 'FormationViolation', uniqueId: '-1' });
    expect(parseFrame('{"a":1}')).toMatchObject({ kind: 'invalid', code: 'FormationViolation' });
    expect(parseFrame('[9,"id"]')).toMatchObject({ kind: 'invalid', uniqueId: 'id' });
    expect(parseFrame('[2,"id","Heartbeat"]')).toMatchObject({ kind: 'invalid', uniqueId: 'id', code: 'FormationViolation' });
    expect(parseFrame(`[2,"${'x'.repeat(40)}","Heartbeat",{}]`)).toMatchObject({ kind: 'invalid', uniqueId: '-1' });
  });
  it('encodes frames', () => {
    expect(JSON.parse(encodeCall('1', 'Heartbeat', {}))).toEqual([2, '1', 'Heartbeat', {}]);
    expect(JSON.parse(encodeResult('1', { a: 1 }))).toEqual([3, '1', { a: 1 }]);
    expect(JSON.parse(encodeError('1', 'NotImplemented', 'x'))).toEqual([4, '1', 'NotImplemented', 'x', {}]);
  });
});

describe('validateStationCall', () => {
  it('answers NotImplemented for unknown actions', () => {
    expect(validateStationCall('DataTransfer', {})).toMatchObject({ ok: false, code: 'NotImplemented' });
    expect(validateStationCall('toString', {})).toMatchObject({ ok: false, code: 'NotImplemented' });
  });
  it('maps missing fields to ProtocolError and wrong types to TypeConstraintViolation', () => {
    expect(validateStationCall('Authorize', {})).toMatchObject({ ok: false, code: 'ProtocolError' });
    expect(validateStationCall('Authorize', { idTag: 5 })).toMatchObject({ ok: false, code: 'TypeConstraintViolation' });
    expect(validateStationCall('Authorize', { idTag: 'x'.repeat(21) })).toMatchObject({ ok: false, code: 'TypeConstraintViolation' });
    expect(validateStationCall('Authorize', { idTag: 'ABC' })).toMatchObject({ ok: true });
  });
});

describe('meter values', () => {
  const mv = [{ timestamp: '2026-11-08T10:00:00Z', sampledValue: [{ value: '1500' }, { value: '7.2', measurand: 'Power.Active.Import' as const, unit: 'kW' as const }] }];
  it('applies OCPP defaults and normalises timestamps', () => {
    const [r] = toReadings(mv);
    expect(r?.timestamp).toBe('2026-11-08T10:00:00.000Z');
    expect(r?.samples[0]).toEqual({ measurand: 'Energy.Active.Import.Register', value: 1500, unit: 'Wh' });
    expect(r?.samples[1]).toMatchObject({ measurand: 'Power.Active.Import', value: 7.2, unit: 'kW' });
  });
  it('rejects non numeric values', () => {
    expect(() => toReadings([{ timestamp: '2026-11-08T10:00:00Z', sampledValue: [{ value: 'abc' }] }])).toThrow(/numeric/);
    expect(() => toReadings([{ timestamp: '2026-11-08T10:00:00Z', sampledValue: [{ value: ' ' }] }])).toThrow(/numeric/);
  });
  it('marks readings older than the threshold as replayed (newest wins)', () => {
    const now = Date.parse('2026-11-08T10:05:00Z');
    expect(isReplayed(toReadings(mv), now, 60)).toBe(true);
    expect(isReplayed(toReadings(mv), Date.parse('2026-11-08T10:00:30Z'), 60)).toBe(false);
    const two = toReadings([...mv, { timestamp: '2026-11-08T10:04:50Z', sampledValue: [{ value: '1' }] }]);
    expect(isReplayed(two, now, 60)).toBe(false);
  });
});
