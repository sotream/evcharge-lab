import { IdempotencyInFlightError, IdempotencyMismatchError } from '@evcharge/platform';
import { z } from 'zod';
import { HttpException } from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import { boundingBox } from '../../src/driver/catalog.service';
import { mapReservationError } from '../../src/driver/driver.service';
import { readConfig } from '../../src/config';
import { badRequest, parse, ProblemError, toProblem } from '../../src/http/problem';
import { stationTime } from '../../src/ocpp/ocpp.service';

describe('problem+json mapping', () => {
  it('ProblemError keeps status, code and detail', () => {
    expect(toProblem(new ProblemError(409, 'connector_unavailable', 'Connector unavailable', 'taken'))).toEqual({ type: 'about:blank', title: 'Connector unavailable', status: 409, code: 'connector_unavailable', detail: 'taken' });
  });
  it('maps idempotency errors and framework exceptions; hides internals of unknown errors', () => {
    expect(toProblem(new IdempotencyMismatchError('k'))).toMatchObject({ status: 422, code: 'idempotency_key_reused' });
    expect(toProblem(new IdempotencyInFlightError('k'))).toMatchObject({ status: 409, code: 'idempotency_in_flight' });
    expect(toProblem(new HttpException('nope', 401))).toMatchObject({ status: 401, code: 'unauthorized' });
    expect(toProblem(new Error('db password is hunter2'))).toEqual({ type: 'about:blank', title: 'Internal error', status: 500, code: 'internal' });
  });
  it('parse throws a 400 listing every issue', () => {
    const schema = z.object({ a: z.number(), b: z.string() });
    expect(() => parse(schema, { a: 'x' })).toThrow(ProblemError);
    expect(() => parse(schema, { a: 'x' })).toThrow(/a: .*; b: /);
    const err = (() => {
      try {
        return parse(schema, { a: 'x' });
      } catch (e) {
        return e as ProblemError;
      }
    })();
    expect(err).toMatchObject({ status: 400, code: 'bad_request' });
    expect(badRequest('x').code).toBe('bad_request');
  });
});

describe('reservation error mapping (ADR 0005)', () => {
  it('23P01 exclusion_violation -> 409 connector_unavailable', () => {
    expect(mapReservationError({ code: '23P01' })).toMatchObject({ status: 409, code: 'connector_unavailable' });
  });
  it('one-open-per-driver unique violation -> 409 reservation_exists, also for the boundary trigger name', () => {
    expect(mapReservationError({ code: '23505', constraint: 'reservations_2026_11_one_open_per_driver' })).toMatchObject({ status: 409, code: 'reservation_exists' });
    expect(mapReservationError({ code: '23505', constraint: 'reservations_one_open_per_driver' })).toMatchObject({ status: 409, code: 'reservation_exists' });
  });
  it('anything else passes through', () => {
    const e = { code: '40001' };
    expect(mapReservationError(e)).toBe(e);
  });
});

describe('catalog helpers', () => {
  it('boundingBox contains the circle at Berlin latitude', () => {
    const b = boundingBox(52.52, 13.405, 5);
    expect(b.maxLat - b.minLat).toBeCloseTo(2 * (5 / 111.32), 3);
    expect(b.maxLng - b.minLng).toBeGreaterThan(b.maxLat - b.minLat);
  });
  it('boundingBox does not blow up at the poles', () => {
    const b = boundingBox(90, 0, 5);
    expect(Number.isFinite(b.minLng) && Number.isFinite(b.maxLng)).toBe(true);
  });
});

describe('stationTime', () => {
  const now = Date.parse('2026-11-08T10:00:00Z');
  it('keeps plausible station timestamps (offline replays up to 24 h old)', () => {
    expect(stationTime('2026-11-07T10:00:00Z', now)).toEqual({ at: new Date('2026-11-07T10:00:00Z'), clamped: false });
  });
  it('replaces garbage or wildly wrong clocks by now', () => {
    expect(stationTime('2001-01-01T00:00:00Z', now)).toEqual({ at: new Date(now), clamped: true });
    expect(stationTime('2099-01-01T00:00:00Z', now).clamped).toBe(true);
    expect(stationTime('not a date', now).clamped).toBe(true);
  });
});

describe('config (fail fast)', () => {
  const ok = {
    INTERNAL_TOKEN: 'a-long-enough-internal-token',
    DATABASE_URL: 'postgres://u:p@h/db',
    KAFKA_BROKERS: 'a:9092,b:9092',
    REDIS_URL: 'redis://r:6379',
    SMART_CHARGING_URL: 'http://smart-charging:8080',
    STATION_SECRET_SEED: 'seed-seed-seed',
  };
  it('parses with defaults', () => {
    const c = readConfig(ok);
    expect(c).toMatchObject({ SERVICE_NAME: 'sessions', SMART_CHARGING_TIMEOUT_MS: 300, ALLOCATION_PERIOD_S: 60, SWEEPER_INTERVAL_MS: 5000, SEED_SITES: 20, WORKERS_ENABLED: true });
    expect(c.KAFKA_BROKERS).toEqual(['a:9092', 'b:9092']);
  });
  it('lists every bad variable at once', () => {
    expect(() => readConfig({ ...ok, DATABASE_URL: 'mysql://x', REDIS_URL: 'nope', STATION_SECRET_SEED: 'short', SEED_DC_SHARE: '2' })).toThrow(/DATABASE_URL[\s\S]*REDIS_URL|REDIS_URL[\s\S]*DATABASE_URL/);
    expect(() => readConfig({})).toThrow(/INTERNAL_TOKEN/);
  });
});
