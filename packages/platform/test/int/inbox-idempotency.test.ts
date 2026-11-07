import { Body, Controller, HttpCode, Module, Post, UseInterceptors } from '@nestjs/common';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { HealthRegistry } from '../../src/health/health';
import {
  beginIdempotent,
  hashRequest,
  IDEMPOTENCY_KEYS_DDL,
  IdempotencyInFlightError,
  IdempotencyMismatchError,
  purgeIdempotencyKeys,
  withIdempotency,
} from '../../src/idempotency/idempotency';
import { IdempotencyInterceptor, IdempotencyModule } from '../../src/idempotency/idempotency.interceptor';
import { PROCESSED_EVENTS_DDL, purgeProcessedEvents, withInbox } from '../../src/inbox/inbox';
import { bootstrapService, type RunningService } from '../../src/lifecycle/bootstrap';
import { withTx } from '../../src/postgres/postgres';
import { captureLogger } from '../helpers';
import { tempDatabase } from './infra';

let db: Awaited<ReturnType<typeof tempDatabase>>;
const uuid = () => crypto.randomUUID();

beforeAll(async () => {
  db = await tempDatabase();
  await db.pool.query(PROCESSED_EVENTS_DDL + IDEMPOTENCY_KEYS_DDL + 'CREATE TABLE effects (id serial PRIMARY KEY, note text);');
});
afterAll(() => db.drop());

describe('withInbox', () => {
  it('runs fn once per (consumer, eventId)', async () => {
    const id = uuid();
    const r1 = await withTx(db.pool, (c) => withInbox(c, 'billing', id, async () => (await c.query("INSERT INTO effects (note) VALUES ('inbox') RETURNING id")).rows[0]));
    const r2 = await withTx(db.pool, (c) => withInbox(c, 'billing', id, async () => 'should not run'));
    const r3 = await withTx(db.pool, (c) => withInbox(c, 'other-consumer', id, async () => 'ran'));
    expect(r1.duplicate).toBe(false);
    expect(r2).toEqual({ duplicate: true });
    expect(r3).toEqual({ duplicate: false, result: 'ran' });
    expect((await db.pool.query("SELECT 1 FROM effects WHERE note = 'inbox'")).rowCount).toBe(1);
  });

  it('a rolled-back attempt leaves no marker, so the retry runs', async () => {
    const id = uuid();
    await expect(withTx(db.pool, (c) => withInbox(c, 'c', id, async () => Promise.reject(new Error('fail'))))).rejects.toThrow('fail');
    expect(await withTx(db.pool, (c) => withInbox(c, 'c', id, async () => 1))).toEqual({ duplicate: false, result: 1 });
  });

  it('concurrent duplicates: exactly one runs', async () => {
    const id = uuid();
    let runs = 0;
    const results = await Promise.all(
      Array.from({ length: 5 }, () =>
        withTx(db.pool, (c) =>
          withInbox(c, 'conc', id, async () => {
            runs++;
            await new Promise((r) => setTimeout(r, 50));
          }),
        ),
      ),
    );
    expect(runs).toBe(1);
    expect(results.filter((r) => r.duplicate)).toHaveLength(4);
    expect(await purgeProcessedEvents(db.pool, 0)).toBeGreaterThan(0);
  });
});

const req = (key: string, body: unknown) => ({ scope: 'POST /charges', key, requestHash: hashRequest('POST', '/charges', body) });

describe('withIdempotency', () => {
  it('replays the stored response for the same key and body', async () => {
    const key = uuid();
    let runs = 0;
    const fn = async () => {
      runs++;
      return { status: 201, body: { id: runs } };
    };
    expect(await withIdempotency(db.pool, req(key, { a: 1 }), fn)).toEqual({ replayed: false, response: { status: 201, body: { id: 1 } } });
    expect(await withIdempotency(db.pool, req(key, { a: 1 }), fn)).toEqual({ replayed: true, response: { status: 201, body: { id: 1 } } });
    expect(runs).toBe(1);
  });

  it('rejects the same key with a different body (422)', async () => {
    const key = uuid();
    await withIdempotency(db.pool, req(key, { a: 1 }), async () => ({ status: 200, body: null }));
    await expect(withIdempotency(db.pool, req(key, { a: 2 }), async () => ({ status: 200, body: null }))).rejects.toBeInstanceOf(IdempotencyMismatchError);
  });

  it('rejects a concurrent in-flight duplicate (409) and takes over stale claims', async () => {
    const key = uuid();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const first = withIdempotency(db.pool, req(key, {}), async () => {
      await gate;
      return { status: 200, body: 'first' };
    });
    await new Promise((r) => setTimeout(r, 50));
    await expect(withIdempotency(db.pool, req(key, {}), async () => ({ status: 200, body: 'second' }))).rejects.toBeInstanceOf(IdempotencyInFlightError);
    release();
    expect((await first).response.body).toBe('first');

    const stale = uuid();
    expect(await beginIdempotent(db.pool, { ...req(stale, {}) })).toEqual({ kind: 'new' });
    await new Promise((r) => setTimeout(r, 30));
    expect(await beginIdempotent(db.pool, { ...req(stale, {}), lockTtlMs: 10 })).toEqual({ kind: 'new' });
  });

  it('stores the response atomically with the side effects; errors release the key', async () => {
    const key = uuid();
    await expect(
      withIdempotency(db.pool, req(key, {}), async (c) => {
        await c.query("INSERT INTO effects (note) VALUES ('rolled-back')");
        throw new Error('handler failed');
      }),
    ).rejects.toThrow('handler failed');
    expect((await db.pool.query("SELECT 1 FROM effects WHERE note = 'rolled-back'")).rowCount).toBe(0);
    const ok = await withIdempotency(db.pool, req(key, {}), async (c) => {
      await c.query("INSERT INTO effects (note) VALUES ('committed')");
      return { status: 201, body: { ok: true } };
    });
    expect(ok.replayed).toBe(false);
    expect(await purgeIdempotencyKeys(db.pool, 3_600_000)).toBe(0);
  });
});

describe('IdempotencyInterceptor', () => {
  let svc: RunningService;
  let runs = 0;

  beforeAll(async () => {
    @Controller('charges')
    class ChargesController {
      @Post()
      @HttpCode(202)
      @UseInterceptors(IdempotencyInterceptor)
      async create(@Body() body: { amount: number; slow?: boolean }) {
        runs++;
        if (body.slow) await new Promise((r) => setTimeout(r, 300));
        return { chargeId: `ch_${runs}`, amount: body.amount };
      }
    }
    @Module({ imports: [IdempotencyModule.forRoot({ pool: db.pool })], controllers: [ChargesController] })
    class AppModule {}
    svc = await bootstrapService({
      module: AppModule,
      config: { SERVICE_NAME: 'idem-test', LOG_LEVEL: 'warn', HTTP_PORT: 0, INTERNAL_TOKEN: 'x'.repeat(16), SHUTDOWN_GRACE_MS: 2000 },
      logger: captureLogger().logger,
      health: new HealthRegistry(),
      handleSignals: false,
      drainDelayMs: 0,
    });
  });
  afterAll(() => svc.shutdown());

  const post = (key: string | undefined, body: unknown) =>
    fetch(`${svc.url}/charges`, { method: 'POST', headers: { 'content-type': 'application/json', ...(key ? { 'idempotency-key': key } : {}) }, body: JSON.stringify(body) });

  it('replays status and body for a repeated key', async () => {
    const key = uuid();
    const a = await post(key, { amount: 5 });
    const b = await post(key, { amount: 5 });
    expect(a.status).toBe(202);
    expect(b.status).toBe(202);
    expect(b.headers.get('idempotent-replayed')).toBe('true');
    expect(await b.json()).toEqual(await a.json());
  });

  it('422 for a different body, 409 while in flight, pass-through without key', async () => {
    const key = uuid();
    await post(key, { amount: 1 });
    expect((await post(key, { amount: 2 })).status).toBe(422);

    const k2 = uuid();
    const slow = post(k2, { amount: 1, slow: true });
    await new Promise((r) => setTimeout(r, 100));
    const dup = await post(k2, { amount: 1, slow: true });
    expect(dup.status).toBe(409);
    expect((await slow).status).toBe(202);

    const before = runs;
    await post(undefined, { amount: 1 });
    await post(undefined, { amount: 1 });
    expect(runs).toBe(before + 2);
  });
});
