# Production-readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the mandatory production gaps M1–M14 and correct the qualified numbers N1–N11 from the 2026-11-10 review.

**Architecture:** Every fix lands in the service that owns the state, following the patterns already in the repo:
migrations in `services/<svc>/migrations/NNNN_*.sql`, events through the transactional outbox (`emitSessionEvent`,
`SagaEngine.emit`), sweepers in `services/sessions/src/workers/maintenance.ts`, contracts in `packages/contracts`
(TS, zod) and `packages/contracts-go`. No new services except Alertmanager.

**Tech Stack:** Node 26, TypeScript 7, NestJS 12, vitest 5, Go 1.27, PostgreSQL 18, TimescaleDB 2.31, Kafka 4.3,
HAProxy, Prometheus 3.16, Alertmanager.

**Spec:** [`docs/superpowers/specs/2026-11-10-prod-readiness-brief.md`](../specs/2026-11-10-prod-readiness-brief.md)

## Global Constraints

- `make check` is the definition of green (Makefile header). Run it before the last commit of every phase.
- Work on branch `prod-readiness`, never on `main`. One commit per task, conventional prefix (`feat:`, `fix:`, `docs:`).
- Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Migrations are append-only: add a new numbered file, never edit an applied one.
- Money is integer minor units (`*_minor`), energy integer Wh, power integer W. No floats for money.
- Every contract change in `packages/contracts/src/*.ts` must be backward compatible: new fields are `.optional()`.
  After a change run `pnpm --filter @evcharge/contracts build` and commit the regenerated `packages/contracts/schemas`.
- Mirror every changed contract struct in `packages/contracts-go/contracts.go` and extend `contract_test.go`.
- Integration tests need the compose infra: `make infra-up` once, then `pnpm --filter @evcharge/<svc> test:int`.
- Docs numbers in generated blocks (`<!-- *:start -->`) come from `node scripts/charts/build.mjs`; never edit them by hand.
- ADR 0000 rule: a missed target is amended with the measured number, not quietly lowered.
- Charging never stops because the backend is down (ADR 0000 "Degradation"). Any new stop path must fire only on a
  definite answer (decline, implausible data), never on a timeout or an unavailable dependency.

## Execution split (two sessions in parallel)

Split by file ownership so the two branches merge without conflicts in `saga-engine.ts` and the billing controller.

| Session | Checkout / branch | Tasks, in this order |
|---|---|---|
| Sonnet | main checkout `/Users/andrii.prisniak/projects/evcharge-lab`, branch `prod-readiness` | 1 (skip Step 1, the branch exists), 2, 4, 5, 6, 13, 14, 15, 18, 16, 17, 19 |
| Opus | worktree `/Users/andrii.prisniak/projects/evcharge-lab-opus`, branch `prod-readiness-opus` | 11, 9, 7, 10, then merge `prod-readiness` (needs Task 2), then 3, 8, 12, 20, 21, then 22 after the final merge |

Rules for both sessions:
- Never edit files under `services/billing/` (Sonnet) or `services/sessions/`, `services/station-gateway/`, `infra/` (Opus, except Task 21's one sessions endpoint, done after the merge).
- Migration numbers are fixed by the plan; do not renumber. Billing: 0005 (T3), 0006 (T7), 0007 (T9), 0008 (T10), 0009 (T20). Sessions: 0005 (T2), 0006 (T13), 0007 (T15).
- `packages/contracts` is shared: touch only the schema the task names; after a merge, run `pnpm --filter @evcharge/contracts build` and commit regenerated `schemas/` if they differ.
- Both sessions use the same compose infra. Integration tests use temp databases and unique topics, so they can run in parallel. Do **not** run `make check`, `make infra-up` or `docker compose stop/down` while the other session may be testing; run the targeted `pnpm --filter … test:int` / `go test` instead. Task 16/17 end-to-end steps: tell the other session before running them.
- Commit after every task; do not push.

## Review Focus

1. **PSP unavailable vs declined.** `authFailed(row, 'psp_unavailable')` must not stop charging (Task 6); only a
   definite decline does. Test pinned in Task 6.
2. **Replayed stop after the reaper closed the session.** A StopTransaction arriving after Task 4 closed the session
   must be accepted (CALLRESULT, no 500) and must not emit a second `session.completed`. Test pinned in Task 4.
3. **Invoice numbers under concurrent sagas.** Two replicas issuing invoices at once must never produce a gap or a
   duplicate number. Test pinned in Task 9.
4. **Energy profile with too few samples.** A session with 0 or 1 meter samples must fall back to time-weighted
   apportioning, not fail the quote. Test pinned in Task 11.
5. **Late result after the Stopping sweeper.** A RemoteStop `Accepted` arriving after Task 5 reverted the session to
   Active must not break: the subsequent StopTransaction completes it normally. Test pinned in Task 5.

---

## Phase A — Numbers and docs (N1–N11)

### Task 1: Correct and qualify the numbers in ADR 0000 and known limits

**Files:**
- Modify: `docs/adr/0000-requirements-and-capacity.md`
- Modify: `docs/known-limits.md`
- Modify: `README.md` (the prose line under "## Results" only, not the generated block)

**Interfaces:** none (docs only).

- [ ] **Step 1: Create the branch**

Run: `git switch -c prod-readiness`
Expected: `Switched to a new branch 'prod-readiness'`

- [ ] **Step 2: Fleet label (N1)**

In ADR 0000 "Context", replace "plausible for a mid-sized German charge point operator (CPO)" with
"sized for a large CPMS platform serving several German CPOs (50,000 stations is more than any single German CPO
runs; verify the national total before quoting it)".

- [ ] **Step 3: Session length and sessions/day (N2, N3)**

In "Fleet and usage", append to the Session length row: "conservative: AC sessions typically last 2–4 h, so this
overstates the start rate about 3×; kept as headroom". Below the Traffic table add one line:
"Sessions per day (~320,000) against 200,000 daily active drivers implies 1.6 sessions per active driver per day,
higher than typical; both inputs are assumptions and the session rate is the conservative one."

- [ ] **Step 4: Inbound OCPP total (N4)**

Add a Traffic row before **Inbound OCPP total**:
`| CALLRESULT for outbound commands (SetChargingProfile) | | ~900/s | = SetChargingProfile outbound | [derived] |`
Change the total row's peak cell to `**~2,500/s, design 3,750/s (×1.5)**` and its formula to
`sum, including command CALLRESULTs`. In "Lab targets" change the gateway fleet need to `50,000 conns, 3,750 msg/s`
and add after the table: "Per instance with 5 surviving of 6: 750 msg/s [derived: 3,750 / 5]."
Update the same figure in `docs/known-limits.md` ("2,400 msg/s design peak spread over 5 surviving instances is
480 msg/s") to 3,750 and 750.

- [ ] **Step 5: Retention (N5)**

In "Data volume", change both "10 years (German tax law)" cells to
"10 years [assumption; verify: since 2025 BEG IV, accounting vouchers may be 8 years]".

- [ ] **Step 6: Measurement qualifications (N6–N11)**

In `docs/known-limits.md` under "**Other limits of the measurements**", add these bullets verbatim in meaning:
- 10,000 connections were measured on a native process with a stub `sessions` and no HAProxy; 10,000 connections
  and 2,000 msg/s were never measured together.
- The driver-api read run used 30 query keys against a 3 s in-process cache (about 99% cache hits) and 20 seeded
  sites; the model has 5,000 sites. The 955.59 RPS is a cache-path number.
- Every k6 run is 60 s, the telemetry bench 20 s per step, allocbench 30 s, one run each: autovacuum, checkpoints and
  compression jobs do not appear in them.
- The smart-charging open-loop p99 of 19.45 ms is one 30 s run with p99.9 55.8 ms: met, at the limit.
- The sessions k6 script stops each transaction at once, so `active_sessions` stays near empty; the database size
  during the run is not recorded.
- No service, Postgres or Kafka container has CPU or memory limits, so "one instance" used as many VM cores as it
  took; the N+1 instance counts in ADR 0000 assume a bounded instance.

- [ ] **Step 7: Verify generated blocks are untouched**

Run: `node scripts/charts/build.mjs && git status --short`
Expected: only the three files of this task modified.

- [ ] **Step 8: Commit**

```bash
git add docs/adr/0000-requirements-and-capacity.md docs/known-limits.md README.md
git commit -m "docs: correct OCPP peak, qualify fleet, session and measurement numbers"
```

---

## Phase B — Session lifecycle (M3 part 1, M7, M8)

### Task 2: Energy plausibility flag on completed sessions

**Files:**
- Create: `services/sessions/migrations/0005_energy_flag.sql`
- Modify: `services/sessions/src/ocpp/ocpp.service.ts` (`complete()`, around lines 225–270)
- Create: `services/sessions/src/energy.ts`
- Modify: `packages/contracts/src/events.ts` (`SessionCompleted`, line 123)
- Test: `services/sessions/test/unit/energy.test.ts`, `services/sessions/test/int/ocpp.test.ts`

**Interfaces:**
- Produces: `export type EnergyFlag = 'ok' | 'meter_regressed' | 'implausible' | 'stale'` and
  `export function energyFlag(meterStartWh: number, meterStopWh: number, startedAt: Date, stoppedAt: Date, maxPowerW: number): EnergyFlag` in `services/sessions/src/energy.ts`.
- Produces: `SessionCompleted.data.energyFlag?: EnergyFlag` (zod `z.enum([...]).optional()`), consumed by Task 3.
- Produces: column `sessions.energy_flag text NULL`.

- [ ] **Step 1: Write the failing unit test**

```ts
import { describe, expect, it } from 'vitest';
import { energyFlag } from '../../src/energy';
const t0 = new Date('2026-11-10T10:00:00Z');
const h = (n: number) => new Date(t0.getTime() + n * 36e5);
describe('energyFlag', () => {
  it('ok within max power x duration', () => expect(energyFlag(1000, 12000, t0, h(1), 11000)).toBe('ok'));
  it('meter_regressed when stop < start', () => expect(energyFlag(5000, 4000, t0, h(1), 11000)).toBe('meter_regressed'));
  it('implausible above 1.1 x max power x duration', () => expect(energyFlag(0, 12_200, t0, h(1), 11000)).toBe('implausible'));
  it('ok exactly at the 1.1 bound', () => expect(energyFlag(0, 12_100, t0, h(1), 11000)).toBe('ok'));
  it('zero-length session with energy is implausible', () => expect(energyFlag(0, 10, t0, t0, 11000)).toBe('implausible'));
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pnpm --filter @evcharge/sessions test:unit -- energy`
Expected: FAIL, cannot resolve `../../src/energy`.

- [ ] **Step 3: Implement `energyFlag` in `services/sessions/src/energy.ts`**

Bound = `maxPowerW * durationMs / 3_600_000 * 1.1`, computed in integers (`maxPowerW * durationMs * 11 / 36_000_000`).
`'stale'` is never returned here; Task 4 sets it.

- [ ] **Step 4: Run the unit test to verify it passes**

Run: `pnpm --filter @evcharge/sessions test:unit -- energy`
Expected: PASS (5 tests).

- [ ] **Step 5: Migration and wiring**

`0005_energy_flag.sql`: `ALTER TABLE sessions ADD COLUMN energy_flag text NULL CHECK (energy_flag IN ('ok','meter_regressed','implausible','stale'));`
In `complete()`, compute the flag from `a.started_at`, `stoppedAt`, `a.meter_start_wh` (select it), `meterStopWh` and
`a.max_w` (already on `active_sessions`), write it in the same `UPDATE sessions`, and add `energyFlag` to the
`session.completed` payload. Add the optional field to `SessionCompleted` in contracts and rebuild schemas.

- [ ] **Step 6: Write the failing integration test** (append to `ocpp.test.ts`)

```ts
it('flags a regressed meter and an implausible energy on session.completed', async () => {
  const d = await h.registerDriver();
  const s = await h.startTx('st-0001-003', 1, d.idTag);          // harness starts with meterStartWh = 0
  await h.stopTx('st-0001-003', s.body.transactionId, undefined, 99_999_999);
  const ev = await waitFor(() => types('session.completed', s.body.sessionId)[0]);
  expect((ev.data as { energyFlag?: string }).energyFlag).toBe('implausible');
});
```

Check `h.startTx` in `harness.ts` for the meterStartWh it sends and adjust the expected flag only if it differs.

- [ ] **Step 7: Run it and the existing suite**

Run: `pnpm --filter @evcharge/sessions test:int -- ocpp`
Expected: PASS, including the existing tests.

- [ ] **Step 8: Commit**

```bash
git add services/sessions packages/contracts
git commit -m "feat(sessions): flag regressed and implausible session energy"
```

### Task 3: Billing parks flagged sessions for review instead of charging

**Files:**
- Modify: `services/billing/src/sagas/saga-engine.ts` (`onSessionCompleted` line 85, `stepPrice` line 247)
- Create: `services/billing/migrations/0005_energy_flag.sql`
- Test: `services/billing/test/int/saga.test.ts`

**Interfaces:**
- Consumes: `SessionCompleted.data.energyFlag` (Task 2).
- Produces: column `billing_sagas.energy_flag text NULL`; a saga with a flag other than `ok`/null stays in state
  `PRICING` with `next_attempt_at = NULL`, `failure_reason = 'energy_review:<flag>'`, and emits
  `billing.payment-failed` with `step: 'pricing'`. Do **not** call `park()`: for a PRICING saga with a hold it moves
  the saga to `CAPTURE_FAILED` (`saga-engine.ts:531`), which Task 8 could not resume. The hold stays on the card
  until an operator resumes (Task 8) or the PSP expires it; that is the intended trade-off for a review case.
- Produces: `private async holdForReview(row: SagaRow, reason: string): Promise<void>` in `SagaEngine`, guarded by
  `WHERE session_id = $1 AND state = 'PRICING'` and throwing `StateChanged` on 0 rows, like `park()`.

- [ ] **Step 1: Write the failing test** (new `describe` in `saga.test.ts`, using `completedEvent` from `harness.ts`)

```ts
it('holds a session flagged implausible for review in PRICING, issues no invoice', async () => {
  const ev = completedEvent(9101, driverIdOf(1), 5000);
  ev.data.energyFlag = 'implausible';
  // publish started + completed as the other tests do, then:
  await waitFor(async () => (await h.db.pool.query("SELECT 1 FROM billing_sagas WHERE session_id = $1 AND state = 'PRICING' AND next_attempt_at IS NULL AND failure_reason = 'energy_review:implausible'", [sessionIdOf(9101)])).rowCount);
  expect((await h.db.pool.query('SELECT 1 FROM invoice_registry WHERE session_id = $1', [sessionIdOf(9101)])).rowCount).toBe(0);
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pnpm --filter @evcharge/billing test:int -- saga`
Expected: the new test FAILS (an invoice is issued).

- [ ] **Step 3: Implement**

Migration adds the column. `onSessionCompleted` stores `d.energyFlag ?? null`. At the top of `stepPrice`, if
`row.energy_flag` is set and not `'ok'`, `return this.holdForReview(row, \`energy_review:${row.energy_flag}\`)`.
Add `energy_flag: string | null` to `SagaRow`.

- [ ] **Step 4: Run the billing suite**

Run: `pnpm --filter @evcharge/billing test:int`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/billing
git commit -m "feat(billing): park sessions with implausible energy for review"
```

### Task 4: Reaper for sessions that never received StopTransaction (M7)

**Files:**
- Modify: `services/sessions/src/workers/maintenance.ts`
- Modify: `services/sessions/src/config.ts` (new `STALE_SESSION_AFTER_H`, default `48`)
- Modify: `services/sessions/src/ocpp/ocpp.service.ts` (make `complete()` callable: export a free function
  `completeSession(c, topics, active, meterStopWh, stoppedAt, reason, flag?)` and have the class method delegate)
- Test: `services/sessions/test/int/ocpp.test.ts`

**Interfaces:**
- Consumes: `EnergyFlag` (Task 2).
- Produces: `export async function closeStaleSessions(pool: Pool, topics: TopicNames, staleAfterMs: number): Promise<number>`
  in `maintenance.ts`, run by the existing `sweeper` timer.
- Behaviour: an `active_sessions` row whose `started_at < now() - staleAfter` is deleted and completed with
  `reason = 'StaleClosed'`, `meterStopWh = meter_start_wh` (energy 0), `energyFlag = 'stale'`, `stoppedAt = now()`,
  in one transaction, `FOR UPDATE SKIP LOCKED`, batches of `SWEEP_BATCH`. 48 h = 24 h offline window (ADR 0000) × 2.

- [ ] **Step 1: Write the failing tests**

```ts
it('closes a stale session as StaleClosed, and a late StopTransaction is accepted without a second completion', async () => {
  const d = await h.registerDriver();
  const s = await h.startTx('st-0001-004', 1, d.idTag);
  const { transactionId, sessionId } = s.body;
  // Age only active_sessions: sessions.started_at is the partition key and the reaper selects on active_sessions.
  await h.db.pool.query("UPDATE active_sessions SET started_at = now() - interval '49 hours' WHERE transaction_id = $1", [transactionId]);
  expect(await closeStaleSessions(h.db.pool, h.topics, 48 * 36e5)).toBe(1);
  const ev = await waitFor(() => types('session.completed', sessionId)[0]);
  expect(ev.data).toMatchObject({ stopReason: 'StaleClosed', energyWh: 0, energyFlag: 'stale' });

  // Review Focus 2
  const r = await h.stopTx('st-0001-004', transactionId, undefined, 20_000);
  expect(r.status).toBe(200);
  await new Promise((res) => setTimeout(res, 500));
  expect(types('session.completed', sessionId)).toHaveLength(1);
});
```

`complete()` updates `sessions` by `(session_id, started_at)` using the `active_sessions.started_at` value; the
aged value no longer matches. In `closeStaleSessions` read `sessions.started_at` by `session_id` (join) and pass
that, not the active row's value.

- [ ] **Step 2: Run to verify they fail**

Run: `pnpm --filter @evcharge/sessions test:int -- ocpp`
Expected: FAIL, `closeStaleSessions` is not exported.

- [ ] **Step 3: Implement `closeStaleSessions` and the config entry; call it in the `sweeper` task of `Maintenance.start()`**

`Maintenance` gets `staleAfterMs` as a constructor parameter; pass `config.STALE_SESSION_AFTER_H * 36e5` from `app.ts`.

- [ ] **Step 4: Run the sessions integration suite**

Run: `pnpm --filter @evcharge/sessions test:int`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/sessions
git commit -m "feat(sessions): close sessions without StopTransaction after 48 h"
```

### Task 5: Recover sessions stuck in Stopping (M8)

**Files:**
- Modify: `services/sessions/src/workers/maintenance.ts`
- Modify: `services/station-gateway/src/gateway.ts:421` (stop swallowing the publish failure)
- Modify: `services/station-gateway/src/metrics.ts` (or wherever `this.metrics` is defined; grep `commands:`)
- Test: `services/sessions/test/int/ocpp.test.ts`

**Interfaces:**
- Produces: `export async function revertUnconfirmedStops(pool: Pool): Promise<number>` in `maintenance.ts`, run by
  the `sweeper` timer. Pattern: copy `rejectUnconfirmedReservations` (same file): join `sessions` (status
  `'Stopping'`) to `commands` (`action = 'RemoteStopTransaction'`, `outcome IS NULL`,
  `expires_at + RESULT_GRACE_S < now()`), set `commands.outcome = 'Expired'`, set `sessions.status = 'Active'`.
- Produces: gateway counter `gateway_command_result_publish_failed_total{action}` incremented in the `.catch` that is
  now `.catch(() => {})`, plus an `error` log with `commandId`.

- [ ] **Step 1: Write the failing test**

```ts
it('reverts a Stopping session whose RemoteStop got no result, so the driver can stop again', async () => {
  const d = await h.registerDriver();
  const s = await h.startTx('st-0001-001', 2, d.idTag);
  const api = h.driver(d.driverId);
  expect((await api('POST', `/v1/sessions/${s.body.sessionId}/stop`)).body.status).toBe('Stopping');
  await h.db.pool.query("UPDATE commands SET expires_at = now() - interval '1 minute' WHERE action = 'RemoteStopTransaction' AND outcome IS NULL");
  expect(await revertUnconfirmedStops(h.db.pool)).toBe(1);
  expect((await api('GET', `/v1/sessions/${s.body.sessionId}`)).body.status).toBe('Active');
  expect((await api('POST', `/v1/sessions/${s.body.sessionId}/stop`)).body.status).toBe('Stopping');
  // Review Focus 5: the station's StopTransaction still completes it
  expect((await h.stopTx('st-0001-001', s.body.transactionId)).status).toBe(200);
});
```

Check the commands table for the session columns the handler uses (`session_id`, `session_started_at`) before
writing the join.

- [ ] **Step 2: Run to verify it fails**

Run: `pnpm --filter @evcharge/sessions test:int -- ocpp`
Expected: FAIL, `revertUnconfirmedStops` is not exported.

- [ ] **Step 3: Implement `revertUnconfirmedStops`, wire it into the sweeper, add the gateway counter and log**

- [ ] **Step 4: Run both suites**

Run: `pnpm --filter @evcharge/sessions test:int && pnpm --filter @evcharge/station-gateway test:unit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/sessions services/station-gateway
git commit -m "fix(sessions): revert Stopping when RemoteStop never got a result"
```

### Task 6: A declined authorization stops the charging session (M5)

**Files:**
- Create: `services/sessions/src/consumers/billing-handler.ts`
- Modify: `services/sessions/src/app.ts` (topics block line 118: add `billingEvents`; start one more consumer the
  way the existing consumers are created)
- Modify: `services/sessions/src/config.ts` (`STOP_ON_AUTH_DECLINE`, boolean, default `true`)
- Test: `services/sessions/test/int/billing-stop.test.ts`

**Interfaces:**
- Consumes: `billing.payment-failed` events from `Topics.BillingEvents` (check the exact constant in
  `packages/contracts/src/topics.ts`), `data.step === 'authorize'`, `data.sessionId`, `data.reason`.
- Produces: `export class BillingEventsHandler { constructor(pool: Pool, topics: AppTopics, enabled: boolean, logger: Logger); handle(e: events.BillingEvent): Promise<void> }`.
  On `payment-failed` with `step === 'authorize'` and `reason !== 'psp_unavailable'`: inside the inbox
  transaction, `UPDATE sessions SET status = 'Stopping' WHERE session_id = $1 AND status = 'Active'` and
  `enqueueCommand(c, topics, station_id, 'RemoteStopTransaction', { transactionId }, { sessionId, sessionStartedAt, driverId })`
  exactly as `DriverService.stopSession` does. Increment `sessions_stopped_on_auth_decline_total`.
- Before writing the reason check, read `stepAuthorize` in `services/billing/src/sagas/saga-engine.ts` (around
  line 200) and list every reason string passed to `authFailed`; only the decline reasons stop.

- [ ] **Step 1: Write the failing tests** (`billing-stop.test.ts`, harness with a `billingEvents` topic)

```ts
it('sends RemoteStop when the authorization is declined', async () => {
  const s = await startSession();               // helper: registerDriver + startTx
  await publishBilling('billing.payment-failed', { invoiceId: null, sessionId: s.sessionId, step: 'authorize', reason: 'declined' });
  await waitFor(() => h.commands.find((c) => c.data.action === 'RemoteStopTransaction' && c.data.payload.transactionId === s.transactionId));
});
it('keeps charging when the PSP was unavailable (ADR 0000 degradation)', async () => {
  const s = await startSession();
  await publishBilling('billing.payment-failed', { invoiceId: null, sessionId: s.sessionId, step: 'authorize', reason: 'psp_unavailable' });
  await new Promise((r) => setTimeout(r, 1000));
  expect(h.commands.some((c) => c.data.action === 'RemoteStopTransaction' && c.data.payload.transactionId === s.transactionId)).toBe(false);
});
```

Extend `HarnessOptions.topics` / `Harness.topics` with `billingEvents` and add `publishBilling` to the harness.

- [ ] **Step 2: Run to verify they fail**

Run: `pnpm --filter @evcharge/sessions test:int -- billing-stop`
Expected: the first test FAILS (timeout in `waitFor`).

- [ ] **Step 3: Implement the handler, config flag and consumer wiring**

- [ ] **Step 4: Run the sessions suite**

Run: `pnpm --filter @evcharge/sessions test:int`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/sessions
git commit -m "feat(sessions): stop charging when the payment authorization is declined"
```

---

## Phase C — Billing (M2, M6)

### Task 7: Capture retry before the final decline (M6 part 1)

**Files:**
- Modify: `services/billing/src/domain.ts`
- Modify: `services/billing/src/sagas/saga-engine.ts` (`stepCapture` line 326, decline path line 444–480)
- Modify: `services/billing/src/config.ts` (`CAPTURE_RETRY_DELAYS_H`, default `"24,72"`)
- Create: `services/billing/migrations/0006_capture_retry.sql` (`ALTER TABLE billing_sagas ADD COLUMN capture_round integer NOT NULL DEFAULT 0;`)
- Test: `services/billing/test/unit/domain.test.ts`, `services/billing/test/int/psp-edge.test.ts`

**Interfaces:**
- Produces: `export function nextCaptureRetryAt(round: number, declinedAt: Date, delaysH: number[]): Date | null`
  in `domain.ts` (null when `round >= delaysH.length`).
- Produces: capture idempotency keys gain the round: existing key + `:r${capture_round}` when `capture_round > 0`
  (round 0 keeps today's key so in-flight sagas are not affected).
- Behaviour: a definite capture decline with `nextCaptureRetryAt(...) !== null` keeps state `INVOICED`, increments
  `capture_round`, sets `next_attempt_at` to that date and emits nothing; with `null` it takes today's
  `CAPTURE_FAILED` path unchanged.

- [ ] **Step 1: Write the failing unit test**

```ts
it('schedules capture retries at 24 h and 72 h, then gives up', () => {
  const t = new Date('2026-11-10T00:00:00Z');
  expect(nextCaptureRetryAt(0, t, [24, 72])?.toISOString()).toBe('2026-11-11T00:00:00.000Z');
  expect(nextCaptureRetryAt(1, t, [24, 72])?.toISOString()).toBe('2026-11-13T00:00:00.000Z');
  expect(nextCaptureRetryAt(2, t, [24, 72])).toBeNull();
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pnpm --filter @evcharge/billing test:unit -- domain`
Expected: FAIL, `nextCaptureRetryAt` is not exported.

- [ ] **Step 3: Implement `nextCaptureRetryAt`; run the unit test**

Expected: PASS.

- [ ] **Step 4: Write the failing integration test** (in `psp-edge.test.ts`, with the mock PSP set to decline captures)

```ts
it('retries a declined capture with a new key after the configured delay, then fails for good', async () => {
  // CAPTURE_RETRY_DELAYS_H = "0.0003,0.0003" (about 1 s) for the test
  // expect: two extra capture calls on the mock PSP with keys ending :r1 and :r2, then state CAPTURE_FAILED and one billing.payment-failed
});
```

Fill the body with the mock PSP's call log the same way the existing tests in `psp-edge.test.ts` inspect it.
`CAPTURE_RETRY_DELAYS_H` must therefore accept fractional hours.

- [ ] **Step 5: Implement the saga change and migration; run the billing suite**

Run: `pnpm --filter @evcharge/billing test:int`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add services/billing
git commit -m "feat(billing): retry declined captures at 24 h and 72 h before failing"
```

### Task 8: Admin API to resume parked sagas (M6 part 2)

**Files:**
- Modify: `services/billing/src/api/billing.controller.ts`
- Modify: `services/billing/src/sagas/saga-engine.ts` (new public method)
- Test: `services/billing/test/int/saga.test.ts`

**Interfaces:**
- Produces: `SagaEngine.resume(sessionId: string): Promise<'resumed' | 'not_parked' | 'not_found'>`. Parked =
  `next_attempt_at IS NULL AND state IN ('PRICING','INVOICED','CAPTURING')`. Resume sets `next_attempt_at = now()`,
  `attempts = 0`, `last_error = NULL`. It does not clear `energy_flag`; a request body
  `{ "acceptEnergy": true }` sets `energy_flag = 'ok'` first (operator confirmed the reading).
- Produces: `POST /internal/sagas/:sessionId/resume` behind the existing internal-token guard; 200
  `{ "result": "resumed" }`, 409 for `not_parked`, 404 for `not_found`.
- Produces: `GET /internal/sagas/parked?limit=100` → `[{ sessionId, state, failureReason, updatedAt }]`.

- [ ] **Step 1: Write the failing test**

```ts
it('lists a parked saga and resumes it with acceptEnergy, which then invoices', async () => {
  // reuse the implausible-energy setup from Task 3
  const list = await internal('GET', '/internal/sagas/parked');
  expect(list.body.map((x: { sessionId: string }) => x.sessionId)).toContain(sessionIdOf(9102));
  expect((await internal('POST', `/internal/sagas/${sessionIdOf(9102)}/resume`, { acceptEnergy: true })).status).toBe(200);
  await waitFor(async () => (await h.db.pool.query('SELECT 1 FROM invoice_registry WHERE session_id = $1', [sessionIdOf(9102)])).rowCount);
  expect((await internal('POST', `/internal/sagas/${sessionIdOf(9102)}/resume`)).status).toBe(409);
});
```

Session 9102 is set up like 9101 in Task 3 (flag `implausible`), so it sits in PRICING with `next_attempt_at NULL`.

- [ ] **Step 2: Run to verify it fails** — `pnpm --filter @evcharge/billing test:int -- saga`, expected 404 on the GET.

- [ ] **Step 3: Implement `resume`, the two routes; run the billing suite** — expected PASS.

- [ ] **Step 4: Commit**

```bash
git add services/billing
git commit -m "feat(billing): internal API to list and resume parked sagas"
```

### Task 9: Gapless invoice number and VAT split (M2 part 1)

**Files:**
- Create: `services/billing/migrations/0007_invoice_number_vat.sql`
- Modify: `services/billing/src/domain.ts`
- Modify: `services/billing/src/sagas/saga-engine.ts` (`stepPrice`, the `INSERT INTO invoices` at line ~280)
- Modify: `services/billing/src/config.ts` (`VAT_RATE_BP`, default `1900`; `INVOICE_PREFIX`, default `EVC`)
- Modify: `services/billing/src/invoices/invoice-reader.ts` and `packages/contracts/src/api.ts` (invoice DTO:
  optional `number`, `netMinor`, `vatMinor`, `vatRateBp`)
- Modify: `packages/contracts/src/events.ts` (`billing.invoice-issued`: same optional fields)
- Test: `services/billing/test/unit/domain.test.ts`, `services/billing/test/int/saga.test.ts`

**Interfaces:**
- Produces: `export function splitVat(grossMinor: number, rateBp: number): { netMinor: number; vatMinor: number }`:
  tariff prices are gross (B2C, PAngV), `vatMinor = Math.round(grossMinor * rateBp / (10_000 + rateBp))`,
  `netMinor = grossMinor - vatMinor`.
- Produces: `export function formatInvoiceNumber(prefix: string, year: number, seq: number): string` →
  `EVC-2026-000042` (6-digit zero pad).
- Produces: table `invoice_counters (year integer PRIMARY KEY, last_seq bigint NOT NULL)`; columns on `invoices`:
  `invoice_number text NULL UNIQUE`... note `invoices` is partitioned by `issued_at`, so a global UNIQUE is not
  allowed: put `UNIQUE (invoice_number)` on `invoice_registry` instead (add column there too) and store the number
  on both. Columns `net_minor integer`, `vat_minor integer`, `vat_rate_bp integer` on `invoices`, all NULL for old rows.
- Gapless: inside the invoice transaction,
  `INSERT INTO invoice_counters (year, last_seq) VALUES ($1, 1) ON CONFLICT (year) DO UPDATE SET last_seq = invoice_counters.last_seq + 1 RETURNING last_seq`.
  The row lock serialises issuers; a rollback undoes the increment, so no gap. Year = `issuedAt` in `Europe/Berlin`.

- [ ] **Step 1: Write the failing unit tests**

```ts
it('splits a gross amount at 19% VAT', () => {
  expect(splitVat(1190, 1900)).toEqual({ netMinor: 1000, vatMinor: 190 });
  expect(splitVat(1, 1900)).toEqual({ netMinor: 1, vatMinor: 0 });
  expect(splitVat(0, 1900)).toEqual({ netMinor: 0, vatMinor: 0 });
});
it('formats invoice numbers', () => expect(formatInvoiceNumber('EVC', 2026, 42)).toBe('EVC-2026-000042'));
```

- [ ] **Step 2: Run to verify they fail; implement both; run to verify they pass**

Run: `pnpm --filter @evcharge/billing test:unit -- domain`

- [ ] **Step 3: Write the failing integration test (Review Focus 3)**

```ts
it('numbers invoices gaplessly under two concurrent engines', async () => {
  // complete 40 sessions with two SagaWorker instances running (the file already runs two replicas in its first describe)
  const { rows } = await h.db.pool.query<{ n: string }>("SELECT invoice_number AS n FROM invoice_registry WHERE invoice_number IS NOT NULL ORDER BY invoice_number");
  const seqs = rows.map((r) => Number(r.n.split('-')[2]));
  expect(new Set(seqs).size).toBe(seqs.length);
  expect(seqs.at(-1)! - seqs[0]! + 1).toBe(seqs.length);
});
```

- [ ] **Step 4: Run to verify it fails; implement migration and `stepPrice` change; run the billing suite**

Run: `pnpm --filter @evcharge/billing test:int`
Expected: PASS.

- [ ] **Step 5: Rebuild contracts schemas and commit**

```bash
pnpm --filter @evcharge/contracts build
git add services/billing packages/contracts
git commit -m "feat(billing): gapless yearly invoice numbers and VAT split"
```

### Task 10: Credit notes with a PSP refund (M2 part 2)

**Files:**
- Modify: `services/billing/src/psp/payment-provider.ts` (add `refund`)
- Modify: `services/billing/src/psp/mock-psp.ts`
- Create: `services/billing/migrations/0008_credit_notes.sql`
- Create: `services/billing/src/invoices/credit-notes.ts`
- Modify: `services/billing/src/api/billing.controller.ts`
- Test: `services/billing/test/int/credit-notes.test.ts`

**Interfaces:**
- Produces: `RefundRequest { idempotencyKey: string; pspReference: string; amountMinor: number; currency: string }`
  and `PaymentProvider.refund(req: RefundRequest): Promise<{ refundReference: string }>`; mock returns a stable
  reference per key and throws `PspIdempotencyMismatchError` on a reused key with a different amount.
- Produces: table `credit_notes (credit_note_id uuid PK, invoice_id uuid NOT NULL, credit_number text UNIQUE NOT NULL, amount_minor integer NOT NULL CHECK (amount_minor > 0), reason text NOT NULL, refund_reference text NULL, created_at timestamptz NOT NULL DEFAULT now())`,
  append-only with the existing `billing_append_only()` trigger for DELETE (UPDATE allowed only for
  `refund_reference`: write a separate trigger function). Numbers from `invoice_counters` with prefix
  `${INVOICE_PREFIX}-CN` (reuse `formatInvoiceNumber`; use a separate counter key, e.g. `year + 100000`, or add a
  `kind` column to `invoice_counters` — pick the `kind` column).
- Produces: `issueCreditNote(pool, psp, invoiceId: string, amountMinor: number, reason: string, idempotencyKey: string): Promise<{ creditNoteId: string; creditNumber: string }>`.
  Rejects when the sum of credit notes would exceed the invoice total (409).
- Produces: `POST /internal/invoices/:invoiceId/credit-notes` `{ amountMinor, reason }`, header `idempotency-key`
  required; 201 with the result.
- Emits `billing.credit-note-issued` (add to `BillingEvent` union in contracts, optional consumer-side).

- [ ] **Step 1: Write the failing tests**

```ts
it('issues a partial credit note with a refund and a CN number', async () => { /* 201, credit_number matches /^EVC-CN-\d{4}-\d{6}$/, mock PSP saw one refund */ });
it('refuses credit notes above the invoice total', async () => { /* second note pushing the sum over total → 409 */ });
it('is idempotent on the idempotency-key', async () => { /* same key twice → same creditNoteId, one refund */ });
```

Fill each body from the comment; amounts must be concrete integers taken from the invoice created in `beforeAll`.

- [ ] **Step 2: Run to verify they fail** — `pnpm --filter @evcharge/billing test:int -- credit-notes`.

- [ ] **Step 3: Implement; run the billing suite** — expected PASS.

- [ ] **Step 4: Rebuild contracts schemas and commit**

```bash
pnpm --filter @evcharge/contracts build
git add services/billing packages/contracts
git commit -m "feat(billing): credit notes with PSP refund"
```

---

## Phase D — Pricing by metered energy (M4) and signed meter data (M3 part 2)

### Task 11: Tariff apportions time-of-use energy by meter samples

**Files:**
- Modify: `services/tariff/internal/pricing/quote.go` (`Request`, `Quote`, around line 84)
- Create: `services/tariff/internal/pricing/profile.go`
- Modify: `packages/contracts-go/contracts.go` (`QuoteReq` line 99) and `contract_test.go`
- Modify: `packages/contracts/src/api.ts` (`QuoteReq`)
- Modify: `services/tariff/internal/...` HTTP handler that maps `QuoteReq` to `pricing.Request` (grep `pricing.Request{`)
- Test: `services/tariff/internal/pricing/profile_test.go`, `quote_test.go`

**Interfaces:**
- Produces: `type MeterSample struct { At time.Time; EnergyWh int64 }` (cumulative register) and
  `QuoteReq.EnergyProfile []MeterSample \`json:"energyProfile,omitempty"\`` (TS: `z.array(z.object({ at: ts, energyWh: z.number().int() })).max(2000).optional()`).
- Produces: `func EnergyPerBand(bands []Band, start time.Time, profile []MeterSample, total int64) ([]int64, bool)` in
  `profile.go`: linear interpolation of the cumulative register at each band boundary; returns `false` when fewer
  than 2 samples or samples not monotone in time; the result is rescaled with `LargestRemainder` so it sums to
  `total` exactly.
- `Quote` uses `EnergyPerBand` when it returns true, else today's time weights (Review Focus 4).

- [ ] **Step 1: Write the failing tests**

```go
func TestEnergyPerBandFollowsMeter(t *testing.T) {
	// 22:00–00:00 session, off-peak from 23:00; 2 kWh before 23:00, 8 kWh after
	start := ts(t, "2026-11-10T21:00:00Z") // 22:00 Europe/Berlin
	bands := []Band{{Window: -1, Ms: 3_600_000}, {Window: 0, Ms: 3_600_000}}
	prof := []MeterSample{{start, 0}, {start.Add(time.Hour), 2000}, {start.Add(2 * time.Hour), 10000}}
	got, ok := EnergyPerBand(bands, start, prof, 10000)
	if !ok || !reflect.DeepEqual(got, []int64{2000, 8000}) { t.Fatalf("got %v %v", got, ok) }
}
func TestEnergyPerBandFallsBackWithOneSample(t *testing.T) {
	_, ok := EnergyPerBand([]Band{{-1, 1}}, epoch, []MeterSample{{epoch, 0}}, 10)
	if ok { t.Fatal("expected fallback") }
}
```

Add a `Quote` test with `seedTou()` that checks the off-peak line quantity is 8 kWh for the profile above (adjust
the window to the seed's 23:00–07:00 in its own timezone).

- [ ] **Step 2: Run to verify they fail**

Run: `cd services/tariff && go test ./internal/pricing/ -run EnergyPerBand`
Expected: FAIL, undefined `EnergyPerBand`.

- [ ] **Step 3: Implement; run the package and the contracts tests**

Run: `cd services/tariff && go test ./... && cd ../../packages/contracts-go && go test ./...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
pnpm --filter @evcharge/contracts build
git add services/tariff packages/contracts packages/contracts-go
git commit -m "feat(tariff): apportion time-of-use energy by meter samples"
```

### Task 12: Billing sends the meter profile with the quote

**Files:**
- Create: `services/billing/src/tariff/energy-profile.ts`
- Modify: `services/billing/src/sagas/saga-engine.ts` (`stepPrice`, the `this.tariff.quote(...)` call)
- Modify: `services/billing/src/config.ts` (`TELEMETRY_URL`, optional; absent = no profile)
- Test: `services/billing/test/unit/energy-profile.test.ts`

**Interfaces:**
- Consumes: telemetry `GET /v1/stations/{id}/connectors/{cid}/power` (`services/telemetry-ingest/internal/api/api.go:61`);
  read its handler for the query parameters and response shape (1-minute buckets with `energy_wh_delta`) before
  writing the client. Internal token header as other internal calls use.
- Produces: `export function profileFromBuckets(startedAt: Date, buckets: { bucket: string; energyWhDelta: number }[]): { at: string; energyWh: number }[]`
  (cumulative sum, first sample `{ at: startedAt, energyWh: 0 }`), and
  `fetchEnergyProfile(url, token, stationId, connectorId, startedAt, stoppedAt): Promise<... | undefined>` that
  returns `undefined` on any error or timeout (2 s) — pricing must never wait on telemetry; the tariff then falls
  back to time weights.

- [ ] **Step 1: Write the failing unit test for `profileFromBuckets`** (3 buckets → 4 cumulative samples, values exact).
- [ ] **Step 2: Run it to verify it fails** — `pnpm --filter @evcharge/billing test:unit -- energy-profile`.
- [ ] **Step 3: Implement both functions and pass `energyProfile` in `stepPrice`; run unit and int suites** — PASS.
- [ ] **Step 4: Commit**

```bash
git add services/billing
git commit -m "feat(billing): price time-of-use sessions from telemetry meter profile"
```

### Task 13: Store and expose signed meter data (Eichrecht, M3 part 2)

**Files:**
- Create: `services/sessions/migrations/0006_signed_meter_data.sql`
  (`ALTER TABLE sessions ADD COLUMN signed_meter_data jsonb NULL;`)
- Modify: `services/station-gateway/src/` mapping of `StopTransaction` to the sessions request (grep `stopTransaction`
  / `meterStopWh` in the gateway) to forward `transactionData`
- Modify: `packages/contracts/src/api.ts` (`StopTxReq`: optional `signedMeterData: z.array(z.object({ format: z.literal('SignedData'), value: z.string().max(8192), context: z.string().optional() })).max(16)`)
- Modify: `services/sessions/src/ocpp/ocpp.service.ts` (`stopTransaction`, `complete()`)
- Modify: sessions session DTO (`SESSION_COLUMNS` / `sessionDto` in `driver.service.ts`) to return `signedMeterData`
- Test: `services/sessions/test/int/ocpp.test.ts`, gateway unit test for the mapping

**Interfaces:**
- Produces: the gateway extracts every `sampledValue` with `format === 'SignedData'` from
  `StopTransaction.transactionData` (`packages/contracts/src/ocpp.ts:137`) into `signedMeterData`; sessions stores
  it verbatim; `GET /v1/sessions/:id` returns it so a driver can verify it with transparency software.
  No signature verification.

- [ ] **Step 1: Write the failing gateway unit test** (StopTransaction with two sampledValues, one SignedData →
  request has exactly that one in `signedMeterData`).
- [ ] **Step 2: Write the failing sessions int test** (stopTx with `signedMeterData` → driver GET returns it).
  Extend `h.stopTx` with an optional `extra` object if needed.
- [ ] **Step 3: Run both to verify they fail.**
- [ ] **Step 4: Implement; run** `pnpm --filter @evcharge/station-gateway test:unit && pnpm --filter @evcharge/sessions test:int` — PASS.
- [ ] **Step 5: Commit**

```bash
pnpm --filter @evcharge/contracts build
git add services/station-gateway services/sessions packages/contracts
git commit -m "feat: store and expose signed meter data from StopTransaction"
```

---

## Phase E — Smart charging closed loop (M9, M10)

### Task 14: Site draw measured from station meter values (M9)

**Files:**
- Modify: `services/station-gateway/src/gateway.ts` (MeterValues handling; `c.credential.siteId` is available, see line 480)
- Modify: the gateway metrics file
- Modify: `infra/prometheus/rules/slo-recording.yml:117`, `infra/prometheus/rules/slo-alerts.yml:54`
- Modify: `infra/prometheus/tests/*.yml` (the test that covers `SiteCapExceeded`)
- Test: gateway unit test for the gauge

**Interfaces:**
- Produces: gauges `gateway_site_drawn_watts{site_id}` (sum over this instance's connected stations of the latest
  live `Power.Active.Import` per connector; readings marked `replayed` are ignored; a connector's value is dropped on
  disconnect) and `gateway_site_cap_watts{site_id}` (from the credential: add `capW` to `CredentialRes` in contracts
  and to `OcppService.credential()` if `chargingDefaults` does not already carry it).
- Recording rule becomes
  `max(sum by (site_id) (gateway_site_drawn_watts) / max by (site_id) (gateway_site_cap_watts))`; keep the simulator
  ratio as a second rule `slo:site_cap_utilisation_sim:max` for the drills.

- [ ] **Step 1: Write the failing gateway unit test** (two stations of one site report 4,000 W and 3,000 W → gauge
  7,000; one disconnects → 3,000; a replayed reading does not change it).
- [ ] **Step 2: Run to verify it fails** — `pnpm --filter @evcharge/station-gateway test:unit`.
- [ ] **Step 3: Implement the gauges; update the rules and the promtool test series to the new metric names.**
- [ ] **Step 4: Verify** — `pnpm --filter @evcharge/station-gateway test:unit && make prom-check` — PASS.
- [ ] **Step 5: Commit**

```bash
git add services/station-gateway services/sessions packages/contracts infra/prometheus
git commit -m "feat: measure site draw from station meter values and alert on it"
```

### Task 15: Escalate stations that keep rejecting profiles (M10)

**Files:**
- Create: `services/sessions/migrations/0007_profile_failures.sql`
  (`ALTER TABLE active_sessions ADD COLUMN profile_failures integer NOT NULL DEFAULT 0;`)
- Modify: `services/sessions/src/consumers/handlers.ts` (`profileResult`, line 147)
- Modify: `services/sessions/src/config.ts` (`PROFILE_FAILURES_TO_STOP`, default `0` = never stop; `PROFILE_FAILURES_TO_ALERT`, default `3`)
- Modify: `services/sessions/src/metrics.ts`, `infra/prometheus/rules/slo-alerts.yml`, `infra/prometheus/tests/`
- Test: `services/sessions/test/int/two-phase.test.ts`

**Interfaces:**
- Behaviour: Accepted resets `profile_failures` to 0. `Rejected` (and only `Rejected`: `StationOffline`/`Timeout`
  are backend-side or connectivity and must not count, ADR 0000 degradation) increments it. Reaching
  `PROFILE_FAILURES_TO_ALERT` increments `sessions_profile_noncompliant_total{site_id}` once; reaching
  `PROFILE_FAILURES_TO_STOP` (> 0) sends RemoteStop the same way as Task 6.
- Alert `StationIgnoresChargingProfile`: `increase(sessions_profile_noncompliant_total[10m]) > 0`, severity warning.

- [ ] **Step 1: Write the failing test** in `two-phase.test.ts`: three `Rejected` results for one session's
  SetChargingProfile commands (use `h.ack.skip` + `h.publishCommandResult`) → counter metric +1 (read it from
  `/metrics` via `h.http`); with `PROFILE_FAILURES_TO_STOP=3` in `env` → a RemoteStop command for that transaction;
  three `Timeout` results → no RemoteStop.
- [ ] **Step 2: Run to verify it fails** — `pnpm --filter @evcharge/sessions test:int -- two-phase`.
- [ ] **Step 3: Implement; add the alert and its promtool test.**
- [ ] **Step 4: Verify** — `pnpm --filter @evcharge/sessions test:int && make prom-check` — PASS.
- [ ] **Step 5: Commit**

```bash
git add services/sessions infra/prometheus
git commit -m "feat(sessions): escalate stations that keep rejecting charging profiles"
```

---

## Phase F — Infrastructure (M1, M11, M12, M13)

### Task 16: TLS for the station edge (M1)

**Files:**
- Create: `infra/haproxy/gen-certs.sh` (openssl: lab CA + server cert for `station-lb` and `localhost`, output to
  `infra/haproxy/certs/`, idempotent: skip when files exist)
- Modify: `.gitignore` (`infra/haproxy/certs/`)
- Modify: `infra/haproxy/haproxy.cfg` (new `bind :8443 ssl crt /usr/local/etc/haproxy/certs/server.pem alpn http/1.1`
  in `frontend ocpp`; keep `:8080` with a comment "plain ws: lab load tests only")
- Modify: `docker-compose.yml` (mount certs into `station-lb`, publish `28443`, simulator `CSMS_URL: wss://station-lb:8443/ocpp`
  and `NODE_EXTRA_CA_CERTS` pointing at the mounted CA)
- Modify: `Makefile` (`infra-up` and a new `certs` target call `infra/haproxy/gen-certs.sh` first)
- Modify: `docs/runbook.md` (one paragraph: certs, ports, production uses OCPP security profile 2/3)

**Interfaces:** none in code; the simulator already accepts `wss://` (`simulator/src/config.ts:10`).

- [ ] **Step 1: Write `gen-certs.sh`; run it twice**

Run: `bash infra/haproxy/gen-certs.sh && bash infra/haproxy/gen-certs.sh && ls infra/haproxy/certs`
Expected: `ca.pem server.pem` (plus keys), second run prints that certs exist.

- [ ] **Step 2: Config check**

Run: `docker run --rm -v "$PWD/infra/haproxy:/usr/local/etc/haproxy:ro" haproxy:<tag used in compose> haproxy -c -f /usr/local/etc/haproxy/haproxy.cfg`
Expected: `Configuration file is valid`.

- [ ] **Step 3: End-to-end**

Run: `docker compose up -d --build station-lb simulator && sleep 20 && curl -s 127.0.0.1:28089/status`
Expected: connected stations equal the fleet size (200).
Run: `openssl s_client -connect localhost:28443 -CAfile infra/haproxy/certs/ca.pem </dev/null | grep 'Verify return code'`
Expected: `Verify return code: 0 (ok)`.

- [ ] **Step 4: Commit**

```bash
git add infra/haproxy docker-compose.yml Makefile .gitignore docs/runbook.md
git commit -m "feat(infra): TLS on the station edge, simulator connects over wss"
```

### Task 17: Alertmanager and a service-down alert (M12)

**Files:**
- Create: `infra/alertmanager/alertmanager.yml` (one route, receiver `lab-log` with a `webhook_configs` URL to a
  tiny sink, or no receiver config; add a commented Slack/PagerDuty example)
- Modify: `docker-compose.yml` (service `alertmanager`, image `prom/alertmanager` pinned to a release at least two
  weeks old, port `127.0.0.1:29093:9093`)
- Modify: `infra/prometheus/prometheus.yml` (`alerting: alertmanagers: [{ static_configs: [{ targets: [alertmanager:9093] }] }]`)
- Modify: `infra/prometheus/rules/slo-alerts.yml` (`ServiceDown`: `up == 0` for 1m, critical) and a promtool test
- Modify: `Makefile` `prom-check` (add `amtool check-config` in a one-off container)
- Modify: `docs/runbook.md` (where alerts go, how to add a receiver)

- [ ] **Step 1: Write the promtool test for `ServiceDown` first; run** `make prom-check` — expected FAIL (rule missing).
- [ ] **Step 2: Add the rule, Alertmanager config and compose service; run** `make prom-check` — PASS, including amtool.
- [ ] **Step 3: End-to-end**

Run: `docker compose up -d prometheus alertmanager && docker compose stop tariff && sleep 90 && curl -s 127.0.0.1:29093/api/v2/alerts | grep -c ServiceDown`
Expected: `1` or more. Then `docker compose start tariff`.

- [ ] **Step 4: Commit**

```bash
git add infra docker-compose.yml Makefile docs/runbook.md
git commit -m "feat(infra): Alertmanager and a service-down alert"
```

### Task 18: Late telemetry reaches the 1-minute rollup (M13)

**Files:**
- Create: `services/telemetry-ingest/internal/ingest/late.go`
- Modify: `services/telemetry-ingest/internal/ingest/pipeline.go` (after a successful batch commit, report the min
  and max reading time of rows older than 2 h)
- Modify: the app wiring that starts the pipeline (`services/telemetry-ingest/internal/app/app.go`)
- Test: `services/telemetry-ingest/internal/ingest/late_test.go` (unit) and a case in `pipeline_integration_test.go`

**Interfaces:**
- Produces: `type LateRefresher struct` with `func NewLateRefresher(db *pgxpool.Pool, every time.Duration, logger *slog.Logger) *LateRefresher`,
  `func (r *LateRefresher) Note(minTs, maxTs time.Time)` (merges into one pending range, goroutine-safe) and
  `func (r *LateRefresher) Run(ctx context.Context)` which every `every` (60 s) calls
  `CALL refresh_continuous_aggregate('power_1m', $1, $2)` with the range widened to whole minutes
  (`minTs.Truncate(time.Minute)`, `maxTs.Truncate(time.Minute).Add(time.Minute)`) and clears it; on error it keeps
  the range for the next tick and logs.
- Threshold "late" = older than the policy's `start_offset` (2 h, `migrations/0002_power_1m.sql:15`); define
  `const LateAfter = 2 * time.Hour` next to the refresher with a comment pointing at that migration.

- [ ] **Step 1: Write the failing unit test** for `Note` merging (two notes → one range spanning both) using an
  unexported `pending()` accessor.
- [ ] **Step 2: Write the failing integration test**: ingest a reading timestamped 5 h ago, call one refresher tick,
  `SELECT samples FROM power_1m WHERE bucket = $1 AND station_id = $2` returns 1. Follow `newHarness(t, …)` in
  `pipeline_integration_test.go`.
- [ ] **Step 3: Run to verify they fail** — `cd services/telemetry-ingest && go test ./internal/ingest/ -run Late`
  (integration needs `TEST_PG_TELEMETRY_URL`, set by the Makefile exports).
- [ ] **Step 4: Implement; run** `cd services/telemetry-ingest && go test -race ./...` — PASS.
- [ ] **Step 5: Commit**

```bash
git add services/telemetry-ingest
git commit -m "fix(telemetry): refresh the 1-minute rollup for late replayed readings"
```

### Task 19: Decide Postgres HA and fencing (M11, decision only)

**Files:**
- Create: `docs/adr/0022-postgres-ha-manager.md`
- Modify: `docs/adr/README.md` (index row), `docs/adr/0018-postgres-failover-and-backup-restore.md` (one
  "Superseded in part by ADR 0022" line), `docs/known-limits.md` (Durability bullet links ADR 0022)

**Interfaces:** none.

- [ ] **Step 1: Write ADR 0022** in the format of the existing ADRs (Status: proposed). Decision: Patroni with a
  3-node etcd as DCS, synchronous standby (`synchronous_mode: true`) for RPO 0 (ADR 0000 Durability amendment),
  fencing through the Patroni leader key plus watchdog, HAProxy routing on Patroni's REST `/primary`. Rejected:
  scripted `pg_promote` (no fencing, `drills/postgres-failover.sh:113`), repmgr (fencing left to scripts), managed
  service (out of scope for a local lab). Consequences: the failover drill must be rewritten to kill the leader and
  check that the restarted old primary rejoins as a replica; that implementation is a separate plan.
- [ ] **Step 2: Verify links** — `grep -n 0022 docs/adr/README.md docs/known-limits.md docs/adr/0018-*.md` shows the three references.
- [ ] **Step 3: Commit**

```bash
git add docs/adr docs/known-limits.md
git commit -m "docs: ADR 0022 proposes Patroni with fencing for Postgres HA"
```

---

## Phase G — GDPR (M14)

### Task 20: Encrypt payment tokens at rest

**Files:**
- Create: `services/billing/src/token-crypto.ts`
- Modify: `services/billing/src/config.ts` (`TOKEN_ENC_KEY`: 32 bytes base64, required when `NODE_ENV=production`;
  a fixed lab default otherwise, like the other compose secrets)
- Modify: every read/write of `auth_token`, `capture_token` and the third token column (`grep -n "_token" services/billing/src`)
- Create: `services/billing/migrations/0009_token_enc.sql` only if a column type change is needed (keep `text`, store
  `v1:<base64 iv>:<base64 ciphertext+tag>`)
- Test: `services/billing/test/unit/token-crypto.test.ts`

**Interfaces:**
- Produces: `encryptToken(plain: string, key: Buffer): string` and `decryptToken(stored: string, key: Buffer): string`
  with AES-256-GCM (`node:crypto`), random 12-byte IV; `decryptToken` returns the input unchanged when it does not
  start with `v1:` (rows written before this change).

- [ ] **Step 1: Write the failing unit tests**: round trip; two encryptions of one token differ; a tampered
  ciphertext throws; a legacy plaintext value passes through.
- [ ] **Step 2: Run to verify they fail** — `pnpm --filter @evcharge/billing test:unit -- token-crypto`.
- [ ] **Step 3: Implement and wire into the saga reads/writes; add an int assertion that the stored
  `billing_sagas.auth_token` starts with `v1:`.**
- [ ] **Step 4: Run** `pnpm --filter @evcharge/billing test:unit && pnpm --filter @evcharge/billing test:int` — PASS.
- [ ] **Step 5: Commit**

```bash
git add services/billing
git commit -m "feat(billing): encrypt payment tokens at rest"
```

### Task 21: Driver account deletion

**Files:**
- Modify: `services/driver-api/src/driver.controller.ts` (`@Delete('me')`), `services/driver-api/src/auth.service.ts`
- Modify: `services/sessions/src/http/internal.controller.ts` (`DELETE /internal/drivers/:driverId`)
- Modify: `services/billing/src/api/billing.controller.ts` (`DELETE /internal/drivers/:driverId/payment-data`)
- Test: `services/driver-api/test/int/`, `services/sessions/test/int/ocpp.test.ts`, `services/billing/test/int/saga.test.ts`

**Interfaces:**
- `DELETE /v1/me` (driver JWT, `idempotency-key` required): 409 `active_session` if the driver has an Active or
  Stopping session; otherwise calls sessions then billing, then deletes credentials and refresh tokens in driver-api;
  204.
- sessions: sets `drivers.blocked = true`, replaces `id_tag` with `deleted-<uuid>`, keeps `driver_id` (sessions and
  invoices must stay for the retention period, ADR 0000 Data volume); 204, idempotent.
- billing: nulls `auth_token`, `capture_token` and the third token column for that driver's sagas in final states
  (`PAID`, `CAPTURE_FAILED`, `AUTH_FAILED` without pending work); 204, idempotent.
- Invoices are not deleted (legal retention); document this in `services/driver-api/README.md`.

- [ ] **Step 1: Write the failing tests** — driver-api: register, delete → 204, login → 401, second delete → 204;
  with an active session → 409. sessions: after delete, Authorize with the old idTag → `Invalid`. billing: token
  columns null for a PAID saga of that driver.
- [ ] **Step 2: Run each to verify they fail.**
- [ ] **Step 3: Implement the three endpoints.**
- [ ] **Step 4: Run** `pnpm test:int` for the three services — PASS.
- [ ] **Step 5: Commit**

```bash
git add services/driver-api services/sessions services/billing
git commit -m "feat: driver account deletion keeping legally retained records"
```

---

## Phase H — Close out

### Task 22: Docs, findings and full check

**Files:**
- Modify: `docs/known-limits.md` (remove or rewrite the gaps closed by Tasks 2–21; keep the scoped-out parts from
  the brief's "Scope decisions" as explicit limits)
- Modify: `docs/adr/0007-billing-saga-and-exactly-once-effect.md` (capture retries, parked-saga API, credit notes,
  VAT, numbering), `docs/adr/0006-smart-charging-allocation.md` (measured site draw, profile escalation),
  `docs/adr/0008-telemetry-storage.md` (late refresh), `docs/adr/0003-...` (TLS, signed meter data)
- Modify: `README.md` only through `node scripts/charts/build.mjs` (ADR count becomes 23)

- [ ] **Step 1: Update the docs listed above, one paragraph each, naming the task's migration and config keys.**
- [ ] **Step 2: Regenerate** — `node scripts/charts/build.mjs`; expected README header shows `23 ADRs`.
- [ ] **Step 3: Full check** — `make check`; expected last line `✔ make check passed`.
- [ ] **Step 4: Commit**

```bash
git add docs README.md
git commit -m "docs: record the production-readiness changes"
```
