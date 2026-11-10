/**
 * billing.events.v1 -> sessions: a payment authorization that was DECLINED stops the charging session (RemoteStop).
 * ADR 0000 "Degradation": charging never stops because a dependency is down, so only a reason on the ALLOWLIST of definite
 * card declines stops it. Anything else (psp_unavailable, voided, no_payment_method of driverless ad-hoc charging, any
 * code a real PSP adds later, a transient error) keeps charging: wrongly stopping a session is worse than late billing.
 * The mock PSP's authorize decline is 'card_declined'; the others are the usual hard-decline codes of real PSPs.
 */
import type { events } from '@evcharge/contracts';
import { type Logger, type Pool, withInbox, withTx } from '@evcharge/platform';
import { m } from '../metrics';
import type { TopicNames } from '../outbox-events';
import { requestRemoteStop } from '../session-stop';

export const BILLING_EVENTS_CONSUMER = 'sessions.billing-events';
export const DECLINE_REASONS = new Set(['card_declined', 'insufficient_funds', 'expired_card', 'stolen_card', 'lost_card', 'do_not_honor', 'card_not_supported']);

export class BillingEventsHandler {
  constructor(
    private readonly pool: Pool,
    private readonly topics: TopicNames,
    private readonly enabled: boolean,
    private readonly logger: Logger,
  ) {}

  async handle(e: events.BillingEvent): Promise<void> {
    if (e.type !== 'billing.payment-failed' || e.data.step !== 'authorize') return;
    const { sessionId, reason } = e.data;
    if (!this.enabled || !DECLINE_REASONS.has(reason)) return;
    const stopped = await withTx(this.pool, (c) =>
      withInbox(c, BILLING_EVENTS_CONSUMER, e.eventId, async () => {
        return requestRemoteStop(c, this.topics, sessionId, 'auth_declined');
      }),
    );
    if (stopped) {
      m.stoppedOnAuthDecline.inc();
      this.logger.warn({ sessionId, reason }, 'authorization declined: stopping the session');
    }
  }
}
