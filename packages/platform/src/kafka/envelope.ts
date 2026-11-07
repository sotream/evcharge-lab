import { randomUUID } from 'node:crypto';

/** Shape of every Kafka record value (mirrors `envelope()` in @evcharge/contracts events). */
export interface Envelope<Data = unknown, Type extends string = string> {
  eventId: string;
  type: Type;
  version: 1;
  occurredAt: string;
  producer: string;
  key: string;
  data: Data;
}

/** Builds an envelope with a fresh uuid v4 eventId and occurredAt = now (ISO-8601, UTC). */
export function makeEnvelope<Type extends string, Data>(
  type: Type,
  key: string,
  data: Data,
  producer: string,
  opts: { eventId?: string; occurredAt?: Date } = {},
): Envelope<Data, Type> {
  return {
    eventId: opts.eventId ?? randomUUID(),
    type,
    version: 1,
    occurredAt: (opts.occurredAt ?? new Date()).toISOString(),
    producer,
    key,
    data,
  };
}
