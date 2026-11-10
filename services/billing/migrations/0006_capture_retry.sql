-- Dunning for declined captures (review M6, plan Task 7). A definite decline schedules retry rounds at the delays of
-- CAPTURE_RETRY_DELAYS_H after the first decline; each round uses a new idempotency key (a decline is final per key).
ALTER TABLE billing_sagas ADD COLUMN capture_round integer NOT NULL DEFAULT 0 CHECK (capture_round >= 0);
ALTER TABLE billing_sagas ADD COLUMN capture_first_declined_at timestamptz NULL;
-- The round whose payment method capture_token holds: a new round re-reads the card once (the driver may have replaced
-- it while the saga waited), and keeps it for every timeout retry of that round, so a key never sees two tokens.
ALTER TABLE billing_sagas ADD COLUMN capture_token_round integer NOT NULL DEFAULT 0;
