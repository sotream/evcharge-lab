-- Persisted PSP request state, so retries replay the exact same requests (ADR 0007).

-- Authorization gave up after repeated unknown outcomes: the hold may exist at the PSP, so it is
-- voided (by the authorization idempotency key) until the PSP answers definitively.
ALTER TABLE billing_sagas ADD COLUMN auth_void_pending boolean NOT NULL DEFAULT false;
-- Payment token of the first authorization attempt, replayed on retries for the same reason as capture_token.
ALTER TABLE billing_sagas ADD COLUMN auth_token text;

-- The capture request is fixed on the first attempt. A later change of the driver's card must not
-- turn a retry of the same idempotency key into a different request.
ALTER TABLE billing_sagas ADD COLUMN capture_token text;
ALTER TABLE billing_sagas ADD COLUMN capture_hold_minor integer;
-- Amount above the hold, captured by token with key capture:{invoiceId}:rest after the hold capture.
ALTER TABLE billing_sagas ADD COLUMN capture_rest_minor integer;
ALTER TABLE billing_sagas ADD COLUMN hold_captured boolean NOT NULL DEFAULT false;
ALTER TABLE billing_sagas ADD COLUMN rest_captured boolean NOT NULL DEFAULT false;
ALTER TABLE billing_sagas ADD COLUMN capture_reference text;

-- One payment row per capture: the hold capture and, when the total exceeded the hold, the remainder.
ALTER TABLE payments ADD COLUMN kind text NOT NULL DEFAULT 'hold' CHECK (kind IN ('hold', 'remainder'));
ALTER TABLE payments DROP CONSTRAINT payments_pkey;
ALTER TABLE payments ADD PRIMARY KEY (invoice_id, kind);
