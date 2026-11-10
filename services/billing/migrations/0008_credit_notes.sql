-- Credit notes with PSP refunds (review M2, plan Task 10).
--
-- A credit note is a document like an invoice: append-only, gapless numbered (invoice_counters kind 'credit_note',
-- EVC-CN-2026-000001), with its own VAT split at the invoice's rate. The money movement is separate: one
-- credit_note_refunds row per capture it is refunded against, whose state changes as the PSP answers.
CREATE TABLE credit_notes (
  credit_note_id  uuid    PRIMARY KEY,
  invoice_id      uuid    NOT NULL,
  session_id      uuid    NOT NULL,
  credit_number   text    NOT NULL UNIQUE,
  idempotency_key text    NOT NULL,
  amount_minor    integer NOT NULL CHECK (amount_minor > 0),
  net_minor       integer NOT NULL,
  vat_minor       integer NOT NULL,
  vat_rate_bp     integer NOT NULL,
  currency        text    NOT NULL,
  reason          text    NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
  issued_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (invoice_id, idempotency_key),
  CHECK (net_minor + vat_minor = amount_minor)
);
CREATE INDEX credit_notes_invoice_idx ON credit_notes (invoice_id);
CREATE TRIGGER credit_notes_append_only BEFORE UPDATE OR DELETE ON credit_notes FOR EACH ROW EXECUTE FUNCTION billing_append_only();

CREATE TABLE credit_note_refunds (
  credit_note_id   uuid    NOT NULL REFERENCES credit_notes (credit_note_id),
  psp_reference    text    NOT NULL,
  amount_minor     integer NOT NULL CHECK (amount_minor > 0),
  state            text    NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'done', 'failed')),
  refund_reference text    NULL,
  failure_reason   text    NULL,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (credit_note_id, psp_reference)
);
CREATE INDEX credit_note_refunds_capture_idx ON credit_note_refunds (psp_reference);

-- mock PSP
CREATE TABLE psp_refunds (
  refund_reference text    PRIMARY KEY,
  psp_reference    text    NOT NULL,
  amount_minor     integer NOT NULL,
  currency         text    NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now()
);
