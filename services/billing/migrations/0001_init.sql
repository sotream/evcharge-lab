-- billing schema (ADR 0007). OLTP Postgres, synchronous_commit=on.

-- ---------------------------------------------------------------- platform: outbox + inbox
CREATE TABLE IF NOT EXISTS outbox (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  key text NOT NULL,
  payload jsonb NOT NULL,
  headers jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz NULL
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS outbox_published_at_idx ON outbox (published_at) WHERE published_at IS NOT NULL;
CREATE OR REPLACE FUNCTION outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('outbox', '');
  RETURN NULL;
END $$;
CREATE OR REPLACE TRIGGER outbox_notify_trg AFTER INSERT ON outbox FOR EACH STATEMENT EXECUTE FUNCTION outbox_notify();

CREATE TABLE IF NOT EXISTS processed_events (
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX IF NOT EXISTS processed_events_processed_at_idx ON processed_events (processed_at);

-- ---------------------------------------------------------------- payment methods
CREATE TABLE payment_methods (
  driver_id  uuid PRIMARY KEY,
  token      text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- saga
-- One row per session. `state` is the ADR 0007 state machine. A worker claims due rows with
-- FOR UPDATE SKIP LOCKED and leases them (lease_until) while it talks to tariff / the PSP, so no
-- transaction is held open across a network call.
CREATE TABLE billing_sagas (
  session_id       uuid PRIMARY KEY,
  state            text NOT NULL CHECK (state IN ('AUTHORIZING','AUTHORIZED','AUTH_FAILED','PRICING','INVOICED','CAPTURING','PAID','CAPTURE_FAILED')),
  driver_id        uuid,
  tariff_id        text NOT NULL,
  site_id          text NOT NULL,
  station_id       text NOT NULL,
  connector_id     integer NOT NULL,
  transaction_id   bigint NOT NULL,
  started_at       timestamptz NOT NULL,
  -- session.completed data (NULL until it was received)
  completed_at     timestamptz,
  stopped_at       timestamptz,
  energy_wh        bigint,
  -- authorization hold
  hold_minor       integer NOT NULL DEFAULT 0,
  currency         text NOT NULL DEFAULT 'EUR',
  authorization_id text,
  hold_released    boolean NOT NULL DEFAULT false,
  -- result
  invoice_id       uuid,
  total_minor      integer,
  -- scheduling
  attempts         integer NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz,
  lease_until      timestamptz,
  deadline_at      timestamptz,
  last_error       text,
  failure_reason   text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);
-- Due work only: terminal states and parked sagas (next_attempt_at NULL) are not indexed.
CREATE INDEX billing_sagas_due_idx ON billing_sagas (next_attempt_at) WHERE next_attempt_at IS NOT NULL;
CREATE INDEX billing_sagas_pending_idx ON billing_sagas (state) WHERE state IN ('AUTHORIZING','AUTHORIZED','PRICING','INVOICED','CAPTURING');
CREATE INDEX billing_sagas_driver_idx ON billing_sagas (driver_id) WHERE driver_id IS NOT NULL;

-- ---------------------------------------------------------------- invoices (append-only, monthly partitions)
-- A unique constraint on a partitioned table must contain the partition key, so the
-- "one invoice per session" rule lives in a small non-partitioned registry written in the same
-- transaction as the invoice.
CREATE TABLE invoice_registry (
  session_id uuid PRIMARY KEY,
  invoice_id uuid NOT NULL UNIQUE,
  driver_id  uuid,
  issued_at  timestamptz NOT NULL
);
CREATE INDEX invoice_registry_driver_idx ON invoice_registry (driver_id, issued_at DESC, invoice_id DESC) WHERE driver_id IS NOT NULL;

CREATE TABLE invoices (
  invoice_id     uuid NOT NULL,
  session_id     uuid NOT NULL,
  driver_id      uuid,
  tariff_id      text NOT NULL,
  currency       text NOT NULL,
  total_minor    integer NOT NULL,
  energy_wh      bigint NOT NULL,
  started_at     timestamptz NOT NULL,
  stopped_at     timestamptz NOT NULL,
  station_id     text NOT NULL,
  site_id        text NOT NULL,
  issued_at      timestamptz NOT NULL,
  PRIMARY KEY (invoice_id, issued_at)
) PARTITION BY RANGE (issued_at);

CREATE TABLE invoice_lines (
  invoice_id   uuid NOT NULL,
  issued_at    timestamptz NOT NULL,
  line_no      integer NOT NULL,
  kind         text NOT NULL,
  description  text NOT NULL,
  quantity     double precision NOT NULL,
  unit         text NOT NULL,
  amount_minor integer NOT NULL,
  PRIMARY KEY (invoice_id, issued_at, line_no)
) PARTITION BY RANGE (issued_at);

CREATE TABLE invoices_default PARTITION OF invoices DEFAULT;
CREATE TABLE invoice_lines_default PARTITION OF invoice_lines DEFAULT;

CREATE OR REPLACE FUNCTION billing_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is append-only: % is not allowed (issue a credit note instead)', TG_TABLE_NAME, TG_OP USING ERRCODE = 'restrict_violation';
END $$;
CREATE TRIGGER invoices_append_only BEFORE UPDATE OR DELETE ON invoices FOR EACH ROW EXECUTE FUNCTION billing_append_only();
CREATE TRIGGER invoice_lines_append_only BEFORE UPDATE OR DELETE ON invoice_lines FOR EACH ROW EXECUTE FUNCTION billing_append_only();

-- Creates monthly partitions for [from_month, from_month + months). Idempotent.
CREATE OR REPLACE FUNCTION billing_ensure_invoice_partitions(from_month date, months integer) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  m date := date_trunc('month', from_month)::date;
  created integer := 0;
  suffix text;
BEGIN
  FOR i IN 0 .. months - 1 LOOP
    suffix := to_char(m, 'YYYY_MM');
    IF to_regclass('invoices_' || suffix) IS NULL THEN
      EXECUTE format('CREATE TABLE %I PARTITION OF invoices FOR VALUES FROM (%L) TO (%L)', 'invoices_' || suffix, m::timestamptz, (m + interval '1 month')::timestamptz);
      EXECUTE format('CREATE TABLE %I PARTITION OF invoice_lines FOR VALUES FROM (%L) TO (%L)', 'invoice_lines_' || suffix, m::timestamptz, (m + interval '1 month')::timestamptz);
      created := created + 1;
    END IF;
    m := (m + interval '1 month')::date;
  END LOOP;
  RETURN created;
END $$;

-- ---------------------------------------------------------------- payments (one capture per invoice)
CREATE TABLE payments (
  invoice_id    uuid PRIMARY KEY,
  session_id    uuid NOT NULL,
  amount_minor  integer NOT NULL,
  currency      text NOT NULL,
  psp_reference text NOT NULL,
  captured_at   timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- mock PSP (own idempotency table)
CREATE TABLE psp_idempotency (
  idempotency_key text PRIMARY KEY,
  operation       text NOT NULL,
  request_hash    text NOT NULL,
  outcome         text NOT NULL CHECK (outcome IN ('ok','declined')),
  response        jsonb NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE psp_authorizations (
  authorization_id text PRIMARY KEY,
  token            text NOT NULL,
  amount_minor     integer NOT NULL,
  currency         text NOT NULL,
  captured_minor   integer NOT NULL DEFAULT 0,
  released         boolean NOT NULL DEFAULT false,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE psp_captures (
  psp_reference    text PRIMARY KEY,
  authorization_id text,
  token            text NOT NULL,
  amount_minor     integer NOT NULL,
  currency         text NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now()
);
