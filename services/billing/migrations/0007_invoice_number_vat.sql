-- Gapless yearly document numbers and the VAT split on invoices (review M2, plan Task 9).
--
-- A counter row per (kind, year) is incremented inside the invoice transaction. The row lock serialises concurrent
-- issuers and a rollback undoes the increment, so numbers have no gaps (a sequence would leave gaps on rollback).
CREATE TABLE invoice_counters (
  kind     text    NOT NULL CHECK (kind IN ('invoice', 'credit_note')),
  year     integer NOT NULL,
  last_seq bigint  NOT NULL CHECK (last_seq > 0),
  PRIMARY KEY (kind, year)
);

-- invoices is partitioned by issued_at, so a global UNIQUE on the number lives on the registry.
-- NULL for invoices issued before this migration.
ALTER TABLE invoice_registry ADD COLUMN invoice_number text NULL;
ALTER TABLE invoice_registry ADD CONSTRAINT invoice_registry_number_unique UNIQUE (invoice_number);

ALTER TABLE invoices ADD COLUMN invoice_number text NULL;
ALTER TABLE invoices ADD COLUMN net_minor integer NULL;
ALTER TABLE invoices ADD COLUMN vat_minor integer NULL;
ALTER TABLE invoices ADD COLUMN vat_rate_bp integer NULL;
ALTER TABLE invoices ADD CONSTRAINT invoices_vat_sums CHECK (net_minor IS NULL OR net_minor + vat_minor = total_minor);
