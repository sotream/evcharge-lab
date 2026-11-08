-- Commit-ordered outbox publishing (SQL of OUTBOX_TX_ORDER_DDL from @evcharge/platform).
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS tx xid8 NOT NULL DEFAULT pg_current_xact_id();
DROP INDEX IF EXISTS outbox_unpublished_idx;
CREATE INDEX IF NOT EXISTS outbox_unpublished_tx_idx ON outbox (tx, id) WHERE published_at IS NULL;
