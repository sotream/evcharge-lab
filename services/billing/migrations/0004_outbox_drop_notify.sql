-- Drop the outbox NOTIFY trigger: NOTIFY serialises every commit on one global lock (see OUTBOX_DROP_NOTIFY_DDL in @evcharge/platform). The relay polls every 50 ms instead.
DROP TRIGGER IF EXISTS outbox_notify_trg ON outbox;
DROP FUNCTION IF EXISTS outbox_notify();
