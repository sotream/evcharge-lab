-- Fresh primary only (docker-entrypoint-initdb.d). The replica entrypoint runs the same statements idempotently,
-- so a primary whose data directory predates replication also works.
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';
SELECT pg_create_physical_replication_slot('replica_1');
