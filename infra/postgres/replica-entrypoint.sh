#!/bin/sh
# Hot standby of the OLTP primary (ADR 0015). First start: create the replicator role and a physical slot on the
# primary if missing, copy the primary with pg_basebackup -R (writes standby.signal and primary_conninfo), then run
# postgres. Later starts reuse the data directory and resume streaming.
set -eu
PGDATA="${PGDATA:-/var/lib/postgresql/18/docker}"
PRIMARY="${PRIMARY_HOST:-postgres}"
SLOT="${REPLICA_SLOT:-replica_1}"
export PGDATA

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until PGPASSWORD=evcharge pg_isready -h "$PRIMARY" -U evcharge -d evcharge >/dev/null 2>&1; do
    echo "waiting for primary $PRIMARY"
    sleep 1
  done
  PGPASSWORD=evcharge psql -h "$PRIMARY" -U evcharge -d evcharge -v ON_ERROR_STOP=1 <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN
    CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';
  END IF;
END \$\$;
SELECT pg_create_physical_replication_slot('$SLOT') WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT');
SQL
  mkdir -p "$PGDATA"
  chmod 700 "$PGDATA"
  pg_basebackup -d "host=$PRIMARY port=5432 user=replicator password=replicator application_name=$SLOT" -D "$PGDATA" -R -X stream -S "$SLOT" -c fast
fi

# max_connections must be at least the primary's value (400) for a hot standby.
exec postgres \
  -c hba_file=/etc/postgresql/pg_hba.conf \
  -c hot_standby=on \
  -c hot_standby_feedback=on \
  -c max_connections=400 \
  -c shared_buffers=512MB \
  -c max_wal_senders=10 \
  -c max_replication_slots=10
