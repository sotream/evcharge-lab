#!/usr/bin/env bash
# Drill: physical backup of the OLTP Postgres (all databases, so `sessions` and `billing`) with pg_basebackup, restored into a
# SEPARATE container, with integrity checks and a measured data age (ADR 0018).
#
# Flow: counts -> pg_basebackup (-X stream: the WAL needed for a consistent copy is inside the backup) -> counts -> the load keeps
# running for BACKUP_DELAY_S (these are the writes a backup-only recovery loses) -> "disaster" counts -> copy the backup into a
# fresh volume (restore copy) -> start Postgres on it (crash recovery to the end of the backup) -> checks -> cleanup.
# The live stack is NOT touched: nothing is stopped and nothing is overwritten.
#
# Env: BACKUP_DELAY_S (60), RESTORE_PORT (25435, host port of the restored instance), KEEP (0: remove the restored container and volumes).
set -euo pipefail
DRILL="backup-restore" source "$(dirname "$0")/lib/common.sh"
require_fleet
BACKUP_DELAY_S="${BACKUP_DELAY_S:-60}"
RESTORE_PORT="${RESTORE_PORT:-25435}"
KEEP="${KEEP:-0}"
PRIM=$(docker compose ps -q postgres)
NET=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$PRIM")
IMG=postgres:18.6-alpine
BK_VOL=evcharge_drill_backup
RS_VOL=evcharge_drill_restore
RESTORED=evcharge-restored
now_ms() { node -e 'console.log(Date.now())'; }
q() { docker exec "$1" psql -U evcharge -d "$2" -tAc "$3"; } # container database sql

# Append-only or insert-mostly tables whose row count can only grow while the load runs.
TABLES="sessions.sessions sessions.reservations sessions.processed_events billing.invoices billing.invoice_lines billing.payments billing.psp_captures billing.processed_events"
counts() { # container -> JSON {"db.table": n}
  local c="$1" first=1 out="{"
  for t in $TABLES; do
    local n; n=$(q "$c" "${t%%.*}" "SELECT count(*) FROM ${t#*.}")
    [ $first = 1 ] && first=0 || out+=","
    out+="\"$t\":$n"
  done
  echo "$out}"
}
cleanup() {
  docker rm -f "$RESTORED" >/dev/null 2>&1 || true
  if [ "$KEEP" != 1 ]; then docker volume rm -f "$BK_VOL" "$RS_VOL" >/dev/null 2>&1 || true; fi
}
trap 'cleanup; stop_sampler' EXIT
cleanup
docker volume create "$BK_VOL" >/dev/null
docker volume create "$RS_VOL" >/dev/null

C_BEFORE=$(counts "$PRIM"); echo "$C_BEFORE" > "$OUT/counts-source-before-backup.json"
log "source row counts before the backup: $C_BEFORE"

# --- backup -------------------------------------------------------------------------------------------------------
phase during
T_B0=$(now_ms)
log "RUN pg_basebackup -h postgres -U replicator -D /backup/base -X stream -c fast  (physical, all databases)"
docker run --rm --network "$NET" -v "$BK_VOL:/backup" -e PGPASSWORD=replicator "$IMG" \
  pg_basebackup -h postgres -U replicator -D /backup/base -X stream -c fast -F plain -v > "$OUT/pg_basebackup.log" 2>&1
T_B1=$(now_ms)
BACKUP_KB=$(docker run --rm -v "$BK_VOL:/backup" alpine du -sk /backup/base | cut -f1)
BACKUP_LABEL=$(docker run --rm -v "$BK_VOL:/backup" alpine cat /backup/base/backup_label | head -3 | tr '\n' ';')
log "backup done in $(( T_B1 - T_B0 )) ms, ${BACKUP_KB} KB; $BACKUP_LABEL"
C_AFTER=$(counts "$PRIM"); echo "$C_AFTER" > "$OUT/counts-source-after-backup.json"

# --- the load keeps writing: this is what a backup-only recovery cannot bring back -------------------------------------
log "load continues for ${BACKUP_DELAY_S}s after the backup"
sleep "$BACKUP_DELAY_S"
T_D=$(now_ms)
C_DISASTER=$(counts "$PRIM"); echo "$C_DISASTER" > "$OUT/counts-source-at-disaster.json"
NEWEST_SRC=$(q "$PRIM" sessions "SELECT to_char(max(created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM reservations")
log "disaster moment T_D=$T_D; newest reservation on the source $NEWEST_SRC"

# --- restore ------------------------------------------------------------------------------------------------------
phase after
T_R0=$(now_ms)
log "RUN restore copy: backup volume -> fresh volume"
docker run --rm -v "$BK_VOL:/from:ro" -v "$RS_VOL:/to" alpine sh -c 'cp -a /from/base/. /to/ && chmod 700 /to'
T_R1=$(now_ms)
log "RUN start Postgres on the restored data directory (port $RESTORE_PORT)"
docker run -d --name "$RESTORED" --network "$NET" -p "$RESTORE_PORT:5432" -v "$RS_VOL:/var/lib/postgresql/18/docker" "$IMG" \
  postgres -c shared_buffers=512MB -c max_connections=400 >/dev/null
READY_S=$(wait_until 120 "restored instance accepts queries" bash -c "docker exec $RESTORED psql -U evcharge -d sessions -tAc 'select 1' | grep -q 1") || READY_S=null
T_R2=$(now_ms)
docker logs "$RESTORED" > "$OUT/restored-postgres.log" 2>&1 || true
log "restore: copy $(( T_R1 - T_R0 )) ms, start to first query $(( T_R2 - T_R1 )) ms, total $(( T_R2 - T_R0 )) ms"
C_RESTORED=$(counts "$RESTORED"); echo "$C_RESTORED" > "$OUT/counts-restored.json"
log "restored row counts: $C_RESTORED"

# --- integrity ------------------------------------------------------------------------------------------------------
sql_out() { q "$RESTORED" "$1" "$2" | tr -d '[:space:]'; }
OVERLAPS=$(sql_out sessions "SELECT count(*) FROM reservations a JOIN reservations b ON a.connector_pk = b.connector_pk AND a.reservation_id < b.reservation_id AND a.period && b.period AND a.status IN ('Pending','Active') AND b.status IN ('Pending','Active')")
INV_SUM_BAD=$(sql_out billing "SELECT count(*) FROM invoices i WHERE i.total_minor <> COALESCE((SELECT sum(l.amount_minor) FROM invoice_lines l WHERE l.invoice_id = i.invoice_id AND l.issued_at = i.issued_at), 0)")
INV_DUP_SESSION=$(sql_out billing "SELECT count(*) FROM (SELECT session_id FROM invoices GROUP BY session_id HAVING count(*) > 1) d")
REGISTRY_GAP=$(sql_out billing "SELECT (SELECT count(*) FROM invoices) - (SELECT count(*) FROM invoice_registry)")
PAY_DIFF=$(sql_out billing "SELECT count(*) FROM payments p JOIN invoices i ON i.invoice_id = p.invoice_id WHERE p.amount_minor <> i.total_minor")
PAY_ORPHAN=$(sql_out billing "SELECT count(*) FROM payments p WHERE NOT EXISTS (SELECT 1 FROM invoices i WHERE i.invoice_id = p.invoice_id)")
CAPTURE_GAP=$(sql_out billing "SELECT COALESCE((SELECT sum(amount_minor) FROM payments),0) - COALESCE((SELECT sum(amount_minor) FROM psp_captures),0)")
SESSION_TIME_BAD=$(sql_out sessions "SELECT count(*) FROM sessions WHERE stopped_at IS NOT NULL AND stopped_at < started_at")
NEWEST_RESTORED=$(q "$RESTORED" sessions "SELECT to_char(max(created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM reservations")
log "integrity: reservation overlaps=$OVERLAPS invoices total<>lines=$INV_SUM_BAD sessions with 2 invoices=$INV_DUP_SESSION invoices-registry=$REGISTRY_GAP payments<>invoice=$PAY_DIFF payments without invoice=$PAY_ORPHAN payments-captures sum gap=$CAPTURE_GAP sessions stopped<started=$SESSION_TIME_BAD"

# Every row count in the restored copy must lie between the source counts taken just before and just after the backup.
RANGE_BAD=$(node -e '
  const [b, a, r] = process.argv.slice(1).map(JSON.parse);
  console.log(Object.keys(b).filter((k) => !(r[k] >= b[k] && r[k] <= a[k])).length);
' "$C_BEFORE" "$C_AFTER" "$C_RESTORED")

# Data age (RPO of a backup-only recovery): the writes made between the backup and the disaster are gone.
node -e '
  const [d, r, b, a, tb0, tb1, tr0, tr1, tr2, td, newestSrc, newestRes, kb] = process.argv.slice(1);
  const D = JSON.parse(d), R = JSON.parse(r), B = JSON.parse(b), A = JSON.parse(a);
  const lost = Object.fromEntries(Object.keys(D).map((k) => [k, D[k] - R[k]]));
  const out = {
    backupMs: Number(tb1) - Number(tb0), backupKb: Number(kb),
    restoreCopyMs: Number(tr1) - Number(tr0), restoreStartToReadyMs: Number(tr2) - Number(tr1), restoreTotalMs: Number(tr2) - Number(tr0),
    backupFinishedToDisasterS: (Number(td) - Number(tb1)) / 1000,
    newestReservationSource: newestSrc, newestReservationRestored: newestRes,
    dataAgeS: Math.round((Date.parse(newestSrc) - Date.parse(newestRes)) / 100) / 10,
    rowsLostPerTable: lost,
    countsBeforeBackup: B, countsAfterBackup: A,
  };
  require("fs").writeFileSync(process.argv[14], JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify({ dataAgeS: out.dataAgeS, backupMs: out.backupMs, restoreTotalMs: out.restoreTotalMs, rowsLost: lost }));
' "$C_DISASTER" "$C_RESTORED" "$C_BEFORE" "$C_AFTER" "$T_B0" "$T_B1" "$T_R0" "$T_R1" "$T_R2" "$T_D" "$NEWEST_SRC" "$NEWEST_RESTORED" "$BACKUP_KB" "$OUT/summary.json" >&2

check "restore_instance_ready" "$([ "$READY_S" = null ] && echo null || echo 1)" eq 1
check "restored_counts_within_backup_window" "$RANGE_BAD" eq 0
check "reservation_overlaps_in_restored_db" "$OVERLAPS" eq 0
check "invoices_total_differs_from_lines" "$INV_SUM_BAD" eq 0
check "sessions_with_more_than_one_invoice" "$INV_DUP_SESSION" eq 0
check "invoices_minus_registry_rows" "$REGISTRY_GAP" eq 0
check "payments_amount_differs_from_invoice" "$PAY_DIFF" eq 0
check "payments_without_invoice" "$PAY_ORPHAN" eq 0
check "payments_minus_psp_captures_sum" "$CAPTURE_GAP" eq 0
check "sessions_stopped_before_started" "$SESSION_TIME_BAD" eq 0
log "backup time $(node -e 'console.log((JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).backupMs/1000).toFixed(1))' "$OUT/summary.json") s, restore time $(node -e 'console.log((JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).restoreTotalMs/1000).toFixed(1))' "$OUT/summary.json") s, data age $(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).dataAgeS)' "$OUT/summary.json") s (recorded in summary.json, not pass/fail)"
finish
