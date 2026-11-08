package ingest

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sotream/evcharge-lab/packages/platform-go/pg"
)

// Sink durably stores a batch of rows and reports how many were new
// (inserted) versus already present (deduplicated by the unique index).
type Sink interface {
	Write(ctx context.Context, rows []Row) (inserted int64, err error)
}

// IngestURL returns dsn with synchronous_commit=off as a connection runtime
// parameter: the ingest connection may lose ~600 ms of commits on a Postgres
// crash, which ADR 0008 allows (RPO <= 5 min). Query connections keep the
// server default.
func IngestURL(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	q := u.Query()
	q.Set("synchronous_commit", "off")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// ConnectIngestPool opens the dedicated ingest pool (synchronous_commit=off).
func ConnectIngestPool(ctx context.Context, dsn, appName string, maxConns int32) (*pgxpool.Pool, error) {
	u, err := IngestURL(dsn)
	if err != nil {
		return nil, err
	}
	return pg.Connect(ctx, pg.Config{URL: u, ApplicationName: appName, MaxConns: maxConns})
}

// CopyWriter is the production sink: COPY into a temporary staging table,
// then INSERT ... SELECT ... ON CONFLICT DO NOTHING into the hypertable, in
// one transaction (ADR 0008).
type CopyWriter struct {
	Pool *pgxpool.Pool
}

const stagingDDL = `CREATE TEMP TABLE IF NOT EXISTS meter_readings_staging
  (LIKE meter_readings INCLUDING DEFAULTS) ON COMMIT DELETE ROWS`

const mergeSQL = `INSERT INTO meter_readings (time, station_id, connector_id, transaction_id, energy_wh, power_w, current_a, voltage_v, soc_pct, replayed)
SELECT time, station_id, connector_id, transaction_id, energy_wh, power_w, current_a, voltage_v, soc_pct, replayed
FROM meter_readings_staging
ON CONFLICT DO NOTHING`

func (w *CopyWriter) Write(ctx context.Context, rows []Row) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, stagingDDL); err != nil {
		return 0, fmt.Errorf("staging table: %w", err)
	}
	n, err := tx.CopyFrom(ctx, pgx.Identifier{"meter_readings_staging"}, Columns, pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
		return rows[i].Values(), nil
	}))
	if err != nil {
		return 0, fmt.Errorf("copy: %w", err)
	}
	if n != int64(len(rows)) {
		return 0, fmt.Errorf("copy: wrote %d of %d rows", n, len(rows))
	}
	tag, err := tx.Exec(ctx, mergeSQL)
	if err != nil {
		return 0, fmt.Errorf("merge: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RowByRowWriter is the measured baseline that ADR 0008 rejects: one
// autocommitted INSERT per row. It exists only for cmd/ingestbench.
type RowByRowWriter struct {
	Pool *pgxpool.Pool
}

const insertOneSQL = `INSERT INTO meter_readings (time, station_id, connector_id, transaction_id, energy_wh, power_w, current_a, voltage_v, soc_pct, replayed)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING`

func (w *RowByRowWriter) Write(ctx context.Context, rows []Row) (int64, error) {
	var inserted int64
	for i := range rows {
		tag, err := w.Pool.Exec(ctx, insertOneSQL, rows[i].Values()...)
		if err != nil {
			return inserted, fmt.Errorf("insert row %d: %w", i, err)
		}
		inserted += tag.RowsAffected()
	}
	return inserted, nil
}
