package backfill

import (
	"context"
	"net/url"

	"github.com/jackc/pgx/v5"
)

// CreateDatabaseIfMissing connects to the "postgres" database on the same
// server as dsn and creates database name when it does not exist.
func CreateDatabaseIfMissing(ctx context.Context, dsn, name string) error {
	u, err := url.Parse(dsn)
	if err != nil {
		return err
	}
	u.Path = "/postgres"
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	return err
}

// DropDatabase drops database name (used by tests and after oversized runs).
func DropDatabase(ctx context.Context, dsn, name string) error {
	u, err := url.Parse(dsn)
	if err != nil {
		return err
	}
	u.Path = "/postgres"
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	return err
}
