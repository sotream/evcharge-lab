package pg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockKey is the pg_advisory_lock key guarding migrations (advisory
// locks are per database, so services sharing an instance do not contend).
const migrationLockKey int64 = 0x65766368_6d696772 // "evchmigr"

var migrationName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// Migration is one parsed migration file.
type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string
}

// LoadMigrations reads NNNN_name.sql files from dir in fsys, sorted by
// version. Other .sql names and duplicate versions are errors; non-.sql files
// are ignored.
func LoadMigrations(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("pg: read migrations: %w", err)
	}
	var out []Migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() || path.Ext(e.Name()) != ".sql" {
			continue
		}
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("pg: migration %q does not match NNNN_name.sql", e.Name())
		}
		v, _ := strconv.Atoi(m[1])
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("pg: duplicate migration version %04d (%s, %s)", v, prev, e.Name())
		}
		seen[v] = e.Name()
		b, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("pg: read %s: %w", e.Name(), err)
		}
		sum := sha256.Sum256(b)
		out = append(out, Migration{Version: v, Name: m[2], SQL: string(b), Checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Migrate applies pending migrations from dir in fsys (typically an
// embed.FS) in version order. Each migration runs in its own transaction and
// is recorded in schema_migrations(version, name, checksum, applied_at). A
// session advisory lock serialises concurrent instances. An already-applied
// migration whose file content changed is an error. Statements that cannot
// run inside a transaction (CREATE INDEX CONCURRENTLY) are not supported.
// It returns the versions applied by this call.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, dir string) ([]int, error) {
	migs, err := LoadMigrations(fsys, dir)
	if err != nil {
		return nil, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("pg: acquire: %w", err)
	}
	defer conn.Release()

	// Migrations may legitimately run longer than the default statement_timeout.
	if _, err := conn.Exec(ctx, "SET statement_timeout = 0"); err != nil {
		return nil, fmt.Errorf("pg: disable statement_timeout: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return nil, fmt.Errorf("pg: advisory lock: %w", err)
	}
	defer func() {
		// Use a fresh context: ctx may be cancelled, the lock must still go.
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrationLockKey)
		_, _ = conn.Exec(context.WithoutCancel(ctx), "RESET statement_timeout")
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer     PRIMARY KEY,
		name       text        NOT NULL,
		checksum   text        NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return nil, fmt.Errorf("pg: create schema_migrations: %w", err)
	}

	applied := map[int]string{}
	rows, err := conn.Query(ctx, "SELECT version, checksum FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("pg: read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v int
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			rows.Close()
			return nil, err
		}
		applied[v] = sum
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var done []int
	for _, m := range migs {
		if sum, ok := applied[m.Version]; ok {
			if sum != m.Checksum {
				return done, fmt.Errorf("pg: migration %04d_%s was modified after being applied", m.Version, m.Name)
			}
			continue
		}
		if err := apply(ctx, conn.Conn(), m); err != nil {
			return done, err
		}
		done = append(done, m.Version)
	}
	return done, nil
}

func apply(ctx context.Context, conn *pgx.Conn, m Migration) (err error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg: begin %04d: %w", m.Version, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	// No arguments → simple protocol, so a file may hold several statements.
	if _, err = tx.Exec(ctx, m.SQL); err != nil {
		return fmt.Errorf("pg: apply %04d_%s: %w", m.Version, m.Name, err)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
		m.Version, m.Name, m.Checksum); err != nil {
		return fmt.Errorf("pg: record %04d: %w", m.Version, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg: commit %04d: %w", m.Version, err)
	}
	return nil
}
