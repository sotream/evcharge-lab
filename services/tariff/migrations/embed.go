// Package migrations embeds the tariff database migrations (pg.Migrate).
package migrations

import "embed"

// FS holds the NNNN_name.sql files at its root.
//
//go:embed *.sql
var FS embed.FS
