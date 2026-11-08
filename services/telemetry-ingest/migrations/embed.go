// Package migrations embeds the canonical telemetry schema (ADR 0008). The SQL
// files are shared with scripts/backfill-telemetry and must not be edited once
// applied: add a new NNNN_name.sql file instead.
package migrations

import "embed"

// FS holds every NNNN_name.sql file of this directory.
//
//go:embed *.sql
var FS embed.FS
