package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LateAfter is how old a reading must be before we refresh it ourselves. The policy
// (migrations/0002_power_1m.sql) refreshes [now - start_offset (2 h), now - end_offset] every
// schedule_interval (1 min) on whole 1-minute buckets, so a reading is safe from the policy only while
// age < start_offset - bucket (1 min) - schedule_interval (1 min) - margin (8 min) = 1 h 50 min.
// Older rows are reported; reporting rows the policy would also catch only costs a redundant refresh.
const LateAfter = 110 * time.Minute

// LateNoter receives the time range of late readings after a durable batch.
type LateNoter interface {
	Note(minTs, maxTs time.Time)
}

// LateRefresher refreshes the power_1m continuous aggregate for readings the
// policy window no longer reaches (a station replaying its offline buffer, up
// to 24 h per ADR 0000), so the 1-minute rollup does not silently miss them.
type LateRefresher struct {
	db    *pgxpool.Pool
	every time.Duration
	log   *slog.Logger

	mu       sync.Mutex
	min, max time.Time
}

func NewLateRefresher(db *pgxpool.Pool, every time.Duration, logger *slog.Logger) *LateRefresher {
	return &LateRefresher{db: db, every: every, log: logger}
}

// Note merges [minTs, maxTs] into the one pending range. Safe for concurrent use.
func (r *LateRefresher) Note(minTs, maxTs time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.min.IsZero() || minTs.Before(r.min) {
		r.min = minTs
	}
	if maxTs.After(r.max) {
		r.max = maxTs
	}
}

func (r *LateRefresher) pending() (minTs, maxTs time.Time, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.min, r.max, !r.min.IsZero()
}

// Flush refreshes the pending range (whole minutes) and clears it; on error the
// range is kept for the next tick.
func (r *LateRefresher) Flush(ctx context.Context) error {
	r.mu.Lock()
	minTs, maxTs := r.min, r.max
	r.min, r.max = time.Time{}, time.Time{}
	r.mu.Unlock()
	if minTs.IsZero() {
		return nil
	}
	_, err := r.db.Exec(ctx, `CALL refresh_continuous_aggregate('power_1m', $1::timestamptz, $2::timestamptz)`,
		minTs.Truncate(time.Minute), maxTs.Truncate(time.Minute).Add(time.Minute))
	if err != nil {
		r.Note(minTs, maxTs)
	}
	return err
}

// Run flushes every `every` until ctx ends.
func (r *LateRefresher) Run(ctx context.Context) {
	t := time.NewTicker(r.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// Shutdown: do not lose the pending range; a fresh short context because ctx is already done.
			fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := r.Flush(fctx); err != nil {
				r.log.Warn("late rollup refresh at shutdown failed", "err", err)
			}
			cancel()
			return
		case <-t.C:
			if err := r.Flush(ctx); err != nil && ctx.Err() == nil {
				r.log.Warn("late rollup refresh failed; will retry", "err", err)
			}
		}
	}
}
