package ingest

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// LatestTTL is how long a latest-power hash lives without a refresh (ADR 0008).
const LatestTTL = 5 * time.Minute

// TSFormat is the fixed-width UTC timestamp stored in the "ts" field.
const TSFormat = "2006-01-02T15:04:05.000Z07:00"

// PowerKey mirrors RedisKeys.connectorPower in packages/contracts.
func PowerKey(stationID string, connectorID int) string {
	return "power:" + stationID + ":" + strconv.Itoa(connectorID)
}

// LatestSink receives the newest live reading per connector after a flush.
type LatestSink interface {
	WriteLatest(ctx context.Context, rows []Row) error
}

// NewestLive picks, per connector, the newest non-replayed row that carries
// a power value. Replayed rows are history and never overwrite "latest".
func NewestLive(rows []Row) []Row {
	type key struct {
		s string
		c int16
	}
	idx := map[key]int{}
	out := make([]Row, 0, 16)
	for i := range rows {
		r := &rows[i]
		if r.Replayed || r.PowerW == nil {
			continue
		}
		k := key{r.StationID, r.ConnectorID}
		if j, ok := idx[k]; ok {
			if r.Time.After(out[j].Time) {
				out[j] = *r
			}
			continue
		}
		idx[k] = len(out)
		out = append(out, *r)
	}
	return out
}

// RedisLatest writes HSET power:{station}:{connector} w <W> ts <time> with a
// 5 min TTL, pipelined, for the newest live reading per connector.
type RedisLatest struct {
	Client  redis.UniversalClient
	Timeout time.Duration // per pipeline, default 1s
}

func (l *RedisLatest) WriteLatest(ctx context.Context, rows []Row) error {
	live := NewestLive(rows)
	if len(live) == 0 {
		return nil
	}
	timeout := l.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	pipe := l.Client.Pipeline()
	for i := range live {
		r := &live[i]
		k := PowerKey(r.StationID, int(r.ConnectorID))
		pipe.HSet(ctx, k, "w", *r.PowerW, "ts", r.Time.UTC().Format(TSFormat))
		pipe.Expire(ctx, k, LatestTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis latest: %w", err)
	}
	return nil
}
