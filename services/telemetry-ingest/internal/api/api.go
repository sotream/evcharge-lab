// Package api serves the telemetry query endpoints (internal token required,
// ADR 0010):
//
//	GET /v1/stations/{id}/connectors/{cid}/power?from=&to=&step=  -> PowerSeriesRes from power_1m
//	GET /v1/stations/{id}/connectors/{cid}/latest                 -> latest live power (Redis, DB fallback)
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/sotream/evcharge-lab/packages/platform-go/httpx"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/ingest"
)

// PowerPoint / PowerSeriesRes mirror api.PowerSeriesRes in packages/contracts.
type PowerPoint struct {
	T    time.Time `json:"t"`
	AvgW float64   `json:"avgW"`
	MaxW float64   `json:"maxW"`
}

type PowerSeriesRes struct {
	StationID   string       `json:"stationId"`
	ConnectorID int          `json:"connectorId"`
	Points      []PowerPoint `json:"points"`
}

// LatestRes is the latest live power of a connector. Source is "redis" (written
// after each flush, TTL 5 min) or "db" (newest stored reading, possibly old).
type LatestRes struct {
	StationID   string    `json:"stationId"`
	ConnectorID int       `json:"connectorId"`
	PowerW      int64     `json:"powerW"`
	At          time.Time `json:"at"`
	Source      string    `json:"source"`
}

// MaxPoints bounds one series response.
const MaxPoints = 1440

// Handler holds the read-side dependencies.
type Handler struct {
	DB           *pgxpool.Pool
	Redis        redis.UniversalClient
	QueryTimeout time.Duration // default 3s
	Now          func() time.Time
}

// Register mounts the routes on mux behind the internal-token middleware.
func (h *Handler) Register(mux *http.ServeMux, internalToken string) {
	auth := httpx.RequireInternalToken(internalToken)
	mux.Handle("GET /v1/stations/{id}/connectors/{cid}/power", auth(http.HandlerFunc(h.power)))
	mux.Handle("GET /v1/stations/{id}/connectors/{cid}/latest", auth(http.HandlerFunc(h.latest)))
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func (h *Handler) ctx(r *http.Request) (context.Context, context.CancelFunc) {
	t := h.QueryTimeout
	if t <= 0 {
		t = 3 * time.Second
	}
	return context.WithTimeout(r.Context(), t)
}

// SeriesQuery is a validated power series request.
type SeriesQuery struct {
	From, To time.Time
	Step     time.Duration
}

// ParseSeriesQuery validates from/to (RFC 3339, default the last hour) and
// step (Go duration, whole minutes, default 1m widened to fit MaxPoints, at most MaxPoints buckets).
func ParseSeriesQuery(from, to, step string, now time.Time) (SeriesQuery, error) {
	q := SeriesQuery{To: now, Step: time.Minute}
	var err error
	if to != "" {
		if q.To, err = time.Parse(time.RFC3339Nano, to); err != nil {
			return q, fmt.Errorf("to: %w", err)
		}
	}
	q.From = q.To.Add(-time.Hour)
	if from != "" {
		if q.From, err = time.Parse(time.RFC3339Nano, from); err != nil {
			return q, fmt.Errorf("from: %w", err)
		}
	}
	if step != "" {
		if q.Step, err = time.ParseDuration(step); err != nil {
			return q, fmt.Errorf("step: %w", err)
		}
	} else if q.From.Before(q.To) && q.To.Sub(q.From)/q.Step > MaxPoints {
		// No step asked for: widen it to the smallest whole number of minutes that fits MaxPoints, so a session longer
		// than 24 h still gets an answer. An explicit step that is too small stays an error.
		q.Step = ((q.To.Sub(q.From) + MaxPoints*time.Minute - 1) / (MaxPoints * time.Minute)) * time.Minute
	}
	switch {
	case q.Step < time.Minute || q.Step%time.Minute != 0:
		return q, errors.New("step must be a whole number of minutes (power_1m resolution)")
	case !q.From.Before(q.To):
		return q, errors.New("from must be before to")
	case q.To.Sub(q.From)/q.Step > MaxPoints:
		return q, fmt.Errorf("range/step exceeds %d points", MaxPoints)
	}
	return q, nil
}

func pathIDs(r *http.Request) (string, int, error) {
	id := r.PathValue("id")
	cid, err := strconv.Atoi(r.PathValue("cid"))
	if id == "" || err != nil || cid < 0 || cid > 32767 {
		return "", 0, errors.New("invalid station or connector id")
	}
	return id, cid, nil
}

// Re-buckets the 1-minute aggregate; avg is weighted by samples per minute.
const seriesSQL = `SELECT time_bucket(make_interval(secs => $4::float8), bucket) AS t,
       (sum(avg_power_w::float8 * samples) / NULLIF(sum(samples), 0))::float8 AS avg_w,
       max(max_power_w)::float8 AS max_w
FROM power_1m
WHERE station_id = $1 AND connector_id = $2 AND bucket >= $3::timestamptz AND bucket < $5::timestamptz
  AND avg_power_w IS NOT NULL
GROUP BY 1 ORDER BY 1`

func (h *Handler) power(w http.ResponseWriter, r *http.Request) {
	id, cid, err := pathIDs(r)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_path", err.Error())
		return
	}
	qp := r.URL.Query()
	q, err := ParseSeriesQuery(qp.Get("from"), qp.Get("to"), qp.Get("step"), h.now())
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	ctx, cancel := h.ctx(r)
	defer cancel()
	rows, err := h.DB.Query(ctx, seriesSQL, id, cid, q.From, q.Step.Seconds(), q.To)
	if err != nil {
		httpx.WriteError(w, fmt.Errorf("power series: %w", err))
		return
	}
	res := PowerSeriesRes{StationID: id, ConnectorID: cid, Points: []PowerPoint{}}
	for rows.Next() {
		var pt PowerPoint
		var avg, mx *float64
		if err := rows.Scan(&pt.T, &avg, &mx); err != nil {
			rows.Close()
			httpx.WriteError(w, fmt.Errorf("power series scan: %w", err))
			return
		}
		if avg == nil || mx == nil {
			continue
		}
		pt.T, pt.AvgW, pt.MaxW = pt.T.UTC(), *avg, *mx
		res.Points = append(res.Points, pt)
	}
	if err := rows.Err(); err != nil {
		httpx.WriteError(w, fmt.Errorf("power series rows: %w", err))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, res)
}

func (h *Handler) latest(w http.ResponseWriter, r *http.Request) {
	id, cid, err := pathIDs(r)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_path", err.Error())
		return
	}
	ctx, cancel := h.ctx(r)
	defer cancel()
	if h.Redis != nil {
		v, err := h.Redis.HGetAll(ctx, ingest.PowerKey(id, cid)).Result()
		if err == nil && v["w"] != "" {
			pw, perr := strconv.ParseInt(v["w"], 10, 64)
			at, terr := time.Parse(ingest.TSFormat, v["ts"])
			if perr == nil && terr == nil {
				httpx.WriteJSON(w, http.StatusOK, LatestRes{StationID: id, ConnectorID: cid, PowerW: pw, At: at.UTC(), Source: "redis"})
				return
			}
		}
	}
	var res LatestRes
	err = h.DB.QueryRow(ctx, `SELECT time, power_w FROM meter_readings
		WHERE station_id = $1 AND connector_id = $2 AND power_w IS NOT NULL ORDER BY time DESC LIMIT 1`, id, cid).Scan(&res.At, &res.PowerW)
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.Error(w, http.StatusNotFound, "not_found", "no power reading for this connector")
		return
	}
	if err != nil {
		httpx.WriteError(w, fmt.Errorf("latest power: %w", err))
		return
	}
	res.StationID, res.ConnectorID, res.At, res.Source = id, cid, res.At.UTC(), "db"
	httpx.WriteJSON(w, http.StatusOK, res)
}
