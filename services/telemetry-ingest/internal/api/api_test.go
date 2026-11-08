package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseSeriesQuery(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	q, err := ParseSeriesQuery("", "", "", now)
	if err != nil || !q.To.Equal(now) || !q.From.Equal(now.Add(-time.Hour)) || q.Step != time.Minute {
		t.Fatalf("defaults: %+v %v", q, err)
	}
	q, err = ParseSeriesQuery("2026-11-08T00:00:00Z", "2026-11-08T06:00:00Z", "15m", now)
	if err != nil || q.Step != 15*time.Minute || q.To.Sub(q.From) != 6*time.Hour {
		t.Fatalf("explicit: %+v %v", q, err)
	}
	bad := [][3]string{
		{"x", "", ""},
		{"", "y", ""},
		{"", "", "30s"},
		{"", "", "90s"},
		{"", "", "nope"},
		{"2026-11-08T12:00:00Z", "2026-11-08T11:00:00Z", ""},
		{"2026-11-01T00:00:00Z", "2026-11-08T00:00:00Z", "1m"}, // > 1440 points
	}
	for _, b := range bad {
		if _, err := ParseSeriesQuery(b[0], b[1], b[2], now); err == nil {
			t.Errorf("%v: expected error", b)
		}
	}
}

func TestRoutesRequireInternalToken(t *testing.T) {
	mux := http.NewServeMux()
	(&Handler{}).Register(mux, "0123456789abcdef")
	for _, path := range []string{"/v1/stations/CP-1/connectors/1/power", "/v1/stations/CP-1/connectors/1/latest"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		req.Header.Set("x-driver-id", "4c1f0f6e-1f43-4b0c-9a83-2cf1e8a0f001") // never trusted without the token
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status %d", path, rec.Code)
		}
	}
}

func TestBadPathIDs(t *testing.T) {
	mux := http.NewServeMux()
	(&Handler{}).Register(mux, "0123456789abcdef")
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/stations/CP-1/connectors/abc/latest", nil)
	req.Header.Set("x-internal-token", "0123456789abcdef")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rec.Code)
	}
}
