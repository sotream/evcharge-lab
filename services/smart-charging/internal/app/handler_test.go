package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
	"github.com/sotream/evcharge-lab/packages/platform-go/config"
	"github.com/sotream/evcharge-lab/packages/platform-go/httpx"

	"github.com/sotream/evcharge-lab/services/smart-charging/internal/alloc"
)

const token = "test-internal-token-0123456789"

var t0 = time.Date(2026, 11, 8, 18, 0, 0, 0, time.UTC)

func newHandler(t *testing.T, allocate Allocator) (http.Handler, *Metrics) {
	t.Helper()
	m := NewMetrics(prometheus.NewRegistry())
	h := &AllocateHandler{Log: slog.New(slog.DiscardHandler), Metrics: m, Allocate: allocate, Now: func() time.Time { return t0 }}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/allocate", httpx.RequireInternalToken(token)(h))
	return mux, m
}

const validBody = `{
  "siteId": "site-1", "siteCapW": 30000, "idleConnectors": 2, "startW": 4140,
  "sessions": [
    {"sessionId": "7b0a3c39-3a8e-4c6b-9a43-3c9a3f1c2b11", "stationId": "st-1", "connectorId": 1,
     "maxW": 22000, "minW": 4140, "measuredW": 5000, "priority": 0, "startedAt": "2026-11-08T17:00:00Z"},
    {"sessionId": "7b0a3c39-3a8e-4c6b-9a43-3c9a3f1c2b12", "stationId": "st-1", "connectorId": 2,
     "maxW": 22000, "minW": 4140, "startedAt": "2026-11-08T17:30:00+01:00"}
  ]
}`

func post(h http.Handler, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/allocate", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-internal-token", token)
	for k, v := range hdr {
		if v == "" {
			req.Header.Del(k)
		} else {
			req.Header.Set(k, v)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAllocateOK(t *testing.T) {
	h, m := newHandler(t, alloc.Allocate)
	rec := post(h, validBody, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var res contracts.AllocateRes
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	// budget 30000 - 8280 = 21720. Session 2 started 17:30+01:00 = 16:30Z, before
	// session 1 (17:00Z), so it comes first. Session 1 has run an
	// hour with 5000 W measured: demand 6000. Session 2 takes the rest: 15720.
	want := contracts.AllocateRes{SiteID: "site-1", Strategy: "optimal", TotalAllocatedW: 21_720, ReserveW: 8280,
		Allocations: []contracts.Allocation{
			{SessionID: "7b0a3c39-3a8e-4c6b-9a43-3c9a3f1c2b12", LimitW: 15_720},
			{SessionID: "7b0a3c39-3a8e-4c6b-9a43-3c9a3f1c2b11", LimitW: 6000},
		}}
	if fmt.Sprint(res) != fmt.Sprint(want) {
		t.Errorf("got  %+v\nwant %+v", res, want)
	}
	if got := testutil.ToFloat64(m.allocations.WithLabelValues("ok")); got != 1 {
		t.Errorf("allocations_total{ok} = %v", got)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("content type %q", rec.Header().Get("Content-Type"))
	}
	// Same request, same bytes.
	if again := post(h, validBody, nil); !bytes.Equal(again.Body.Bytes(), rec.Body.Bytes()) {
		t.Error("response not deterministic")
	}
}

func TestComputedAtOverridesClock(t *testing.T) {
	h, _ := newHandler(t, alloc.Allocate)
	// computedAt only 30 s after session 1 started: its measurement is ignored.
	body := strings.Replace(validBody, `"startW": 4140,`, `"startW": 4140, "computedAt": "2026-11-08T17:00:30Z",`, 1)
	body = strings.Replace(body, "2026-11-08T17:30:00+01:00", "2026-11-08T16:30:00Z", 1)
	rec := post(h, body, nil)
	var res contracts.AllocateRes
	_ = json.Unmarshal(rec.Body.Bytes(), &res)
	if rec.Code != http.StatusOK || res.Allocations[1].LimitW != 10_860 {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
}

func TestAllocateEmptySessionsIsArray(t *testing.T) {
	h, _ := newHandler(t, alloc.Allocate)
	rec := post(h, `{"siteId":"s","siteCapW":10000,"idleConnectors":2,"startW":4140,"sessions":[]}`, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"allocations":[]`) || !strings.Contains(rec.Body.String(), `"reserveW":8280`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestAllocateRejects(t *testing.T) {
	big := `{"siteId":"s","siteCapW":1,"idleConnectors":0,"startW":0,"sessions":[` + strings.Repeat(`{"sessionId":"x"},`, 40_000) + `{}]}`
	for _, tc := range []struct {
		name   string
		body   string
		hdr    map[string]string
		status int
		code   string
	}{
		{"no token", validBody, map[string]string{"x-internal-token": ""}, 401, "unauthorized"},
		{"wrong token", validBody, map[string]string{"x-internal-token": "nope-nope-nope-nope"}, 401, "unauthorized"},
		{"malformed", `{"siteId":`, nil, 400, "invalid_json"},
		{"unknown field", `{"siteId":"s","bogus":1}`, nil, 400, "unknown_field"},
		{"wrong type", `{"siteId":"s","siteCapW":"lots"}`, nil, 400, "invalid_json"},
		{"fractional watts", `{"siteId":"s","siteCapW":1.5,"idleConnectors":0,"startW":0,"sessions":[]}`, nil, 400, "invalid_json"},
		{"missing fields", `{"siteId":"s"}`, nil, 400, "validation_failed"},
		{"missing session field", strings.Replace(validBody, `"minW": 4140, "measuredW"`, `"measuredW"`, 1), nil, 400, "validation_failed"},
		{"minW > maxW", strings.Replace(validBody, `"maxW": 22000, "minW": 4140, "measuredW"`, `"maxW": 1000, "minW": 4140, "measuredW"`, 1), nil, 400, "validation_failed"},
		{"negative cap", strings.Replace(validBody, `30000`, `-1`, 1), nil, 400, "validation_failed"},
		{"bad uuid", strings.Replace(validBody, `7b0a3c39-3a8e-4c6b-9a43-3c9a3f1c2b11`, `abc`, 1), nil, 400, "validation_failed"},
		{"priority out of range", strings.Replace(validBody, `"priority": 0`, `"priority": 11`, 1), nil, 400, "validation_failed"},
		{"reserve exceeds cap", strings.Replace(validBody, `"idleConnectors": 2`, `"idleConnectors": 7`, 1), nil, 400, "infeasible_reserve"},
		{"wrong content type", validBody, map[string]string{"Content-Type": "text/plain"}, 415, "unsupported_media_type"},
		{"body too large", big, nil, 413, "body_too_large"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newHandler(t, alloc.Allocate)
			rec := post(h, tc.body, tc.hdr)
			if rec.Code != tc.status {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.status, rec.Body)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Errorf("content type %q", ct)
			}
			var p contracts.Problem
			if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || p.Code != tc.code || p.Status != tc.status {
				t.Errorf("problem %+v (err %v), want code %s", p, err, tc.code)
			}
		})
	}
}

func TestInvariantViolationIs500(t *testing.T) {
	broken := func(req *contracts.AllocateReq, now time.Time) (alloc.Result, error) {
		r, err := alloc.Allocate(req, now)
		if err != nil {
			return r, err
		}
		r.Res.Allocations[0].LimitW += 1_000_000 // a buggy allocator
		r.Details[0].LimitW += 1_000_000
		r.Res.TotalAllocatedW += 1_000_000
		return r, alloc.Check(req, &r)
	}
	h, m := newHandler(t, broken)
	rec := post(h, validBody, nil)
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "invariant_violation") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "limitW") {
		t.Error("violating limits leaked in the error body")
	}
	if got := testutil.ToFloat64(m.invariantViolations); got != 1 {
		t.Errorf("invariant_violations_total = %v", got)
	}
}

func TestConfigFailsFast(t *testing.T) {
	var cfg Config
	err := config.LoadFrom(&cfg, map[string]string{"INTERNAL_TOKEN": "short", "LOG_LEVEL": "loud"})
	if err == nil {
		t.Fatal("invalid config accepted")
	}
	for _, want := range []string{"SERVICE_NAME", "INTERNAL_TOKEN", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
	if err := config.LoadFrom(&cfg, map[string]string{"SERVICE_NAME": "smart-charging", "INTERNAL_TOKEN": token}); err != nil {
		t.Fatal(err)
	}
}
