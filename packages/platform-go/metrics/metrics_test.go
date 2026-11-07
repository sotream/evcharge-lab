package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMiddlewareRecordsRouteAndStatus(t *testing.T) {
	reg := New()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tariffs/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	mux.HandleFunc("POST /v1/quote", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	h := reg.Middleware(mux)

	for _, rq := range []struct{ method, path string }{
		{http.MethodGet, "/v1/tariffs/a"},
		{http.MethodGet, "/v1/tariffs/b"},
		{http.MethodPost, "/v1/quote"},
		{http.MethodGet, "/nope/123"},
	} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), rq.method, rq.path, nil))
	}

	want := map[string]uint64{
		`method="GET",route="/v1/tariffs/{id}",status="418"`: 2,
		`method="POST",route="/v1/quote",status="200"`:       1,
		`method="GET",route="unmatched",status="404"`:        1,
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]uint64{}
	for _, f := range families {
		if f.GetName() != "http_server_requests_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			var parts []string
			for _, l := range m.GetLabel() {
				parts = append(parts, l.GetName()+`="`+l.GetValue()+`"`)
			}
			got[strings.Join(parts, ",")] = m.GetHistogram().GetSampleCount()
		}
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: got %d want %d (all: %v)", k, got[k], v, got)
		}
	}
}

func TestHandlerExposesCollectors(t *testing.T) {
	reg := New()
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "go_goroutines") {
		t.Errorf("missing go_goroutines in:\n%s", body)
	}
	if n, err := testutil.GatherAndCount(reg.Registry, "go_goroutines"); err != nil || n != 1 {
		t.Fatalf("go collector: %d %v", n, err)
	}
}

func TestRoute(t *testing.T) {
	for p, want := range map[string]string{"": UnmatchedRoute, "/a": "/a", "GET /a/{id}": "/a/{id}"} {
		if got := Route(&http.Request{Pattern: p}); got != want {
			t.Errorf("%q: got %q", p, got)
		}
	}
}

func TestStatusWriterFlush(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec}
	var _ http.Flusher = sw
	sw.Flush()
	if !rec.Flushed {
		t.Fatal("flush not forwarded")
	}
}
