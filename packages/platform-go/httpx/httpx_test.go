package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

func TestWriteProblem(t *testing.T) {
	rec := httptest.NewRecorder()
	Error(rec, http.StatusNotFound, "tariff_not_found", "no tariff t1")
	if rec.Code != 404 || rec.Header().Get("Content-Type") != ProblemContentType {
		t.Fatalf("%d %s", rec.Code, rec.Header())
	}
	var p contracts.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	want := contracts.Problem{Type: "about:blank", Title: "Not Found", Status: 404, Code: "tariff_not_found", Detail: "no tariff t1"}
	if p != want {
		t.Fatalf("got %+v", p)
	}
}

func TestWriteErrorHidesInternal(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, errors.New("db password is hunter2"))
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestReadProblem(t *testing.T) {
	rec := httptest.NewRecorder()
	Error(rec, http.StatusConflict, "dup", "already")
	pe := ReadProblem(rec.Result())
	if pe.Status != 409 || pe.Code != "dup" || pe.Detail != "already" {
		t.Fatalf("%+v", pe)
	}
	plain := &http.Response{StatusCode: 502, Header: http.Header{"Content-Type": {"text/plain"}}, Body: io.NopCloser(strings.NewReader("bad gateway"))}
	pe = ReadProblem(plain)
	if pe.Status != 502 || pe.Title != "Bad Gateway" || pe.Detail != "bad gateway" {
		t.Fatalf("%+v", pe)
	}
	if !strings.Contains(pe.Error(), "502") {
		t.Fatal(pe.Error())
	}
}

type quote struct {
	TariffID string `json:"tariffId"`
	EnergyWh int64  `json:"energyWh"`
}

func decode(body, ct string, limit int64) (quote, error) {
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(body))
	if ct != "" {
		r.Header.Set("Content-Type", ct)
	}
	var q quote
	err := DecodeJSON(httptest.NewRecorder(), r, &q, limit)
	return q, err
}

func TestDecodeJSON(t *testing.T) {
	q, err := decode(`{"tariffId":"t1","energyWh":5}`, "application/json; charset=utf-8", 0)
	if err != nil || q.TariffID != "t1" || q.EnergyWh != 5 {
		t.Fatalf("%+v %v", q, err)
	}
	cases := []struct {
		name, body, ct string
		limit          int64
		status         int
		code           string
	}{
		{"unknown field", `{"tariffId":"t1","extra":1}`, "application/json", 0, 400, "unknown_field"},
		{"syntax", `{"tariffId":`, "application/json", 0, 400, "invalid_json"},
		{"type", `{"energyWh":"x"}`, "application/json", 0, 400, "invalid_json"},
		{"empty", ``, "", 0, 400, "invalid_json"},
		{"trailing", `{} {}`, "", 0, 400, "invalid_json"},
		{"too large", `{"tariffId":"` + strings.Repeat("x", 100) + `"}`, "", 32, 413, "body_too_large"},
		{"media type", `{}`, "text/plain", 0, 415, "unsupported_media_type"},
	}
	for _, c := range cases {
		_, err := decode(c.body, c.ct, c.limit)
		var pe *ProblemError
		if !errors.As(err, &pe) || pe.Status != c.status || pe.Code != c.code {
			t.Errorf("%s: got %v", c.name, err)
		}
	}
}

func TestRequireInternalToken(t *testing.T) {
	h := RequireInternalToken("0123456789abcdef")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	for tok, want := range map[string]int{"": 401, "wrong": 401, "0123456789abcdef": 204} {
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		if tok != "" {
			r.Header.Set("x-internal-token", tok)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Code != want {
			t.Errorf("token %q: %d", tok, rec.Code)
		}
	}
	empty := RequireInternalToken("")(http.NotFoundHandler())
	rec := httptest.NewRecorder()
	empty.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	if rec.Code != 401 {
		t.Fatal("empty configured token must reject everything")
	}
}

func fastClient(attempts int) *Client {
	return NewClient(ClientOptions{MaxAttempts: attempts, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, AttemptTimeout: 200 * time.Millisecond, InternalToken: "0123456789abcdef"})
}

func flaky(failures int32, status int) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if n.Add(1) <= failures {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("X-Token", r.Header.Get(InternalTokenHeader))
		_, _ = w.Write(body)
	}))
	return srv, &n
}

func TestClientRetriesGETOn5xx(t *testing.T) {
	srv, n := flaky(2, 503)
	defer srv.Close()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	resp, err := fastClient(3).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || n.Load() != 3 || resp.Header.Get("X-Token") != "0123456789abcdef" {
		t.Fatalf("status %d attempts %d", resp.StatusCode, n.Load())
	}
}

func TestClientGivesUpAfterMaxAttempts(t *testing.T) {
	srv, n := flaky(10, 429)
	defer srv.Close()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	resp, err := fastClient(3).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 429 || n.Load() != 3 {
		t.Fatalf("status %d attempts %d", resp.StatusCode, n.Load())
	}
}

func TestClientNoRetryForPOSTWithoutKey(t *testing.T) {
	srv, n := flaky(1, 500)
	defer srv.Close()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader("{}"))
	resp, err := fastClient(3).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 500 || n.Load() != 1 {
		t.Fatalf("status %d attempts %d", resp.StatusCode, n.Load())
	}
}

func TestClientNoRetryOn4xx(t *testing.T) {
	srv, n := flaky(1, 400)
	defer srv.Close()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	resp, err := fastClient(3).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 400 || n.Load() != 1 {
		t.Fatalf("status %d attempts %d", resp.StatusCode, n.Load())
	}
}

func TestClientRetriesPOSTWithIdempotencyKeyAndReplaysBody(t *testing.T) {
	srv, n := flaky(1, 502)
	defer srv.Close()
	var out quote
	err := fastClient(3).DoJSON(context.Background(), http.MethodPost, srv.URL, quote{TariffID: "t1", EnergyWh: 9}, &out,
		http.Header{IdempotencyKeyHeader: {"k1"}})
	if err != nil {
		t.Fatal(err)
	}
	if out.TariffID != "t1" || out.EnergyWh != 9 || n.Load() != 2 {
		t.Fatalf("%+v attempts %d", out, n.Load())
	}
}

func TestClientAttemptTimeoutRetries(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
			return
		}
		_, _ = w.Write([]byte(`{"tariffId":"slow"}`))
	}))
	defer srv.Close()
	c := NewClient(ClientOptions{AttemptTimeout: 50 * time.Millisecond, BaseBackoff: time.Millisecond})
	var out quote
	if err := c.DoJSON(context.Background(), http.MethodGet, srv.URL, nil, &out, nil); err != nil {
		t.Fatal(err)
	}
	if out.TariffID != "slow" || n.Load() != 2 {
		t.Fatalf("%+v attempts %d", out, n.Load())
	}
}

func TestClientNetworkErrorRetriesThenFails(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // connection refused from now on
	start := time.Now()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	_, err := fastClient(3).Do(req) //nolint:bodyclose // err path
	if err == nil {
		t.Fatal("expected error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("took too long")
	}
}

func TestClientDoJSONProblem(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		Error(w, http.StatusUnprocessableEntity, "bad_window", "stop before start")
	}))
	defer srv.Close()
	err := fastClient(1).DoJSON(context.Background(), http.MethodPost, srv.URL, quote{}, nil, nil)
	var pe *ProblemError
	if !errors.As(err, &pe) || pe.Status != 422 || pe.Code != "bad_window" {
		t.Fatalf("%v", err)
	}
}

func TestBackoffBounded(t *testing.T) {
	c := NewClient(ClientOptions{BaseBackoff: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond})
	for attempt := 1; attempt < 70; attempt++ {
		if d := c.backoff(attempt); d < 0 || d >= 40*time.Millisecond {
			t.Fatalf("attempt %d: %s", attempt, d)
		}
	}
}
