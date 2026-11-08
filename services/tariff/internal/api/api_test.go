package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
	"github.com/sotream/evcharge-lab/services/tariff/internal/pricing"
	"github.com/sotream/evcharge-lab/services/tariff/internal/store"
)

const token = "test-internal-token-123"

type fake map[string]*pricing.Tariff

func (f fake) Get(_ context.Context, id string) (*pricing.Tariff, error) {
	if id == "broken" {
		return nil, errors.New("db down")
	}
	t, ok := f[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return t, nil
}

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	tr, err := pricing.NewTariff("std", []*pricing.Version{{TariffID: "std", Version: 1,
		ValidFrom: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), Currency: "EUR", Timezone: "Europe/Berlin",
		SessionFeeMinor: 49, EnergyPriceMinorPerKWh: 39}})
	if err != nil {
		t.Fatal(err)
	}
	a := New(fake{"std": tr}, slog.New(slog.DiscardHandler), prometheus.NewRegistry())
	srv := httptest.NewServer(a.Routes(token))
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url, body, tok string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("x-internal-token", tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestQuoteAndErrors(t *testing.T) {
	srv := newServer(t)
	ok := `{"tariffId":"std","startedAt":"2026-01-15T10:00:00Z","stoppedAt":"2026-01-15T10:45:00Z","energyWh":1234}`
	status, body := do(t, "POST", srv.URL+"/v1/quote", ok, token)
	if status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	var res contracts.QuoteRes
	if err := json.Unmarshal(body, &res); err != nil || res.TotalMinor != 98 || len(res.Lines) != 4 {
		t.Fatalf("%v %+v", err, res)
	}
	for _, c := range []struct {
		name, body, tok string
		status          int
		code            string
	}{
		{"no token", ok, "", 401, ""},
		{"wrong token", ok, "nope-nope-nope-nope", 401, ""},
		{"unknown field", `{"tariffId":"std","x":1}`, token, 400, ""},
		{"missing tariff id", `{"startedAt":"2026-01-15T10:00:00Z","stoppedAt":"2026-01-15T10:45:00Z","energyWh":1}`, token, 400, "validation"},
		{"stop before start", `{"tariffId":"std","startedAt":"2026-01-15T10:00:00Z","stoppedAt":"2026-01-15T09:45:00Z","energyWh":1}`, token, 400, "validation"},
		{"negative energy", `{"tariffId":"std","startedAt":"2026-01-15T10:00:00Z","stoppedAt":"2026-01-15T10:45:00Z","energyWh":-1}`, token, 400, "validation"},
		{"unknown tariff", strings.Replace(ok, `"std"`, `"nope"`, 1), token, 404, "tariff_not_found"},
		{"before first version", strings.Replace(ok, "2026-01-15T10:00", "2019-01-15T10:00", 1), token, 422, "no_tariff_version"},
		{"store down", strings.Replace(ok, `"std"`, `"broken"`, 1), token, 503, "unavailable"},
	} {
		t.Run(c.name, func(t *testing.T) {
			status, body := do(t, "POST", srv.URL+"/v1/quote", c.body, c.tok)
			if status != c.status {
				t.Fatalf("status %d want %d: %s", status, c.status, body)
			}
			if c.code != "" {
				var p contracts.Problem
				_ = json.Unmarshal(body, &p)
				if p.Code != c.code {
					t.Fatalf("code %q want %q", p.Code, c.code)
				}
			}
		})
	}
}

func TestGetTariff(t *testing.T) {
	srv := newServer(t)
	status, body := do(t, "GET", srv.URL+"/v1/tariffs/std", "", token)
	if status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	var dto TariffDTO
	if err := json.Unmarshal(body, &dto); err != nil || dto.TariffID != "std" || dto.Current == nil || *dto.Current != 1 ||
		len(dto.Versions) != 1 || dto.Versions[0].EnergyPriceMinorPerKWh != 39 {
		t.Fatalf("%v %s", err, body)
	}
	if status, _ := do(t, "GET", srv.URL+"/v1/tariffs/nope", "", token); status != 404 {
		t.Fatalf("status %d", status)
	}
	if status, _ := do(t, "GET", srv.URL+"/v1/tariffs/std", "", ""); status != 401 {
		t.Fatalf("status %d", status)
	}
}
