package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func get(t *testing.T, c *Checker, path string) (int, Report) {
	t.Helper()
	mux := http.NewServeMux()
	c.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	var rep Report
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatalf("bad body %q", rec.Body.String())
	}
	return rec.Code, rep
}

func TestLiveAlwaysOK(t *testing.T) {
	c := New()
	c.AddReadinessCheck("bad", func(context.Context) error { return errors.New("down") })
	c.SetDraining(true)
	if code, _ := get(t, c, "/health/live"); code != http.StatusOK {
		t.Fatalf("live = %d", code)
	}
}

func TestReadyAllOK(t *testing.T) {
	c := New()
	c.AddReadinessCheck("pg", func(context.Context) error { return nil })
	code, rep := get(t, c, "/health/ready")
	if code != http.StatusOK || rep.Status != "ok" || rep.Checks["pg"] != "ok" {
		t.Fatalf("%d %+v", code, rep)
	}
}

func TestReadyFailingCheck(t *testing.T) {
	c := New()
	c.AddReadinessCheck("pg", func(context.Context) error { return nil })
	c.AddReadinessCheck("redis", func(context.Context) error { return errors.New("conn refused") })
	code, rep := get(t, c, "/health/ready")
	if code != http.StatusServiceUnavailable || rep.Status != "fail" || rep.Checks["redis"] != "conn refused" || rep.Checks["pg"] != "ok" {
		t.Fatalf("%d %+v", code, rep)
	}
}

func TestReadyPerCheckTimeout(t *testing.T) {
	c := New()
	block := make(chan struct{})
	defer close(block)
	c.AddReadinessCheck("stuck", func(context.Context) error { <-block; return nil }, WithTimeout(20*time.Millisecond))
	start := time.Now()
	code, rep := get(t, c, "/health/ready")
	if code != http.StatusServiceUnavailable || rep.Checks["stuck"] != "timeout" {
		t.Fatalf("%d %+v", code, rep)
	}
	if time.Since(start) > time.Second {
		t.Fatal("timeout not enforced")
	}
}

func TestReadyPanicIsFailure(t *testing.T) {
	c := New()
	c.AddReadinessCheck("p", func(context.Context) error { panic("x") })
	if code, rep := get(t, c, "/health/ready"); code != http.StatusServiceUnavailable || rep.Checks["p"] != "check panicked" {
		t.Fatalf("%d %+v", code, rep)
	}
}

func TestDrainingMakesReady503(t *testing.T) {
	c := New()
	c.SetDraining(true)
	code, rep := get(t, c, "/health/ready")
	if code != http.StatusServiceUnavailable || rep.Status != "draining" {
		t.Fatalf("%d %+v", code, rep)
	}
	c.SetDraining(false)
	if code, _ := get(t, c, "/health/ready"); code != http.StatusOK {
		t.Fatalf("after undrain %d", code)
	}
}
