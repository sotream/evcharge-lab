package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"

	"github.com/sotream/evcharge-lab/services/smart-charging/internal/sitegen"
)

const token = "integration-internal-token-0123"

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("integration test: set INTEGRATION=1")
	}
}

func build(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "smart-charging")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

// syncBuffer collects the child's stdout safely.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func do(t *testing.T, method, url string, body []byte, hdr map[string]string) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestBinaryFailsFastOnBadConfig(t *testing.T) {
	requireIntegration(t)
	cmd := exec.CommandContext(t.Context(), build(t))
	cmd.Env = []string{"INTERNAL_TOKEN=short"}
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 2 {
		t.Fatalf("exit %v, want code 2\n%s", err, out)
	}
	for _, want := range []string{"SERVICE_NAME is required", "INTERNAL_TOKEN must be at least"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestBinaryServesAllocations(t *testing.T) {
	requireIntegration(t)
	bin := build(t)
	port := freePort(t)
	base := "http://127.0.0.1:" + strconv.Itoa(port)
	var stdout syncBuffer
	cmd := exec.CommandContext(t.Context(), bin)
	cmd.Env = []string{
		"SERVICE_NAME=smart-charging", "INTERNAL_TOKEN=" + token, "HTTP_PORT=" + strconv.Itoa(port),
		"LOG_LEVEL=debug", "SHUTDOWN_GRACE=6s",
	}
	cmd.Stdout, cmd.Stderr = &stdout, &stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		if err := <-exited; err != nil {
			t.Logf("service exit: %v", err)
		}
		if t.Failed() {
			t.Logf("service output:\n%s", stdout.String())
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/health/ready", nil)
		resp, err := http.DefaultClient.Do(req)
		cancel()
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("not ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if code, _ := do(t, http.MethodGet, base+"/health/live", nil, nil); code != 200 {
		t.Fatalf("live %d", code)
	}

	auth := map[string]string{"Content-Type": "application/json", "x-internal-token": token}
	r := rand.New(rand.NewPCG(11, 12))
	now := time.Now()
	site := sitegen.Site(r, "site-int", now)
	body, _ := json.Marshal(site)

	t.Run("internal token required", func(t *testing.T) {
		code, b := do(t, http.MethodPost, base+"/v1/allocate", body, map[string]string{"Content-Type": "application/json"})
		if code != 401 || !strings.Contains(string(b), "unauthorized") {
			t.Fatalf("%d %s", code, b)
		}
		code, _ = do(t, http.MethodPost, base+"/v1/allocate", body, map[string]string{"Content-Type": "application/json", "x-internal-token": token + "x"})
		if code != 401 {
			t.Fatalf("wrong token: %d", code)
		}
	})

	t.Run("problem json on bad input", func(t *testing.T) {
		code, b := do(t, http.MethodPost, base+"/v1/allocate", []byte(`{"siteId":"s"}`), auth)
		if code != 400 || !strings.Contains(string(b), "validation_failed") {
			t.Fatalf("%d %s", code, b)
		}
	})

	t.Run("realistic sites respect the cap and carry the trace id", func(t *testing.T) {
		traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
		hdr := map[string]string{"traceparent": "00-" + traceID + "-00f067aa0ba902b7-01"}
		for k, v := range auth {
			hdr[k] = v
		}
		for i := range 300 {
			req := sitegen.Site(r, fmt.Sprintf("site-%d", i), now)
			b, _ := json.Marshal(req)
			code, out := do(t, http.MethodPost, base+"/v1/allocate", b, hdr)
			if code != 200 {
				t.Fatalf("site %d: %d %s", i, code, out)
			}
			var res contracts.AllocateRes
			if err := json.Unmarshal(out, &res); err != nil {
				t.Fatal(err)
			}
			var sum int64
			for _, a := range res.Allocations {
				sum += a.LimitW
			}
			if len(res.Allocations) != len(req.Sessions) || sum != res.TotalAllocatedW ||
				sum+res.ReserveW > req.SiteCapW || res.ReserveW != req.IdleConnectors*req.StartW {
				t.Fatalf("site %d: bad result %+v for cap %d", i, res, req.SiteCapW)
			}
			// Deterministic: the same request answers byte for byte the same.
			if i < 20 {
				if _, again := do(t, http.MethodPost, base+"/v1/allocate", b, hdr); !bytes.Equal(again, out) {
					t.Fatalf("site %d: answer changed between identical requests", i)
				}
			}
		}
		if !strings.Contains(stdout.String(), `"trace_id":"`+traceID+`"`) {
			t.Errorf("logs do not carry the incoming trace id")
		}
	})

	t.Run("metrics", func(t *testing.T) {
		code, b := do(t, http.MethodGet, base+"/metrics", nil, nil)
		m := string(b)
		for _, want := range []string{
			`allocations_total{outcome="ok"} 320`,
			`allocations_total{outcome="invalid"} 1`,
			"invariant_violations_total 0",
			"allocation_duration_seconds_bucket",
			"allocation_sessions_bucket",
			`http_server_requests_seconds_count{method="POST",route="/v1/allocate",status="200"} 320`,
		} {
			if code != 200 || !strings.Contains(m, want) {
				t.Errorf("metrics lack %q", want)
			}
		}
	})

	t.Run("graceful shutdown on SIGTERM", func(t *testing.T) {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-exited:
			exited <- err // let cleanup drain it
			if err != nil {
				t.Fatalf("exit: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("did not exit within the grace period")
		}
		if !strings.Contains(stdout.String(), "shutdown complete") {
			t.Error("no shutdown log")
		}
	})
}
