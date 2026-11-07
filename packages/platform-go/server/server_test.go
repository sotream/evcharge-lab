package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sotream/evcharge-lab/packages/platform-go/config"
	"github.com/sotream/evcharge-lab/packages/platform-go/health"
)

func testCfg() config.Base {
	return config.Base{ServiceName: "t", LogLevel: "info", HTTPPort: 8080, InternalToken: "0123456789abcdef", ShutdownGrace: 3 * time.Second}
}

func listen(t *testing.T) (net.Listener, string) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln, "http://" + ln.Addr().String()
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func quiet() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestRunServesAndShutsDownInOrder(t *testing.T) {
	ln, base := listen(t)
	hc := health.New()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/items/{id}", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("item")) })
	mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("kaboom") })

	var mu sync.Mutex
	var order []string
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}
	closer := func(name string) func(context.Context) error {
		return func(context.Context) error { record(name); return nil }
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- Run(ctx, testCfg(), mux,
			WithListener(ln), WithHealth(hc), WithLogger(quiet()), WithDrainDelay(300*time.Millisecond),
			WithCloser("tracer", closer("tracer")),
			WithCloser("pool", closer("pool")),
			WithCloser("consumer", closer("consumer")),
			WithRunner("loop", func(ctx context.Context) error { <-ctx.Done(); record("runner-stopped"); return nil }),
		)
	}()

	waitReady(t, base)
	if code, body := get(t, base+"/v1/items/42"); code != 200 || body != "item" {
		t.Fatalf("%d %s", code, body)
	}
	if code, body := get(t, base+"/boom"); code != 500 || !strings.Contains(body, `"status":500`) {
		t.Fatalf("panic not recovered: %d %s", code, body)
	}
	if code, _ := get(t, base+"/health/live"); code != 200 {
		t.Fatal("live")
	}
	_, metricsBody := get(t, base+"/metrics")
	if !strings.Contains(metricsBody, `http_server_requests_seconds_count{method="GET",route="/v1/items/{id}",status="200"} 1`) {
		t.Fatalf("route metric missing:\n%s", grep(metricsBody, "http_server_requests"))
	}

	cancel()
	// During the drain delay the listener still answers, with ready=503.
	deadline := time.Now().Add(time.Second)
	for !hc.Draining() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if code, body := get(t, base+"/health/ready"); code != 503 || !strings.Contains(body, "draining") {
		t.Fatalf("ready during drain: %d %s", code, body)
	}

	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	want := []string{"runner-stopped", "consumer", "pool", "tracer"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order %v want %v", order, want)
	}
}

func TestRunFinishesInFlightRequests(t *testing.T) {
	ln, base := listen(t)
	started := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(400 * time.Millisecond)
		_, _ = w.Write([]byte("done"))
	})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- Run(ctx, testCfg(), mux, WithListener(ln), WithLogger(quiet()), WithDrainDelay(0)) }()
	waitReady(t, base)

	res := make(chan string, 1)
	go func() { _, b := get(t, base+"/slow"); res <- b }()
	<-started
	cancel()
	if b := <-res; b != "done" {
		t.Fatalf("in-flight request cut: %q", b)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestRunStopsOnSignal(t *testing.T) {
	ln, base := listen(t)
	errc := make(chan error, 1)
	go func() {
		errc <- Run(context.Background(), testCfg(), http.NotFoundHandler(),
			WithListener(ln), WithLogger(quiet()), WithDrainDelay(0), WithSignals(syscall.SIGUSR1))
	}()
	waitReady(t, base)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("signal did not stop Run")
	}
}

func TestRunnerFailureTriggersShutdown(t *testing.T) {
	ln, _ := listen(t)
	closed := false
	err := Run(context.Background(), testCfg(), http.NotFoundHandler(),
		WithListener(ln), WithLogger(quiet()), WithDrainDelay(0),
		WithRunner("consumer", func(context.Context) error { return errors.New("broker gone") }),
		WithCloser("x", func(context.Context) error { closed = true; return nil }),
	)
	if err == nil || !strings.Contains(err.Error(), "broker gone") || !closed {
		t.Fatalf("err=%v closed=%v", err, closed)
	}
}

func TestCloserErrorsReported(t *testing.T) {
	ln, _ := listen(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Run(ctx, testCfg(), http.NotFoundHandler(), WithListener(ln), WithLogger(quiet()), WithDrainDelay(0),
		WithCloser("pool", func(context.Context) error { return errors.New("close failed") }))
	if err == nil || !strings.Contains(err.Error(), "close pool") {
		t.Fatalf("%v", err)
	}
}

func TestDefaultTimeouts(t *testing.T) {
	ln, _ := listen(t)
	var got *http.Server
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = Run(ctx, testCfg(), http.NotFoundHandler(), WithListener(ln), WithLogger(quiet()), WithDrainDelay(0),
		WithHTTPServer(func(s *http.Server) { got = s }))
	if got.ReadHeaderTimeout != DefaultReadHeaderTimeout || got.ReadTimeout != DefaultReadTimeout ||
		got.WriteTimeout != DefaultWriteTimeout || got.IdleTimeout != DefaultIdleTimeout {
		t.Fatalf("%+v", got)
	}
}

func waitReady(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, base+"/health/ready", nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server never became ready")
}

func grep(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
