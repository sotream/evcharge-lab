// Command quotebench measures tariff quote throughput and latency (ADR 0000
// lab target: >= 2,000 calculations/s, p99 < 20 ms per instance).
//
// Modes:
//
//	http  closed-loop POST /v1/quote against a running service (-url, -token)
//	calc  the pure calculation in-process, tariffs loaded from the database (-dsn)
//
// The result is written as JSON to -out (and stdout).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
	"github.com/sotream/evcharge-lab/packages/platform-go/pg"
	"github.com/sotream/evcharge-lab/services/tariff/internal/pricing"
	"github.com/sotream/evcharge-lab/services/tariff/internal/store"
)

type opts struct {
	url, token, dsn, out, modes string
	concurrency                 int
	rate                        int
	serverMode, serverCPU       string
	serverConc                  string
	duration, warmup            time.Duration
	seed                        uint64
}

type result struct {
	Mode        string         `json:"mode"`
	Label       string         `json:"label"`
	Concurrency int            `json:"concurrency,omitempty"`
	TargetRate  int            `json:"offeredRatePerSecond,omitempty"`
	DurationS   float64        `json:"durationS"`
	Requests    int            `json:"requests"`
	Errors      map[string]int `json:"errors"`
	PerSecond   float64        `json:"quotesPerSecond"`
	LatencyMs   map[string]any `json:"latencyMs"`
	Target      map[string]any `json:"target"`
	Pass        bool           `json:"pass"`
}

type report struct {
	Tool      string         `json:"tool"`
	StartedAt time.Time      `json:"startedAt"`
	Host      map[string]any `json:"host"`
	Config    map[string]any `json:"config"`
	Server    map[string]any `json:"server,omitempty"`
	Results   []result       `json:"results"`
}

func main() {
	var o opts
	flag.StringVar(&o.url, "url", "http://localhost:28085", "tariff base URL (http mode)")
	flag.StringVar(&o.token, "token", os.Getenv("INTERNAL_TOKEN"), "x-internal-token (http mode; default $INTERNAL_TOKEN)")
	flag.StringVar(&o.dsn, "dsn", "", "tariff database URL (calc mode)")
	flag.StringVar(&o.modes, "modes", "http", "comma list of modes: http,calc")
	flag.StringVar(&o.out, "out", "", "write the JSON report to this file")
	flag.IntVar(&o.concurrency, "c", 32, "concurrent workers (closed loop)")
	flag.IntVar(&o.rate, "rate", 2000, "offered quotes/s (openloop mode)")
	flag.StringVar(&o.serverMode, "server-mode", "unspecified", "how the server under test runs: native or container (recorded in the JSON)")
	flag.StringVar(&o.serverCPU, "server-cpu-limit", "unspecified", "CPU limit of the server under test, e.g. 2 (docker --cpus) or none (recorded)")
	flag.StringVar(&o.serverConc, "server-concurrency", "unspecified", "server concurrency settings, e.g. GOMAXPROCS and DB_MAX_CONNS (recorded)")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "measured duration per mode")
	flag.DurationVar(&o.warmup, "warmup", 3*time.Second, "unmeasured warm-up per mode")
	flag.Uint64Var(&o.seed, "seed", 1, "request generator seed")
	flag.Parse()
	if err := runBench(o); err != nil {
		fmt.Fprintln(os.Stderr, "quotebench:", err)
		os.Exit(1)
	}
}

func runBench(o opts) error {
	rep := report{Tool: "services/tariff/cmd/quotebench", StartedAt: time.Now().UTC(),
		Host: map[string]any{"goos": runtime.GOOS, "goarch": runtime.GOARCH, "numCPU": runtime.NumCPU(), "go": runtime.Version()},
		Config: map[string]any{"url": o.url, "concurrency": o.concurrency, "durationS": o.duration.Seconds(),
			"warmupS": o.warmup.Seconds(), "seed": o.seed, "tariffs": tariffIDs,
			"sessions": "start uniform over 2025-2026, duration 5-240 min (mean ~45), energy from 3-60 kW mean power"}}
	rep.Server = map[string]any{"mode": o.serverMode, "cpuLimit": o.serverCPU, "concurrency": o.serverConc,
		"loadGeneratorHost": "same machine as the server; the load generator shares host CPU"}
	for _, m := range strings.Split(o.modes, ",") {
		var r result
		var err error
		switch strings.TrimSpace(m) {
		case "http":
			r, err = benchHTTP(o)
		case "openloop":
			r, err = benchOpenLoop(o)
		case "calc":
			r, err = benchCalc(o)
		default:
			err = fmt.Errorf("unknown mode %q", m)
		}
		if err != nil {
			return err
		}
		rep.Results = append(rep.Results, r)
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if o.out != "" {
		return os.WriteFile(o.out, append(b, '\n'), 0o644)
	}
	return nil
}

var tariffIDs = []string{"std", "tou", "fleet"}

// genReq draws a plausible session (ADR 0000: 45 min average).
func genReq(rng *rand.Rand) contracts.QuoteReq {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(rng.Int64N(int64(2 * 365 * 24 * time.Hour)))).Truncate(time.Second)
	mins := math.Min(240, math.Max(5, rng.ExpFloat64()*40+5))
	dur := time.Duration(mins * float64(time.Minute)).Truncate(time.Second)
	kw := 3 + rng.Float64()*57
	wh := int64(kw * dur.Hours() * 1000)
	return contracts.QuoteReq{TariffID: tariffIDs[rng.IntN(len(tariffIDs))], StartedAt: start, StoppedAt: start.Add(dur), EnergyWh: &wh}
}

type worker func(ctx context.Context, rng *rand.Rand) (errKey string)

// closedLoop runs c workers for warmup+duration and collects latencies of
// the measured part.
func closedLoop(o opts, fn worker) (lat []time.Duration, errs map[string]int, elapsed time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), o.warmup+o.duration)
	defer cancel()
	measureFrom := time.Now().Add(o.warmup)
	var mu sync.Mutex
	errs = map[string]int{}
	var wg sync.WaitGroup
	for w := range o.concurrency {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(o.seed, uint64(w)))
			local := make([]time.Duration, 0, 1<<16)
			localErr := map[string]int{}
			for ctx.Err() == nil {
				t0 := time.Now()
				key := fn(ctx, rng)
				d := time.Since(t0)
				if t0.Before(measureFrom) || ctx.Err() != nil {
					continue
				}
				if key != "" {
					localErr[key]++
					continue
				}
				local = append(local, d)
			}
			mu.Lock()
			lat = append(lat, local...)
			for k, v := range localErr {
				errs[k] += v
			}
			mu.Unlock()
		})
	}
	wg.Wait()
	return lat, errs, o.duration
}

func summarize(mode, label string, o opts, lat []time.Duration, errs map[string]int, elapsed time.Duration) result {
	slices.Sort(lat)
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		i := int(math.Ceil(p*float64(len(lat)))) - 1
		return float64(lat[max(0, i)].Microseconds()) / 1000
	}
	r := result{Mode: mode, Label: label, Concurrency: o.concurrency, DurationS: elapsed.Seconds(), Requests: len(lat), Errors: errs,
		PerSecond: math.Round(float64(len(lat))/elapsed.Seconds()*10) / 10,
		LatencyMs: map[string]any{"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99), "max": pct(1)},
		Target:    map[string]any{"quotesPerSecond": 2000, "p99Ms": 20, "source": "docs/adr/0000-requirements-and-capacity.md"}}
	total := 0
	for _, v := range errs {
		total += v
	}
	r.Pass = r.PerSecond >= 2000 && pct(0.99) < 20 && total == 0
	return r
}

func benchHTTP(o opts) (result, error) {
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{
		MaxIdleConns: o.concurrency * 2, MaxIdleConnsPerHost: o.concurrency * 2, IdleConnTimeout: 90 * time.Second}}
	u := strings.TrimRight(o.url, "/") + "/v1/quote"
	// Fail fast if the service is not reachable or the token is wrong.
	if key := postQuote(context.Background(), client, u, o.token, genReq(rand.New(rand.NewPCG(0, 0)))); key != "" {
		return result{}, fmt.Errorf("preflight POST %s: %s", u, key)
	}
	lat, errs, el := closedLoop(o, func(ctx context.Context, rng *rand.Rand) string {
		return postQuote(ctx, client, u, o.token, genReq(rng))
	})
	return summarize("http", "[measured] POST /v1/quote over HTTP keep-alive, closed loop, one service instance", o, lat, errs, el), nil
}

func postQuote(ctx context.Context, c *http.Client, u, token string, q contracts.QuoteReq) string {
	body, _ := json.Marshal(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return "request"
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-internal-token", token)
	resp, err := c.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ""
		}
		return "transport"
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Sprintf("http_%d", resp.StatusCode)
	}
	var res contracts.QuoteRes
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil || len(res.Lines) == 0 {
		return "decode"
	}
	return ""
}

func benchCalc(o opts) (result, error) {
	if o.dsn == "" {
		return result{}, fmt.Errorf("calc mode needs -dsn")
	}
	ctx := context.Background()
	pool, err := pg.Connect(ctx, pg.Config{URL: o.dsn, ApplicationName: "quotebench"})
	if err != nil {
		return result{}, err
	}
	defer pool.Close()
	tariffs := map[string]*pricing.Tariff{}
	for _, id := range tariffIDs {
		t, err := store.PG{Pool: pool}.Load(ctx, id)
		if err != nil {
			return result{}, err
		}
		tariffs[id] = t
	}
	lat, errs, el := closedLoop(o, func(_ context.Context, rng *rand.Rand) string {
		q := genReq(rng)
		v, err := tariffs[q.TariffID].At(q.StartedAt)
		if err != nil {
			return "no_version"
		}
		if _, err := pricing.Quote(v, pricing.Request{StartedAt: q.StartedAt, StoppedAt: q.StoppedAt, EnergyWh: *q.EnergyWh}); err != nil {
			return "quote"
		}
		return ""
	})
	return summarize("calc", "[measured] pricing.Quote in-process (CPU only, no HTTP/JSON)", o, lat, errs, el), nil
}

// benchOpenLoop offers o.rate quotes/s on a fixed schedule. Latency runs from the scheduled send time
// to completion, so queueing behind a slow server counts (no coordinated omission).
func benchOpenLoop(o opts) (result, error) {
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		MaxIdleConns: 1024, MaxIdleConnsPerHost: 1024, IdleConnTimeout: 90 * time.Second}}
	u := strings.TrimRight(o.url, "/") + "/v1/quote"
	if key := postQuote(context.Background(), client, u, o.token, genReq(rand.New(rand.NewPCG(0, 0)))); key != "" {
		return result{}, fmt.Errorf("preflight POST %s: %s", u, key)
	}
	interval := time.Second / time.Duration(o.rate)
	total := int((o.warmup + o.duration) / interval)
	warmN := int(o.warmup / interval)
	var mu sync.Mutex
	var lat []time.Duration
	errs := map[string]int{}
	var wg sync.WaitGroup
	rng := rand.New(rand.NewPCG(o.seed, 1))
	t0 := time.Now()
	for i := range total {
		sched := t0.Add(time.Duration(i) * interval)
		if d := time.Until(sched); d > 0 {
			time.Sleep(d)
		}
		q := genReq(rng)
		wg.Go(func() {
			key := postQuote(context.Background(), client, u, o.token, q)
			d := time.Since(sched)
			if i < warmN {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if key != "" {
				errs[key]++
				return
			}
			lat = append(lat, d)
		})
	}
	wg.Wait()
	r := summarize("openloop", fmt.Sprintf("[measured] POST /v1/quote over HTTP, open loop at a fixed %d quotes/s, latency from scheduled send time", o.rate), o, lat, errs, o.duration)
	r.Concurrency = 0
	r.TargetRate = o.rate
	r.Pass = r.Pass && r.PerSecond >= 0.99*float64(o.rate)
	return r, nil
}
