// Command allocbench drives POST /v1/allocate with realistic sites (ADR 0000
// fleet model, 10-80 connectors) and reports allocations/s and latency
// percentiles as JSON. Two scenarios:
//
//   - saturation: closed loop, -concurrency workers back to back (max throughput);
//   - fixed-rate: open loop at -rate requests/s; latency is measured from the
//     scheduled send time, so a stalled server cannot hide queueing
//     (no coordinated omission).
//
// Every response is checked client side: Σ limitW + reserveW ≤ siteCapW.
//
//	go run ./cmd/allocbench -url http://127.0.0.1:28084 -out ../../docs/load-output/smart-charging-allocbench.json
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"

	"github.com/sotream/evcharge-lab/services/smart-charging/internal/alloc"
	"github.com/sotream/evcharge-lab/services/smart-charging/internal/sitegen"
)

type options struct {
	URL         string        `json:"url"`
	Sites       int           `json:"sites"`
	Seed        uint64        `json:"seed"`
	Concurrency int           `json:"concurrency"`
	Rate        int           `json:"ratePerSec"`
	Duration    time.Duration `json:"-"`
	Warmup      time.Duration `json:"-"`
	DurationS   float64       `json:"durationSec"`
	WarmupS     float64       `json:"warmupSec"`
	Targets     string        `json:"targetsFile"`
	out         string
	token       string
}

type latency struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

type scenario struct {
	Name                string         `json:"name"`
	Mode                string         `json:"mode"`
	Concurrency         int            `json:"concurrency,omitempty"`
	TargetRatePerSec    int            `json:"targetRatePerSec,omitempty"`
	DurationSec         float64        `json:"durationSec"`
	Requests            int            `json:"requests"`
	OK                  int            `json:"ok"`
	Errors              map[string]int `json:"errors"`
	Dropped             int            `json:"dropped,omitempty"`
	InvariantViolations int            `json:"clientInvariantViolations"`
	AllocationsPerSec   float64        `json:"allocationsPerSec"`
	SessionsPerSec      float64        `json:"sessionsPerSec"`
	LatencyMs           latency        `json:"latencyMs"`
	MeetsTarget         bool           `json:"meetsTarget"`
}

type report struct {
	Tool       string     `json:"tool"`
	StartedAt  time.Time  `json:"startedAt"`
	GoVersion  string     `json:"goVersion"`
	Platform   string     `json:"platform"`
	NumCPU     int        `json:"clientNumCPU"`
	Options    options    `json:"options"`
	SiteShape  siteShape  `json:"siteShape"`
	Target     target     `json:"target"`
	Scenarios  []scenario `json:"scenarios"`
	ServerMetr serverMetr `json:"serverMetrics"`
	Note       string     `json:"note"`
}

type siteShape struct {
	ConnectorsMin   int     `json:"connectorsMin"`
	ConnectorsMax   int     `json:"connectorsMax"`
	SessionsMean    float64 `json:"sessionsMean"`
	SessionsMax     int     `json:"sessionsMax"`
	BodyBytesMean   float64 `json:"bodyBytesMean"`
	SuspendedShare  float64 `json:"suspendedShareOfSessions"`
	CapBindingShare float64 `json:"capBindingShareOfSites"`
}

type target struct {
	AllocationsPerSec float64 `json:"allocationsPerSec"`
	P99Ms             float64 `json:"p99Ms"`
	Source            string  `json:"source"`
}

type serverMetr struct {
	InvariantViolationsTotal string `json:"invariant_violations_total"`
	AllocationsOK            string `json:"allocations_total_ok"`
}

type payload struct {
	body     []byte
	capW     int64
	sessions int
}

func main() {
	var o options
	flag.StringVar(&o.URL, "url", "http://127.0.0.1:28084", "smart-charging base URL")
	flag.StringVar(&o.token, "token", os.Getenv("INTERNAL_TOKEN"), "x-internal-token (default $INTERNAL_TOKEN)")
	flag.IntVar(&o.Sites, "sites", 2000, "distinct generated sites, sent round robin")
	flag.Uint64Var(&o.Seed, "seed", 20261108, "site generator seed")
	flag.IntVar(&o.Concurrency, "concurrency", 16, "closed-loop workers (saturation scenario)")
	flag.IntVar(&o.Rate, "rate", 1000, "open-loop requests/s (fixed-rate scenario); 0 skips it")
	flag.DurationVar(&o.Duration, "duration", 30*time.Second, "measured duration per scenario")
	flag.DurationVar(&o.Warmup, "warmup", 3*time.Second, "unmeasured warmup before each scenario")
	flag.StringVar(&o.Targets, "targets", "../../load/targets.json", "load/targets.json for pass/fail")
	flag.StringVar(&o.out, "out", "", "write the JSON report here (default stdout only)")
	flag.Parse()
	if o.token == "" {
		fmt.Fprintln(os.Stderr, "allocbench: -token or INTERNAL_TOKEN is required")
		os.Exit(2)
	}
	o.DurationS, o.WarmupS = o.Duration.Seconds(), o.Warmup.Seconds()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "allocbench:", err)
		os.Exit(1)
	}
}

func run(o options) error {
	rep := report{
		Tool: "services/smart-charging/cmd/allocbench", StartedAt: time.Now().UTC(), GoVersion: runtime.Version(),
		Platform: runtime.GOOS + "/" + runtime.GOARCH, NumCPU: runtime.NumCPU(), Options: o,
		Target: loadTarget(o.Targets),
		Note:   "client and server on the same machine; latency is client-observed end to end over HTTP/1.1 keep-alive",
	}
	sites, shape, err := buildSites(o)
	if err != nil {
		return err
	}
	rep.SiteShape = shape
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 1024, MaxIdleConnsPerHost: 1024, IdleConnTimeout: 90 * time.Second},
	}
	b := &bench{o: o, sites: sites, client: client}

	if _, err := b.warm(o.Warmup, o.Concurrency); err != nil {
		return err
	}
	sat := b.closedLoop(o.Duration, o.Concurrency)
	sat.Name = fmt.Sprintf("saturation-c%d", o.Concurrency)
	rep.Scenarios = append(rep.Scenarios, sat)
	if o.Rate > 0 {
		if _, err := b.warm(o.Warmup, o.Concurrency); err != nil {
			return err
		}
		fr := b.openLoop(o.Duration, o.Rate)
		fr.Name = fmt.Sprintf("fixed-rate-%d", o.Rate)
		rep.Scenarios = append(rep.Scenarios, fr)
	}
	for i := range rep.Scenarios {
		s := &rep.Scenarios[i]
		s.MeetsTarget = s.InvariantViolations == 0 && len(s.Errors) == 0 && s.LatencyMs.P99 < rep.Target.P99Ms &&
			s.AllocationsPerSec >= rep.Target.AllocationsPerSec*0.99 // open loop lands within 1% of the scheduled rate
	}
	rep.ServerMetr = scrape(client, o.URL)

	out, _ := json.MarshalIndent(rep, "", "  ")
	out = append(out, '\n')
	_, _ = os.Stdout.Write(out)
	if o.out != "" {
		return os.WriteFile(o.out, out, 0o644)
	}
	return nil
}

func loadTarget(path string) target {
	t := target{AllocationsPerSec: 1000, P99Ms: 20, Source: "ADR 0000 (built-in default)"}
	raw, err := os.ReadFile(path)
	if err != nil {
		return t
	}
	var f struct {
		PerInstance map[string]struct {
			AllocationsPerSec float64 `json:"allocationsPerSec"`
			P99Ms             float64 `json:"p99Ms"`
		} `json:"perInstance"`
	}
	if json.Unmarshal(raw, &f) == nil {
		if sc, ok := f.PerInstance["smart-charging"]; ok && sc.AllocationsPerSec > 0 {
			t = target{AllocationsPerSec: sc.AllocationsPerSec, P99Ms: sc.P99Ms, Source: "load/targets.json"}
		}
	}
	return t
}

// buildSites generates the request bodies once and allocates each locally to
// describe the workload (how often the cap binds, how many get suspended).
func buildSites(o options) ([]payload, siteShape, error) {
	r := rand.New(rand.NewPCG(o.Seed, o.Seed^0x9e3779b97f4a7c15))
	now := time.Now()
	out := make([]payload, o.Sites)
	sh := siteShape{ConnectorsMin: 1 << 30}
	var sessions, bytesTotal, suspended, binding int
	for i := range out {
		s := sitegen.Site(r, fmt.Sprintf("bench-site-%05d", i), now)
		body, err := json.Marshal(s)
		if err != nil {
			return nil, sh, err
		}
		c := int(s.IdleConnectors) + len(s.Sessions)
		sh.ConnectorsMin, sh.ConnectorsMax = min(sh.ConnectorsMin, c), max(sh.ConnectorsMax, c)
		sh.SessionsMax = max(sh.SessionsMax, len(s.Sessions))
		sessions += len(s.Sessions)
		bytesTotal += len(body)
		out[i] = payload{body: body, capW: s.SiteCapW, sessions: len(s.Sessions)}
		res, err := alloc.Allocate(&s, *s.ComputedAt)
		if err != nil {
			return nil, sh, fmt.Errorf("site %d: %w", i, err)
		}
		suspended += res.Suspended
		if res.LevelW >= 0 || res.Suspended > 0 {
			binding++
		}
	}
	sh.SessionsMean = float64(sessions) / float64(len(out))
	sh.BodyBytesMean = float64(bytesTotal) / float64(len(out))
	sh.SuspendedShare = float64(suspended) / float64(max(sessions, 1))
	sh.CapBindingShare = float64(binding) / float64(len(out))
	return out, sh, nil
}

type bench struct {
	o      options
	sites  []payload
	client *http.Client
	next   atomic.Uint64
}

type outcome struct {
	lat      time.Duration
	err      string
	violated bool
	sessions int
}

func (b *bench) send(ctx context.Context, p payload) outcome {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.o.URL+"/v1/allocate", bytes.NewReader(p.body))
	if err != nil {
		return outcome{err: "request"}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-internal-token", b.o.token)
	start := time.Now()
	resp, err := b.client.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "Timeout") || strings.Contains(err.Error(), "deadline") {
			return outcome{err: "timeout", lat: time.Since(start)}
		}
		return outcome{err: "transport", lat: time.Since(start)}
	}
	body, rerr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	lat := time.Since(start)
	if rerr != nil {
		return outcome{err: "read", lat: lat}
	}
	if resp.StatusCode != http.StatusOK {
		return outcome{err: fmt.Sprintf("http_%d", resp.StatusCode), lat: lat}
	}
	var res contracts.AllocateRes
	if err := json.Unmarshal(body, &res); err != nil {
		return outcome{err: "decode", lat: lat}
	}
	var sum int64
	for _, a := range res.Allocations {
		sum += a.LimitW
	}
	return outcome{lat: lat, sessions: p.sessions, violated: sum != res.TotalAllocatedW || sum+res.ReserveW > p.capW || len(res.Allocations) != p.sessions}
}

func (b *bench) pick() payload { return b.sites[int(b.next.Add(1))%len(b.sites)] }

// warm runs closed loop without recording, also priming keep-alive conns.
func (b *bench) warm(d time.Duration, workers int) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var n, bad atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for ctx.Err() == nil {
				if o := b.send(context.Background(), b.pick()); o.err != "" {
					bad.Add(1)
				}
				n.Add(1)
			}
		})
	}
	wg.Wait()
	if n.Load() > 0 && bad.Load() == n.Load() {
		return 0, fmt.Errorf("every warmup request failed (is the service up at %s and the token right?)", b.o.URL)
	}
	return int(n.Load()), nil
}

type recorder struct {
	mu     sync.Mutex
	lats   []time.Duration
	errs   map[string]int
	viol   int
	sess   int
	ok     int
	issued int
}

func (r *recorder) add(o outcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.issued++
	if o.err != "" {
		r.errs[o.err]++
	} else {
		r.ok++
		r.sess += o.sessions
	}
	if o.violated {
		r.viol++
	}
	r.lats = append(r.lats, o.lat)
}

func (r *recorder) scenario(mode string, elapsed time.Duration) scenario {
	s := scenario{Mode: mode, DurationSec: elapsed.Seconds(), Requests: r.issued, OK: r.ok, Errors: r.errs, InvariantViolations: r.viol}
	s.AllocationsPerSec = float64(r.ok) / elapsed.Seconds()
	s.SessionsPerSec = float64(r.sess) / elapsed.Seconds()
	s.LatencyMs = percentiles(r.lats)
	return s
}

func (b *bench) closedLoop(d time.Duration, workers int) scenario {
	rec := &recorder{errs: map[string]int{}, lats: make([]time.Duration, 0, 1<<18)}
	start := time.Now()
	deadline := start.Add(d)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				rec.add(b.send(context.Background(), b.pick()))
			}
		})
	}
	wg.Wait()
	s := rec.scenario("closed-loop", time.Since(start))
	s.Concurrency = workers
	return s
}

// openLoop schedules request i at start + i/rate and measures latency from
// that scheduled instant. At most maxInFlight requests run at once; a slot
// shortage counts as dropped (and the server is then clearly saturated).
func (b *bench) openLoop(d time.Duration, rate int) scenario {
	const maxInFlight = 2048
	rec := &recorder{errs: map[string]int{}, lats: make([]time.Duration, 0, rate*int(d.Seconds()+1))}
	sem := make(chan struct{}, maxInFlight)
	interval := time.Second / time.Duration(rate)
	total := int(d / interval)
	start := time.Now()
	dropped := 0
	var wg sync.WaitGroup
	for i := range total {
		at := start.Add(time.Duration(i) * interval)
		if w := time.Until(at); w > 0 {
			time.Sleep(w)
		}
		select {
		case sem <- struct{}{}:
		default:
			dropped++
			continue
		}
		p := b.pick()
		wg.Go(func() {
			defer func() { <-sem }()
			o := b.send(context.Background(), p)
			o.lat = time.Since(at) // from the scheduled time, not the actual send
			rec.add(o)
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	s := rec.scenario("open-loop", elapsed)
	s.TargetRatePerSec = rate
	s.Dropped = dropped
	return s
}

func percentiles(l []time.Duration) latency {
	if len(l) == 0 {
		return latency{}
	}
	slices.Sort(l)
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	at := func(q float64) float64 {
		i := int(q*float64(len(l)-1) + 0.5)
		return ms(l[i])
	}
	var sum time.Duration
	for _, d := range l {
		sum += d
	}
	return latency{P50: at(0.50), P90: at(0.90), P95: at(0.95), P99: at(0.99), P999: at(0.999), Max: ms(l[len(l)-1]), Mean: ms(sum / time.Duration(len(l)))}
}

func scrape(c *http.Client, base string) serverMetr {
	var m serverMetr
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/metrics", nil)
	if err != nil {
		return m
	}
	resp, err := c.Do(req)
	if err != nil {
		return m
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	for line := range strings.SplitSeq(string(body), "\n") {
		switch {
		case strings.HasPrefix(line, "invariant_violations_total "):
			m.InvariantViolationsTotal = strings.TrimPrefix(line, "invariant_violations_total ")
		case strings.HasPrefix(line, `allocations_total{outcome="ok"} `):
			m.AllocationsOK = strings.TrimPrefix(line, `allocations_total{outcome="ok"} `)
		}
	}
	return m
}
