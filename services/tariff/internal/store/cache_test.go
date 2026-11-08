package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sotream/evcharge-lab/services/tariff/internal/pricing"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func tariff(t *testing.T, id string) *pricing.Tariff {
	t.Helper()
	tr, err := pricing.NewTariff(id, []*pricing.Version{{TariffID: id, Version: 1, Currency: "EUR", Timezone: "UTC", EnergyPriceMinorPerKWh: 1}})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestCacheTTLNegativeAndStale(t *testing.T) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	var calls atomic.Int32
	var fail atomic.Bool
	c := NewCache(func(_ context.Context, id string) (*pricing.Tariff, error) {
		calls.Add(1)
		if fail.Load() {
			return nil, errors.New("db down")
		}
		if id == "missing" {
			return nil, ErrNotFound
		}
		return tariff(t, id), nil
	}, CacheConfig{TTL: time.Minute, NegativeTTL: 10 * time.Second, StaleFor: 5 * time.Minute, Now: clk.now})
	ctx := context.Background()

	for range 3 {
		if _, err := c.Get(ctx, "std"); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("calls %d, want 1 (cached)", calls.Load())
	}
	clk.add(61 * time.Second)
	if _, err := c.Get(ctx, "std"); err != nil || calls.Load() != 2 {
		t.Fatalf("expired entry not reloaded: %v calls=%d", err, calls.Load())
	}

	if _, err := c.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) || calls.Load() != 3 {
		t.Fatalf("negative entry not cached: calls=%d", calls.Load())
	}

	// Database outage: serve stale up to TTL+StaleFor, retrying at most once a second.
	fail.Store(true)
	clk.add(61 * time.Second)
	if tr, err := c.Get(ctx, "std"); err != nil || tr == nil {
		t.Fatalf("stale not served: %v", err)
	}
	before := calls.Load()
	if _, err := c.Get(ctx, "std"); err != nil || calls.Load() != before {
		t.Fatalf("stale retry not spaced: %v calls=%d->%d", err, before, calls.Load())
	}
	clk.add(5 * time.Minute)
	if _, err := c.Get(ctx, "std"); err == nil {
		t.Fatal("expected error after the stale window")
	}
	if _, err := c.Get(ctx, "fleet"); err == nil {
		t.Fatal("expected error for a never-loaded tariff")
	}
}

func TestCacheDeduplicatesConcurrentLoads(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	c := NewCache(func(_ context.Context, id string) (*pricing.Tariff, error) {
		calls.Add(1)
		<-release
		return tariff(t, id), nil
	}, CacheConfig{})
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if _, err := c.Get(context.Background(), "std"); err != nil {
				t.Error(err)
			}
		})
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("calls %d, want 1", calls.Load())
	}
}

func TestCacheWaiterHonoursContext(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	c := NewCache(func(_ context.Context, id string) (*pricing.Tariff, error) {
		<-release
		return tariff(t, id), nil
	}, CacheConfig{})
	go func() { _, _ = c.Get(context.Background(), "std") }()
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Get(ctx, "std"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}

func TestCacheWarm(t *testing.T) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	var fail atomic.Bool
	c := NewCache(func(_ context.Context, id string) (*pricing.Tariff, error) {
		if fail.Load() {
			return nil, errors.New("db down")
		}
		return tariff(t, id), nil
	}, CacheConfig{TTL: time.Minute, StaleFor: 5 * time.Minute, Now: clk.now})
	ctx := context.Background()
	if c.Warm() {
		t.Fatal("empty cache must not be warm")
	}
	if _, err := c.Get(ctx, "std"); err != nil {
		t.Fatal(err)
	}
	if !c.Warm() {
		t.Fatal("loaded cache must be warm")
	}
	fail.Store(true)
	clk.add(2 * time.Minute) // expired but inside the stale window
	if _, err := c.Get(ctx, "std"); err != nil {
		t.Fatal(err)
	}
	if !c.Warm() {
		t.Fatal("stale-servable cache must stay warm during a database outage")
	}
	clk.add(10 * time.Minute) // beyond TTL + StaleFor
	if c.Warm() {
		t.Fatal("cache past the stale window must not be warm")
	}
}
