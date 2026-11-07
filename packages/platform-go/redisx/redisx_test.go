package redisx

import (
	"context"
	"testing"
	"time"

	"github.com/sotream/evcharge-lab/packages/platform-go/internal/testenv"
)

func TestOptionsDefaults(t *testing.T) {
	o, err := Options(Config{URL: "redis://:pw@cache:6380/2"})
	if err != nil {
		t.Fatal(err)
	}
	if o.Addr != "cache:6380" || o.DB != 2 || o.Password != "pw" ||
		o.DialTimeout != 2*time.Second || o.ReadTimeout != time.Second || o.WriteTimeout != time.Second ||
		o.PoolTimeout != 2*time.Second || !o.ContextTimeoutEnabled {
		t.Fatalf("%+v", o)
	}
	o, _ = Options(Config{URL: "redis://x:1", ReadTimeout: 5 * time.Second, PoolSize: 7})
	if o.ReadTimeout != 5*time.Second || o.PoolSize != 7 {
		t.Fatalf("%+v", o)
	}
	if _, err := Options(Config{URL: "http://nope"}); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestIntegrationConnect(t *testing.T) {
	testenv.Require(t)
	ctx := context.Background()
	c, err := Connect(ctx, Config{URL: testenv.RedisURL()})
	if err != nil {
		t.Fatal(err)
	}
	if err := ReadinessCheck(c)(ctx); err != nil {
		t.Fatal(err)
	}
	key := testenv.Unique("redisx-test-")
	if err := c.Set(ctx, key, "v", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	defer c.Del(ctx, key)
	if v, err := c.Get(ctx, key).Result(); err != nil || v != "v" {
		t.Fatalf("%q %v", v, err)
	}
	if err := Closer(c)(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ReadinessCheck(c)(ctx); err == nil {
		t.Fatal("closed client should fail readiness")
	}
}

func TestIntegrationConnectFailsFast(t *testing.T) {
	testenv.Require(t)
	start := time.Now()
	if _, err := Connect(context.Background(), Config{URL: "redis://127.0.0.1:1", DialTimeout: 200 * time.Millisecond}); err == nil {
		t.Fatal("expected error")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("slow failure")
	}
}
