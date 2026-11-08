package alloc

import (
	"fmt"
	"math/rand/v2"
	"testing"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"

	"github.com/sotream/evcharge-lab/services/smart-charging/internal/sitegen"
)

// BenchmarkAllocate measures the pure algorithm on full sites with n active
// sessions (every connector busy, demand above the cap so water-filling runs).
func BenchmarkAllocate(b *testing.B) {
	for _, n := range []int{1, 10, 40, 80} {
		b.Run(fmt.Sprintf("sessions=%d", n), func(b *testing.B) {
			req := contracts.AllocateReq{SiteID: "s", SiteCapW: int64(n) * 7000, StartW: 4140}
			r := rand.New(rand.NewPCG(1, 2))
			for i := range n {
				s := sess(i, []int64{1380, 4140, 6000}[r.IntN(3)], 11_000+r.Int64N(140_000))
				m := s.MaxW / 2
				s.MeasuredW = &m
				req.Sessions = append(req.Sessions, s)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Allocate(&req, t0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAllocateRealistic cycles through ADR 0000-shaped sites (10-80 connectors).
func BenchmarkAllocateRealistic(b *testing.B) {
	r := rand.New(rand.NewPCG(3, 4))
	sites := make([]contracts.AllocateReq, 256)
	for i := range sites {
		sites[i] = sitegen.Site(r, fmt.Sprintf("site-%d", i), t0)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, err := Allocate(&sites[i%len(sites)], t0); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func TestSitegenSitesAreValidAndFeasible(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	for i := range 2000 {
		req := sitegen.Site(r, fmt.Sprintf("site-%d", i), t0)
		if p := Validate(&req); len(p) > 0 {
			t.Fatalf("invalid generated site: %v", p)
		}
		if c := req.IdleConnectors + int64(len(req.Sessions)); c < 10 || c > 80 {
			t.Fatalf("connectors %d outside 10..80", c)
		}
		if err := checkProperties(&req); err != nil {
			t.Fatal(err)
		}
	}
}
