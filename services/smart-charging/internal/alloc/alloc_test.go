package alloc

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

var t0 = time.Date(2026, 11, 8, 18, 0, 0, 0, time.UTC)

// sess builds a session whose id sorts by n and that started n minutes before t0.
func sess(n int, minW, maxW int64) contracts.AllocateSession {
	return contracts.AllocateSession{
		SessionID: fmt.Sprintf("00000000-0000-4000-8000-%012d", n), StationID: fmt.Sprintf("st-%d", n), ConnectorID: 1,
		MinW: minW, MaxW: maxW, StartedAt: t0.Add(-time.Duration(100-n) * time.Minute),
	}
}

func withPrio(s contracts.AllocateSession, p int) contracts.AllocateSession { s.Priority = p; return s }

func withMeasured(s contracts.AllocateSession, w int64, age time.Duration) contracts.AllocateSession {
	s.MeasuredW, s.StartedAt = &w, t0.Add(-age)
	return s
}

type want struct {
	id        int
	limitW    int64
	suspended bool
}

func TestAllocateTable(t *testing.T) {
	ac := func(n int) contracts.AllocateSession { return sess(n, 4140, 22_000) }
	dc := func(n int) contracts.AllocateSession { return sess(n, 6000, 50_000) }
	eighty := make([]contracts.AllocateSession, 40)
	eightyWant := make([]want, 40)
	for i := range eighty {
		eighty[i] = ac(i + 1)
		eightyWant[i] = want{i + 1, 5860, false}
	}
	cases := []struct {
		name     string
		req      contracts.AllocateReq
		reserveW int64
		want     []want
	}{
		{
			name:     "all idle: only the start reserve",
			req:      contracts.AllocateReq{SiteCapW: 44_000, IdleConnectors: 10, StartW: 4140},
			reserveW: 41_400, want: []want{},
		},
		{
			name:     "single session under budget gets its demand",
			req:      contracts.AllocateReq{SiteCapW: 22_000, IdleConnectors: 1, StartW: 4140, Sessions: []contracts.AllocateSession{sess(1, 4140, 11_000)}},
			reserveW: 4140, want: []want{{1, 11_000, false}},
		},
		{
			name:     "single session capped by budget",
			req:      contracts.AllocateReq{SiteCapW: 11_000, IdleConnectors: 1, StartW: 4140, Sessions: []contracts.AllocateSession{sess(1, 4140, 22_000)}},
			reserveW: 4140, want: []want{{1, 6860, false}},
		},
		{
			name: "demand below the fair share is met, excess redistributed",
			req: contracts.AllocateReq{SiteCapW: 30_000, StartW: 4140, Sessions: []contracts.AllocateSession{
				sess(1, 1380, 3680), ac(2), ac(3)}},
			want: []want{{1, 3680, false}, {2, 13_160, false}, {3, 13_160, false}},
		},
		{
			name: "cap below Σ minW: admit oldest first, suspend the rest at startW",
			req: contracts.AllocateReq{SiteCapW: 30_000, IdleConnectors: 2, StartW: 4140, Sessions: []contracts.AllocateSession{
				dc(4), dc(2), dc(1), dc(3)}},
			reserveW: 8280,
			want:     []want{{1, 6720, false}, {2, 6720, false}, {3, 4140, true}, {4, 4140, true}},
		},
		{
			name: "priority beats age in admission",
			req: contracts.AllocateReq{SiteCapW: 30_000, IdleConnectors: 2, StartW: 4140, Sessions: []contracts.AllocateSession{
				dc(1), dc(2), dc(3), withPrio(dc(4), 5)}},
			reserveW: 8280,
			want:     []want{{4, 6720, false}, {1, 6720, false}, {2, 4140, true}, {3, 4140, true}},
		},
		{
			name: "uniform minimums above cap/n: everyone held at startW (equal share)",
			req: contracts.AllocateReq{SiteCapW: 10_000, StartW: 3333, Sessions: []contracts.AllocateSession{
				sess(1, 4140, 11_000), sess(2, 4140, 11_000), sess(3, 4140, 11_000)}},
			want: []want{{1, 3333, true}, {2, 3333, true}, {3, 3333, true}},
		},
		{
			name:     "80 connectors, 40 charging: level = budget / 40",
			req:      contracts.AllocateReq{SiteCapW: 400_000, IdleConnectors: 40, StartW: 4140, Sessions: eighty},
			reserveW: 165_600, want: eightyWant,
		},
		{
			name: "measured demand after 60 s frees headroom for others",
			req: contracts.AllocateReq{SiteCapW: 20_000, StartW: 4140, Sessions: []contracts.AllocateSession{
				withMeasured(ac(1), 5000, 2*time.Minute),  // demand 5000 + 500 + 500
				withMeasured(ac(2), 1000, 2*time.Minute),  // max(minW, 1600) = 4140
				withMeasured(ac(3), 5000, 30*time.Second), // too young: demand = maxW
			}},
			want: []want{{1, 6000, false}, {2, 4140, false}, {3, 9860, false}},
		},
		{
			name: "floor rounding leaves < n W unallocated, never over",
			req: contracts.AllocateReq{SiteCapW: 10_000, Sessions: []contracts.AllocateSession{
				sess(1, 0, 11_000), sess(2, 0, 11_000), sess(3, 0, 11_000)}},
			want: []want{{1, 3333, false}, {2, 3333, false}, {3, 3333, false}},
		},
		{
			name: "mixed minimums: a session pinned at its minW stays above the level",
			req: contracts.AllocateReq{SiteCapW: 10_000, Sessions: []contracts.AllocateSession{
				sess(1, 1380, 11_000), sess(2, 6000, 11_000)}},
			want: []want{{1, 4000, false}, {2, 6000, false}},
		},
		{
			name: "zero cap, zero startW",
			req: contracts.AllocateReq{SiteCapW: 0, Sessions: []contracts.AllocateSession{
				sess(1, 0, 11_000), sess(2, 4140, 11_000)}},
			want: []want{{1, 0, false}, {2, 0, true}},
		},
		{
			name: "ties on startedAt break by sessionId",
			req: contracts.AllocateReq{SiteCapW: 30_000, IdleConnectors: 2, StartW: 4140, Sessions: func() []contracts.AllocateSession {
				s := []contracts.AllocateSession{dc(4), dc(3), dc(2), dc(1)}
				for i := range s {
					s[i].StartedAt = t0
				}
				return s
			}()},
			reserveW: 8280,
			want:     []want{{1, 6720, false}, {2, 6720, false}, {3, 4140, true}, {4, 4140, true}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.req.SiteID = "site-1"
			r, err := Allocate(&tc.req, t0)
			if err != nil {
				t.Fatal(err)
			}
			if r.Res.ReserveW != tc.reserveW {
				t.Errorf("reserveW = %d, want %d", r.Res.ReserveW, tc.reserveW)
			}
			if r.Res.Allocations == nil {
				t.Fatal("allocations must be [] not null")
			}
			got := make([]want, len(r.Res.Allocations))
			var total int64
			for i, a := range r.Res.Allocations {
				var id int
				_, _ = fmt.Sscanf(a.SessionID[24:], "%d", &id)
				got[i] = want{id, a.LimitW, a.Suspended}
				total += a.LimitW
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("allocations\n got %v\nwant %v", got, tc.want)
			}
			if r.Res.TotalAllocatedW != total || total+r.Res.ReserveW > tc.req.SiteCapW {
				t.Errorf("total %d reserve %d cap %d", r.Res.TotalAllocatedW, r.Res.ReserveW, tc.req.SiteCapW)
			}
			if r.Res.Strategy != "optimal" || r.Res.SiteID != "site-1" {
				t.Errorf("header %+v", r.Res)
			}
		})
	}
}

func TestInfeasibleReserve(t *testing.T) {
	for _, req := range []contracts.AllocateReq{
		{SiteCapW: 10_000, IdleConnectors: 3, StartW: 4140},
		{SiteCapW: 10_000, IdleConnectors: 1, StartW: 4140, Sessions: []contracts.AllocateSession{sess(1, 0, 1), sess(2, 0, 1)}},
	} {
		if _, err := Allocate(&req, t0); !errors.Is(err, ErrInfeasible) {
			t.Errorf("err = %v, want ErrInfeasible", err)
		}
	}
}

func TestEffectiveDemand(t *testing.T) {
	m := func(w int64) *int64 { return &w }
	base := contracts.AllocateSession{MinW: 4140, MaxW: 22_000, StartedAt: t0.Add(-time.Hour)}
	for _, tc := range []struct {
		measured *int64
		age      time.Duration
		want     int64
	}{
		{nil, time.Hour, 22_000},
		{m(10_000), 59 * time.Second, 22_000},
		{m(10_000), 60 * time.Second, 11_500},
		{m(10_009), time.Hour, 11_509}, // floor(1.1 × 10009) = 11009
		{m(0), time.Hour, 4140},
		{m(30_000), time.Hour, 22_000},
	} {
		s := base
		s.MeasuredW, s.StartedAt = tc.measured, t0.Add(-tc.age)
		if got := EffectiveDemand(&s, t0); got != tc.want {
			t.Errorf("measured %v age %s: demand %d, want %d", tc.measured, tc.age, got, tc.want)
		}
	}
}

func TestCheckRejectsTamperedResults(t *testing.T) {
	req := contracts.AllocateReq{SiteID: "s", SiteCapW: 20_000, IdleConnectors: 1, StartW: 4140,
		Sessions: []contracts.AllocateSession{sess(1, 4140, 22_000), sess(2, 4140, 22_000)}}
	tamper := map[string]func(r *Result){
		"over cap":        func(r *Result) { r.Res.Allocations[0].LimitW++; r.Details[0].LimitW++; r.Res.TotalAllocatedW++ },
		"wrong total":     func(r *Result) { r.Res.TotalAllocatedW-- },
		"wrong reserve":   func(r *Result) { r.Res.ReserveW = 0 },
		"below minW":      func(r *Result) { r.Res.Allocations[0].LimitW = 1; r.Details[0].LimitW = 1 },
		"missing session": func(r *Result) { r.Res.Allocations = r.Res.Allocations[:1] },
		"suspended flag":  func(r *Result) { r.Res.Allocations[1].Suspended = true },
		"above maxW": func(r *Result) {
			r.Details[1].DemandW = 30_000
			r.Details[1].LimitW = 25_000
			r.Res.Allocations[1].LimitW = 25_000
		},
		"suspended≠startW": func(r *Result) { r.Details[1].Admitted = false; r.Res.Allocations[1].Suspended = true },
	}
	for name, fn := range tamper {
		r, err := Allocate(&req, t0)
		if err != nil {
			t.Fatal(err)
		}
		fn(&r)
		if err := Check(&req, &r); !errors.Is(err, ErrInvariant) {
			t.Errorf("%s: Check = %v, want ErrInvariant", name, err)
		}
	}
}

func TestOutputIndependentOfInputOrder(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 7))
	for range 200 {
		req := randomSite(r)
		a, err := Allocate(&req, t0)
		if err != nil {
			t.Fatal(err)
		}
		r.Shuffle(len(req.Sessions), func(i, j int) { req.Sessions[i], req.Sessions[j] = req.Sessions[j], req.Sessions[i] })
		b, err := Allocate(&req, t0)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(a.Res.Allocations, b.Res.Allocations) || a.Res.TotalAllocatedW != b.Res.TotalAllocatedW {
			t.Fatalf("output depends on input order:\n%v\n%v", a.Res.Allocations, b.Res.Allocations)
		}
	}
}

func TestValidate(t *testing.T) {
	ok := contracts.AllocateReq{SiteID: "s", SiteCapW: 10_000, StartW: 1000, Sessions: []contracts.AllocateSession{sess(1, 4140, 11_000)}}
	if p := Validate(&ok); len(p) != 0 {
		t.Fatalf("valid request rejected: %v", p)
	}
	neg := int64(-1)
	for name, mut := range map[string]func(r *contracts.AllocateReq){
		"empty siteId":   func(r *contracts.AllocateReq) { r.SiteID = "" },
		"negative cap":   func(r *contracts.AllocateReq) { r.SiteCapW = -1 },
		"huge cap":       func(r *contracts.AllocateReq) { r.SiteCapW = MaxSiteCapW + 1 },
		"negative idle":  func(r *contracts.AllocateReq) { r.IdleConnectors = -1 },
		"negative start": func(r *contracts.AllocateReq) { r.StartW = -1 },
		"bad uuid":       func(r *contracts.AllocateReq) { r.Sessions[0].SessionID = "not-a-uuid" },
		"dup session":    func(r *contracts.AllocateReq) { r.Sessions = append(r.Sessions, r.Sessions[0]) },
		"min > max":      func(r *contracts.AllocateReq) { r.Sessions[0].MinW = 20_000 },
		"neg measured":   func(r *contracts.AllocateReq) { r.Sessions[0].MeasuredW = &neg },
		"priority 11":    func(r *contracts.AllocateReq) { r.Sessions[0].Priority = 11 },
		"no startedAt":   func(r *contracts.AllocateReq) { r.Sessions[0].StartedAt = time.Time{} },
		"empty station":  func(r *contracts.AllocateReq) { r.Sessions[0].StationID = "" },
		"too many":       func(r *contracts.AllocateReq) { r.Sessions = make([]contracts.AllocateSession, MaxSessions+1) },
	} {
		req := ok
		req.Sessions = slices.Clone(ok.Sessions)
		mut(&req)
		if p := Validate(&req); len(p) == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
}
