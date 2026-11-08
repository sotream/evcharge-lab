package alloc

import (
	"fmt"
	mathrand "math/rand"
	"math/rand/v2"
	"os"
	"reflect"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

// propertySites is the ADR 0006 property-test size (10^5 random sites).
// PROPERTY_SITES overrides it for longer soak runs.
func propertySites(t *testing.T) int {
	if v := os.Getenv("PROPERTY_SITES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("PROPERTY_SITES: %v", err)
		}
		return n
	}
	return 100_000
}

// randomSite draws a feasible site with 1..80 connectors from deliberately
// wide distributions: tiny and huge caps, minW 0 / 1-phase / 3-phase / DC,
// maxW = minW, priorities, startedAt ties, measured values young and old.
func randomSite(r *rand.Rand) contracts.AllocateReq {
	connectors := 1 + r.IntN(80)
	n := r.IntN(connectors + 1)
	idle := connectors - n
	if idle > 0 && r.IntN(4) == 0 {
		idle -= r.IntN(idle + 1) // some connectors faulted/unavailable: neither idle nor active
	}
	var capW int64
	switch r.IntN(4) {
	case 0:
		capW = r.Int64N(20_000) // starved site
	case 1:
		capW = r.Int64N(int64(connectors)*4140 + 1)
	default:
		capW = r.Int64N(int64(connectors)*60_000 + 1)
	}
	startW := min(int64(4140), capW/int64(connectors))
	if r.IntN(5) == 0 && startW > 0 {
		startW = r.Int64N(startW + 1)
	}
	mins := []int64{0, 1380, 2760, 4140, 5000, 6000}
	sessions := make([]contracts.AllocateSession, n)
	for i := range sessions {
		minW := mins[r.IntN(len(mins))]
		if r.IntN(10) == 0 {
			minW = r.Int64N(10_000)
		}
		maxW := minW
		if r.IntN(10) != 0 {
			maxW += r.Int64N(150_000)
		}
		s := contracts.AllocateSession{
			SessionID: fmt.Sprintf("00000000-0000-4000-8000-%012d", r.IntN(1_000_000_000)*100+i), // unique
			StationID: fmt.Sprintf("st-%d", i/2), ConnectorID: 1 + i%2,
			MinW: minW, MaxW: maxW,
			StartedAt: t0.Add(-time.Duration(r.IntN(240)) * time.Second), // many ties at 1 s resolution
		}
		if r.IntN(3) == 0 {
			s.Priority = r.IntN(11)
		}
		if r.IntN(3) != 0 {
			m := r.Int64N(maxW + 1)
			s.MeasuredW = &m
		}
		sessions[i] = s
	}
	return contracts.AllocateReq{SiteID: "s", SiteCapW: capW, IdleConnectors: int64(idle), StartW: startW, Sessions: sessions}
}

// site makes randomSite a testing/quick generator. quick hands over a
// math/rand (v1) source; it seeds a PCG so the whole run is reproducible
// from the fixed seed in quick.Config.
type site struct{ req contracts.AllocateReq }

func (site) Generate(r *mathrand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(site{randomSite(rand.New(rand.NewPCG(r.Uint64(), r.Uint64())))})
}

func TestPropertyRandomSites(t *testing.T) {
	f := func(s site) bool {
		if err := checkProperties(&s.req); err != nil {
			t.Errorf("%v\nrequest: %+v", err, s.req)
			return false
		}
		return true
	}
	cfg := &quick.Config{MaxCount: propertySites(t), Rand: mathrand.New(mathrand.NewSource(20261108))}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatal(err)
	}
}

func checkProperties(req *contracts.AllocateReq) error {
	res, err := Allocate(req, t0)
	if err != nil {
		return fmt.Errorf("allocate: %w", err)
	}
	a := res.Res
	// 1. Hard cap.
	if a.TotalAllocatedW+a.ReserveW > req.SiteCapW {
		return fmt.Errorf("cap exceeded: %d + %d > %d", a.TotalAllocatedW, a.ReserveW, req.SiteCapW)
	}
	if a.ReserveW != req.IdleConnectors*req.StartW {
		return fmt.Errorf("reserve %d", a.ReserveW)
	}
	// 2. Bounds, and admission is a prefix of the admission order.
	var sum, admittedSum int64
	admitted, unsat := 0, 0
	minUnsat := int64(-1)
	for i, d := range res.Details {
		al := a.Allocations[i]
		if al.SessionID != d.Session.SessionID {
			return fmt.Errorf("allocation %d out of order", i)
		}
		if i > 0 && Less(res.Details[i-1].Session, d.Session) >= 0 {
			return fmt.Errorf("output not in admission order at %d", i)
		}
		sum += al.LimitW
		if al.Suspended {
			if al.LimitW != req.StartW {
				return fmt.Errorf("suspended limit %d != startW %d", al.LimitW, req.StartW)
			}
			continue
		}
		if admitted != i {
			return fmt.Errorf("admitted session %d after a suspended one", i)
		}
		admitted++
		admittedSum += al.LimitW
		if al.LimitW < 0 || al.LimitW > d.Session.MaxW || al.LimitW < d.Session.MinW || al.LimitW > d.DemandW {
			return fmt.Errorf("limit %d outside [%d, %d] (maxW %d)", al.LimitW, d.Session.MinW, d.DemandW, d.Session.MaxW)
		}
		if al.LimitW < d.DemandW {
			unsat++
			if minUnsat < 0 || al.LimitW < minUnsat {
				minUnsat = al.LimitW
			}
		}
	}
	if sum != a.TotalAllocatedW {
		return fmt.Errorf("total %d != Σ %d", a.TotalAllocatedW, sum)
	}
	// 3. Admission stopped for a reason: the first suspended session did not fit.
	if admitted < len(res.Details) {
		var usedMin int64
		for _, d := range res.Details[:admitted] {
			usedMin += d.Session.MinW
		}
		rest := int64(len(res.Details)-admitted-1) * req.StartW
		budget := req.SiteCapW - a.ReserveW
		if usedMin+res.Details[admitted].Session.MinW+rest <= budget {
			return fmt.Errorf("session %d suspended although it fits", admitted)
		}
	}
	// 4. Max-min fairness: a session below another's limit is either capped by
	// its own demand, or the other is pinned at its own minW.
	if minUnsat >= 0 {
		for i, d := range res.Details[:admitted] {
			if l := d.LimitW; l > minUnsat && l != d.Session.MinW {
				return fmt.Errorf("unfair: session %d at %d above an unsatisfied session at %d", i, l, minUnsat)
			}
		}
	}
	// 5. Work conserving: with unmet demand, less than one W per unsatisfied
	// session is left (the int64 floor), so no power is left on the table.
	if unsat > 0 && res.BudgetW-admittedSum >= int64(unsat) {
		return fmt.Errorf("left %d W unallocated with %d unsatisfied sessions", res.BudgetW-admittedSum, unsat)
	}
	return nil
}
