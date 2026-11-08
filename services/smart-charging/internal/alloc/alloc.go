// Package alloc is the pure site power allocator of ADR 0006: start reserve
// for idle connectors, effective demand from measured power, admission by
// (priority desc, startedAt asc), max-min fair water-filling between each
// admitted session's minW and its demand, int64 W rounded down, and the hard
// invariant Σ limitW + reserveW ≤ siteCapW checked on every result.
//
// The package has no state, no clock and no I/O: Allocate is a function of
// its arguments, and its output order is fully determined (independent of the
// order of the input sessions).
package alloc

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

// Strategy is the AllocateRes.strategy literal.
const Strategy = "optimal"

const (
	// MeasuredAfter is how long a session must have run before its measured
	// draw replaces maxW as its demand (ADR 0006 step 3).
	MeasuredAfter = 60 * time.Second
	// HeadroomW is the fixed headroom added to 1.1 × measuredW.
	HeadroomW = 500
)

// ErrInvariant means a computed result would break Σ limitW + reserveW ≤
// siteCapW (or a per-session bound). Callers must not send such a result.
var ErrInvariant = errors.New("alloc: invariant violated")

// ErrInfeasible means the request cannot be answered safely: even holding
// every idle connector and every active session at startW exceeds the cap.
// ADR 0006 rules this out by construction (startW ≤ cap / connectorsAtSite),
// so it signals an inconsistent caller.
var ErrInfeasible = errors.New("alloc: (idleConnectors + sessions) × startW exceeds siteCapW")

// EffectiveDemand implements ADR 0006 step 3: maxW, or once the session has
// run for MeasuredAfter and a measurement exists,
// min(maxW, max(minW, floor(1.1 × measuredW) + 500)).
func EffectiveDemand(s *contracts.AllocateSession, now time.Time) int64 {
	if s.MeasuredW == nil || now.Sub(s.StartedAt) < MeasuredAfter {
		return s.MaxW
	}
	m := *s.MeasuredW
	want := m + m/10 + HeadroomW // floor(1.1 m) for m ≥ 0
	return min(s.MaxW, max(s.MinW, want))
}

// Less is the admission order: priority desc, startedAt asc, then sessionId,
// stationId and connectorId asc so that ties never depend on input order.
func Less(a, b *contracts.AllocateSession) int {
	if c := cmp.Compare(b.Priority, a.Priority); c != 0 {
		return c
	}
	if c := a.StartedAt.Compare(b.StartedAt); c != 0 {
		return c
	}
	if c := strings.Compare(a.SessionID, b.SessionID); c != 0 {
		return c
	}
	if c := strings.Compare(a.StationID, b.StationID); c != 0 {
		return c
	}
	return cmp.Compare(a.ConnectorID, b.ConnectorID)
}

// Detail is the per-session working state, exposed for tests and metrics.
type Detail struct {
	Session  *contracts.AllocateSession
	DemandW  int64
	LimitW   int64
	Admitted bool
}

// Result is the full outcome of one allocation round.
type Result struct {
	Res contracts.AllocateRes
	// Details follows Res.Allocations index by index (admission order).
	Details []Detail
	// BudgetW is the power the admitted sessions share:
	// cap − reserve − startW × suspended.
	BudgetW int64
	// LevelW is the water level when demand exceeds BudgetW, else -1.
	LevelW    int64
	Suspended int
}

// Allocate computes the limits for one site at reference time now. It never
// returns a result that violates the invariant: such a result yields an error
// wrapping ErrInvariant instead.
func Allocate(req *contracts.AllocateReq, now time.Time) (Result, error) {
	n := len(req.Sessions)
	reserve := req.IdleConnectors * req.StartW
	budget := req.SiteCapW - reserve // ADR 0006 step 2
	if budget < 0 || int64(n)*req.StartW > budget {
		return Result{}, ErrInfeasible
	}

	order := make([]Detail, n)
	for i := range req.Sessions {
		s := &req.Sessions[i]
		order[i] = Detail{Session: s, DemandW: EffectiveDemand(s, now)}
	}
	slices.SortFunc(order, func(a, b Detail) int { return Less(a.Session, b.Session) })

	// Step 4, admission. Every session that is not admitted is held at startW,
	// so the admission test keeps startW for all sessions still behind it:
	// Σ minW(admitted) + startW × (not admitted) ≤ budget. Admission stops at
	// the first session that does not fit, so priority order is strict.
	var usedMin int64
	k := 0
	for ; k < n; k++ {
		rest := int64(n-k-1) * req.StartW
		if usedMin+order[k].Session.MinW+rest > budget {
			break
		}
		usedMin += order[k].Session.MinW
		order[k].Admitted = true
	}
	suspended := n - k
	share := budget - int64(suspended)*req.StartW

	// Step 5, water-filling over the admitted prefix.
	level := waterLevel(order[:k], share)
	var total int64
	for i := range order {
		d := &order[i]
		if d.Admitted {
			if level < 0 {
				d.LimitW = d.DemandW
			} else {
				d.LimitW = min(max(level, d.Session.MinW), d.DemandW)
			}
		} else {
			d.LimitW = req.StartW
		}
		total += d.LimitW
	}

	allocs := make([]contracts.Allocation, n)
	for i, d := range order {
		allocs[i] = contracts.Allocation{SessionID: d.Session.SessionID, LimitW: d.LimitW, Suspended: !d.Admitted}
	}
	r := Result{
		Res: contracts.AllocateRes{
			SiteID: req.SiteID, Strategy: Strategy, TotalAllocatedW: total, ReserveW: reserve, Allocations: allocs,
		},
		Details: order, BudgetW: share, LevelW: level, Suspended: suspended,
	}
	if err := Check(req, &r); err != nil {
		return Result{}, err
	}
	return r, nil
}

// waterLevel returns the largest integer level L with
// Σ clamp(L, minW, demand) ≤ budget over ds, or -1 when every session's
// demand fits (no level binds). It needs Σ minW ≤ budget. O(n log n): sort the
// 2n breakpoints and sweep the piecewise-linear sum.
func waterLevel(ds []Detail, budget int64) int64 {
	var sumMin, sumDemand int64
	type event struct{ at, slope int64 }
	ev := make([]event, 0, 2*len(ds))
	for _, d := range ds {
		lo, hi := d.Session.MinW, d.DemandW
		sumMin += lo
		sumDemand += hi
		if lo < hi {
			ev = append(ev, event{lo, 1}, event{hi, -1})
		}
	}
	if sumDemand <= budget {
		return -1
	}
	slices.SortFunc(ev, func(a, b event) int { return cmp.Compare(a.at, b.at) })
	f, at, slope := sumMin, int64(0), int64(0)
	for _, e := range ev {
		if e.at > at {
			next := f + slope*(e.at-at)
			if next > budget { // slope > 0 here, since f ≤ budget
				return at + (budget-f)/slope
			}
			f, at = next, e.at
		}
		slope += e.slope
	}
	return at // unreachable: f reaches sumDemand > budget inside the sweep
}

// Check asserts the safety and shape invariants of r against req:
// Σ limitW + reserveW ≤ siteCapW, reserveW = idleConnectors × startW,
// totalAllocatedW = Σ limitW, one allocation per session, admitted limits in
// [minW, demand] ⊆ [0, maxW], suspended limits = startW.
func Check(req *contracts.AllocateReq, r *Result) error {
	res := &r.Res
	if res.ReserveW != req.IdleConnectors*req.StartW {
		return fmt.Errorf("%w: reserveW %d != idle %d × startW %d", ErrInvariant, res.ReserveW, req.IdleConnectors, req.StartW)
	}
	if len(res.Allocations) != len(req.Sessions) || len(r.Details) != len(res.Allocations) {
		return fmt.Errorf("%w: %d allocations for %d sessions", ErrInvariant, len(res.Allocations), len(req.Sessions))
	}
	var sum int64
	for i, a := range res.Allocations {
		d := r.Details[i]
		if a.SessionID != d.Session.SessionID || a.Suspended == d.Admitted || a.LimitW != d.LimitW {
			return fmt.Errorf("%w: allocation %d does not match its session", ErrInvariant, i)
		}
		switch {
		case a.Suspended && a.LimitW != req.StartW:
			return fmt.Errorf("%w: suspended %s limit %d != startW %d", ErrInvariant, a.SessionID, a.LimitW, req.StartW)
		case !a.Suspended && (a.LimitW < d.Session.MinW || a.LimitW > d.DemandW || d.DemandW > d.Session.MaxW || a.LimitW < 0):
			return fmt.Errorf("%w: %s limit %d outside [minW %d, demand %d ≤ maxW %d]", ErrInvariant,
				a.SessionID, a.LimitW, d.Session.MinW, d.DemandW, d.Session.MaxW)
		}
		sum += a.LimitW
	}
	if sum != res.TotalAllocatedW {
		return fmt.Errorf("%w: totalAllocatedW %d != Σ limitW %d", ErrInvariant, res.TotalAllocatedW, sum)
	}
	if sum+res.ReserveW > req.SiteCapW {
		return fmt.Errorf("%w: Σ limitW %d + reserveW %d > siteCapW %d", ErrInvariant, sum, res.ReserveW, req.SiteCapW)
	}
	return nil
}
