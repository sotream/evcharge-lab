package pricing

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

// Input bounds keep every intermediate product inside int64:
// MaxEnergyWh × duration(ms) ≤ 1e7 × 2.7e9 and price × quantity ≤ 1e7 × 2.7e9.
const (
	MaxEnergyWh = 10_000_000 // 10 MWh
	MaxDuration = 31 * 24 * time.Hour
	maxPrice    = 10_000_000
)

// ErrInvalidRequest reports a request that cannot be priced.
var ErrInvalidRequest = errors.New("invalid quote request")

// Request is the priced input (contracts.QuoteReq without the tariff id).
type Request struct {
	StartedAt time.Time
	StoppedAt time.Time
	EnergyWh  int64
}

// Validate checks the request bounds.
func (r Request) Validate() error {
	switch {
	case r.StartedAt.IsZero() || r.StoppedAt.IsZero():
		return fmt.Errorf("%w: startedAt and stoppedAt are required", ErrInvalidRequest)
	case r.StoppedAt.Before(r.StartedAt):
		return fmt.Errorf("%w: stoppedAt is before startedAt", ErrInvalidRequest)
	case r.StoppedAt.Sub(r.StartedAt) > MaxDuration:
		return fmt.Errorf("%w: session longer than %s", ErrInvalidRequest, MaxDuration)
	case r.EnergyWh < 0 || r.EnergyWh > MaxEnergyWh:
		return fmt.Errorf("%w: energyWh must be in 0..%d", ErrInvalidRequest, MaxEnergyWh)
	}
	return nil
}

// Band is the time a session spent under one energy price.
type Band struct {
	Window int // index into Version.Windows, or -1 for the base price
	Ms     int64
}

// ceilDiv is ceil(a/b) for a >= 0, b > 0.
func ceilDiv(a, b int64) int64 { return (a + b - 1) / b }

// Quote prices req under v. It is a pure function of (v, req).
func Quote(v *Version, req Request) (contracts.QuoteRes, error) {
	if err := req.Validate(); err != nil {
		return contracts.QuoteRes{}, err
	}
	if v.loc == nil {
		return contracts.QuoteRes{}, fmt.Errorf("%w: version not prepared", ErrInvalidTariff)
	}
	res := contracts.QuoteRes{TariffID: v.TariffID, Currency: v.Currency, Lines: make([]contracts.QuoteLine, 0, 5)}
	add := func(l contracts.QuoteLine) {
		res.Lines = append(res.Lines, l)
		res.TotalMinor += l.AmountMinor
	}

	add(contracts.QuoteLine{
		Kind: "session", Description: fmt.Sprintf("Session fee (tariff %s v%d)", v.TariffID, v.Version),
		Quantity: 1, Unit: "session", AmountMinor: v.SessionFeeMinor,
	})

	billable := req.EnergyWh
	minApplied := false
	if billable > 0 && billable < v.MinBillableWh {
		billable, minApplied = v.MinBillableWh, true
	}
	bands := v.Split(req.StartedAt, req.StoppedAt)
	weights := make([]int64, len(bands))
	for i, b := range bands {
		weights[i] = b.Ms
	}
	parts := LargestRemainder(billable, weights)
	for i, b := range bands {
		price, name := v.EnergyPriceMinorPerKWh, "base"
		if b.Window >= 0 {
			w := &v.Windows[b.Window]
			price, name = w.PriceMinorPerKWh, w.describe()
		}
		desc := fmt.Sprintf("Energy %s: %d Wh x %s %s/kWh, rounded up per line", name, parts[i], money(price, v.Currency), v.Currency)
		if minApplied {
			desc += fmt.Sprintf("; minimum billable %d Wh applied to %d Wh delivered", v.MinBillableWh, req.EnergyWh)
		}
		add(contracts.QuoteLine{
			Kind: "energy", Description: desc, Quantity: float64(parts[i]) / 1000, Unit: "kWh",
			AmountMinor: ceilDiv(parts[i]*price, 1000),
		})
	}

	ms := req.StoppedAt.Truncate(time.Millisecond).Sub(req.StartedAt.Truncate(time.Millisecond)).Milliseconds()
	add(contracts.QuoteLine{
		Kind:        "time",
		Description: fmt.Sprintf("Time: %s %s/h, rounded up", money(v.TimePriceMinorPerHour, v.Currency), v.Currency),
		Quantity:    round3(float64(ms) / 60_000), Unit: "min",
		AmountMinor: ceilDiv(ms*v.TimePriceMinorPerHour, 3_600_000),
	})
	// ADR 0011: idle time is unknown from totals only, so v1 bills quantity 0.
	add(contracts.QuoteLine{
		Kind: "idle",
		Description: fmt.Sprintf("Idle: %s %s/min after %d min grace (not measured in v1)",
			money(v.IdleFeeMinorPerMin, v.Currency), v.Currency, v.IdleGraceMin),
		Quantity: 0, Unit: "min", AmountMinor: 0,
	})
	return res, nil
}

func round3(x float64) float64 { return math.Round(x*1000) / 1000 }

var zeroDecimalCurrencies = map[string]bool{"JPY": true, "KRW": true, "ISK": true, "CLP": true, "VND": true}

func money(minor int64, currency string) string {
	if zeroDecimalCurrencies[currency] {
		return fmt.Sprintf("%d", minor)
	}
	return fmt.Sprintf("%d.%02d", minor/100, minor%100)
}

// LargestRemainder splits total into len(weights) integer parts proportional
// to weights that sum exactly to total (Hamilton's method). Ties on the
// remainder go to the lower index. If all weights are 0 the first part gets
// everything.
func LargestRemainder(total int64, weights []int64) []int64 {
	parts := make([]int64, len(weights))
	if len(weights) == 0 {
		return parts
	}
	var sum int64
	for _, w := range weights {
		sum += w
	}
	if sum == 0 {
		parts[0] = total
		return parts
	}
	type rem struct {
		i int
		r int64
	}
	rems := make([]rem, len(weights))
	var assigned int64
	for i, w := range weights {
		parts[i] = total * w / sum
		rems[i] = rem{i, total * w % sum}
		assigned += parts[i]
	}
	sort.SliceStable(rems, func(a, b int) bool { return rems[a].r > rems[b].r })
	for k := int64(0); k < total-assigned; k++ {
		parts[rems[k].i]++
	}
	return parts
}

type instance struct {
	window   int
	from, to time.Time
}

// Split returns the time [start, stop) spent under each energy price band,
// in order of first occurrence. Instants are truncated to milliseconds. Windows are evaluated as absolute instants in
// the tariff timezone (see LocalInstant for DST gaps and overlaps); where
// windows overlap the lower index wins. A zero-length session gets one band
// (the one in force at start) with Ms 0.
func (v *Version) Split(start, stop time.Time) []Band {
	start, stop = start.Truncate(time.Millisecond), stop.Truncate(time.Millisecond)
	if len(v.Windows) == 0 {
		return []Band{{Window: -1, Ms: stop.Sub(start).Milliseconds()}}
	}
	var inst []instance
	ls := start.In(v.loc)
	le := stop.In(v.loc)
	first := time.Date(ls.Year(), ls.Month(), ls.Day()-1, 12, 0, 0, 0, time.UTC)
	last := time.Date(le.Year(), le.Month(), le.Day(), 12, 0, 0, 0, time.UTC)
	for d := first; !d.After(last); d = d.AddDate(0, 0, 1) {
		for i := range v.Windows {
			w := &v.Windows[i]
			if w.dayMask&(1<<d.Weekday()) == 0 {
				continue
			}
			from := LocalInstant(d.Year(), d.Month(), d.Day(), w.fromMin, v.loc)
			toDay := d.Day()
			if w.wrapping {
				toDay++
			}
			to := LocalInstant(d.Year(), d.Month(), toDay, w.toMin, v.loc)
			if to.After(from) && to.After(start) && !from.After(stop) {
				inst = append(inst, instance{i, from, to})
			}
		}
	}
	bandAt := func(t time.Time) int {
		best := -1
		for _, in := range inst {
			if !t.Before(in.from) && t.Before(in.to) && (best == -1 || in.window < best) {
				best = in.window
			}
		}
		return best
	}
	if !stop.After(start) {
		return []Band{{Window: bandAt(start), Ms: 0}}
	}
	cuts := []time.Time{start, stop}
	for _, in := range inst {
		if in.from.After(start) && in.from.Before(stop) {
			cuts = append(cuts, in.from)
		}
		if in.to.After(start) && in.to.Before(stop) {
			cuts = append(cuts, in.to)
		}
	}
	sort.Slice(cuts, func(i, j int) bool { return cuts[i].Before(cuts[j]) })
	var bands []Band
	idx := map[int]int{}
	for k := 0; k+1 < len(cuts); k++ {
		if !cuts[k+1].After(cuts[k]) {
			continue
		}
		w := bandAt(cuts[k])
		j, ok := idx[w]
		if !ok {
			j = len(bands)
			idx[w] = j
			bands = append(bands, Band{Window: w})
		}
		bands[j].Ms += cuts[k+1].Sub(cuts[k]).Milliseconds()
	}
	return bands
}

// LocalInstant resolves the wall-clock time y-m-d + minute (minute may be
// 1440 = next midnight; d may overflow the month) in loc deterministically:
//   - an ambiguous wall time (DST fall back) resolves to its first
//     occurrence (the earlier instant);
//   - a non-existent wall time (DST spring forward gap) is shifted forward by
//     the gap length (03:30 in a 03:00→04:00 gap becomes 04:30).
//
// time.Date leaves both cases unspecified, so it is not used for this.
func LocalInstant(y int, m time.Month, d, minute int, loc *time.Location) time.Time {
	naive := time.Date(y, m, d, 0, minute, 0, 0, time.UTC)
	wy, wm, wd := naive.Date()
	wh, wmin := naive.Hour(), naive.Minute()
	_, offBefore := naive.Add(-24 * time.Hour).In(loc).Zone()
	_, offAfter := naive.Add(24 * time.Hour).In(loc).Zone()
	var best time.Time
	found := false
	for _, off := range []int{offBefore, offAfter} {
		t := naive.Add(-time.Duration(off) * time.Second)
		l := t.In(loc)
		ly, lm, ld := l.Date()
		if ly == wy && lm == wm && ld == wd && l.Hour() == wh && l.Minute() == wmin {
			if !found || t.Before(best) {
				best, found = t, true
			}
		}
	}
	if found {
		return best
	}
	return naive.Add(-time.Duration(offBefore) * time.Second)
}
