package pricing

import (
	"sort"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

// EnergyPerBand splits total between the bands of v.Split(start, stop) (same order) by the energy the station's
// register shows in each band, instead of by the time spent in it (ADR 0011). The register is read at every segment
// boundary by linear interpolation between samples and held flat outside the sampled range; the per-band energies are
// then rescaled with LargestRemainder so they sum to total exactly (meterStop and the register may differ by a few Wh,
// and the minimum billable energy may apply).
//
// It returns false, and the caller falls back to time weights, when there is only one band, fewer than two samples,
// samples that are not strictly increasing in time, a register that goes down, or a register that does not move.
func (v *Version) EnergyPerBand(start, stop time.Time, profile []contracts.EnergySample, total int64) ([]int64, bool) {
	if len(profile) < 2 || len(v.Windows) == 0 {
		return nil, false
	}
	for i := 1; i < len(profile); i++ {
		if !profile[i].At.After(profile[i-1].At) || profile[i].EnergyWh < profile[i-1].EnergyWh {
			return nil, false
		}
	}
	start, stop = start.Truncate(time.Millisecond), stop.Truncate(time.Millisecond)
	if !stop.After(start) {
		return nil, false
	}
	bands := v.Split(start, stop)
	if len(bands) < 2 {
		return nil, false
	}
	idx := make(map[int]int, len(bands))
	for i, b := range bands {
		idx[b.Window] = i
	}
	weights := make([]int64, len(bands))
	var sum int64
	for _, s := range v.segments(start, stop) {
		e := register(profile, s.To) - register(profile, s.From)
		weights[idx[s.Window]] += e
		sum += e
	}
	if sum == 0 {
		return nil, false
	}
	return LargestRemainder(total, weights), true
}

// register is the cumulative energy at t, interpolated linearly between samples and held flat outside them.
// profile is non-empty and strictly increasing in time.
func register(profile []contracts.EnergySample, t time.Time) int64 {
	first, last := profile[0], profile[len(profile)-1]
	if !t.After(first.At) {
		return first.EnergyWh
	}
	if !t.Before(last.At) {
		return last.EnergyWh
	}
	k := sort.Search(len(profile), func(i int) bool { return profile[i].At.After(t) }) // first sample after t, k >= 1
	a, b := profile[k-1], profile[k]
	span := b.At.Sub(a.At).Milliseconds()
	if span == 0 {
		return a.EnergyWh
	}
	// Request.Validate bounds every sample to 0..MaxEnergyWh and to within MaxDuration of the session, so b-a <= 1e7 Wh and
	// elapsed (t is inside the session, a.At at most MaxDuration before it) <= 3 x MaxDuration = 8e9 ms: the product is
	// below 1e17, far inside int64. The weights then sum to at most the last register value (1e7), so LargestRemainder's
	// total*w stays below 1e14.
	return a.EnergyWh + (b.EnergyWh-a.EnergyWh)*t.Sub(a.At).Milliseconds()/span
}
