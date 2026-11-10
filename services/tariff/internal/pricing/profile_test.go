package pricing

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

func sample(t time.Time, wh int64) contracts.EnergySample {
	return contracts.EnergySample{At: t, EnergyWh: wh}
}

// seedTou: Europe/Kyiv, off-peak 23:00–07:00. 2026-11-10 is UTC+2.
func TestEnergyPerBandFollowsMeter(t *testing.T) {
	v := prep(t, seedTou())
	start := ts(t, "2026-11-10T20:00:00Z") // 22:00 Kyiv, base price
	stop := ts(t, "2026-11-10T22:00:00Z")  // 00:00 Kyiv, off-peak since 23:00
	prof := []contracts.EnergySample{sample(start, 0), sample(start.Add(time.Hour), 2000), sample(stop, 10000)}
	got, ok := v.EnergyPerBand(start, stop, prof, 10000)
	if !ok || !reflect.DeepEqual(got, []int64{2000, 8000}) {
		t.Fatalf("got %v %v, want [2000 8000] true", got, ok)
	}
}

// base 22–23, off-peak 23–07, base 07–08: Split aggregates the two base segments into one band.
func TestEnergyPerBandAggregatesNonContiguousSegments(t *testing.T) {
	v := prep(t, seedTou())
	start := ts(t, "2026-11-10T20:00:00Z")
	stop := ts(t, "2026-11-11T06:00:00Z")
	prof := []contracts.EnergySample{
		sample(start, 0), sample(ts(t, "2026-11-10T21:00:00Z"), 1000),
		sample(ts(t, "2026-11-11T05:00:00Z"), 5000), sample(stop, 6000),
	}
	bands := v.Split(start, stop)
	if len(bands) != 2 || bands[0].Window != -1 || bands[1].Window != 0 {
		t.Fatalf("unexpected bands %+v", bands)
	}
	got, ok := v.EnergyPerBand(start, stop, prof, 6000)
	if !ok || !reflect.DeepEqual(got, []int64{2000, 4000}) {
		t.Fatalf("got %v %v, want [2000 4000] true", got, ok)
	}
}

// The profile is rescaled to the billed total (the register and meterStop may disagree by a few Wh).
func TestEnergyPerBandRescalesToTotal(t *testing.T) {
	v := prep(t, seedTou())
	start := ts(t, "2026-11-10T20:00:00Z")
	stop := ts(t, "2026-11-10T22:00:00Z")
	prof := []contracts.EnergySample{sample(start, 0), sample(start.Add(time.Hour), 1000), sample(stop, 3000)}
	got, ok := v.EnergyPerBand(start, stop, prof, 3001)
	if !ok || got[0]+got[1] != 3001 || got[0] != 1000 {
		t.Fatalf("got %v %v", got, ok)
	}
}

// Samples between the boundaries are interpolated linearly; outside the sampled range the register is held.
func TestEnergyPerBandInterpolates(t *testing.T) {
	v := prep(t, seedTou())
	start := ts(t, "2026-11-10T20:00:00Z")
	stop := ts(t, "2026-11-10T22:00:00Z")
	prof := []contracts.EnergySample{sample(start.Add(30*time.Minute), 0), sample(start.Add(90*time.Minute), 4000)}
	got, ok := v.EnergyPerBand(start, stop, prof, 4000)
	if !ok || !reflect.DeepEqual(got, []int64{2000, 2000}) {
		t.Fatalf("got %v %v, want [2000 2000] true", got, ok)
	}
}

func TestEnergyPerBandFallsBack(t *testing.T) {
	v := prep(t, seedTou())
	start := ts(t, "2026-11-10T20:00:00Z")
	stop := ts(t, "2026-11-10T22:00:00Z")
	cases := map[string][]contracts.EnergySample{
		"none":          nil,
		"one sample":    {sample(start, 0)},
		"time reversed": {sample(stop, 0), sample(start, 10)},
		"register down": {sample(start, 100), sample(stop, 50)},
		"flat register": {sample(start, 100), sample(stop, 100)},
	}
	for name, prof := range cases {
		if _, ok := v.EnergyPerBand(start, stop, prof, 1000); ok {
			t.Errorf("%s: expected fallback", name)
		}
	}
}

// Review Focus 4: a quote with an unusable profile still prices, by time.
func TestQuoteUsesProfileAndFallsBack(t *testing.T) {
	v := prep(t, seedTou())
	start := ts(t, "2026-11-10T20:00:00Z")
	stop := ts(t, "2026-11-10T22:00:00Z")
	energyLines := func(prof []contracts.EnergySample) []contracts.QuoteLine {
		res, err := Quote(v, Request{StartedAt: start, StoppedAt: stop, EnergyWh: 10000, EnergyProfile: prof})
		if err != nil {
			t.Fatal(err)
		}
		var out []contracts.QuoteLine
		for _, l := range res.Lines {
			if l.Kind == "energy" {
				out = append(out, l)
			}
		}
		return out
	}
	metered := energyLines([]contracts.EnergySample{sample(start, 0), sample(start.Add(time.Hour), 2000), sample(stop, 10000)})
	if metered[0].Quantity != 2 || metered[1].Quantity != 8 || !strings.Contains(metered[1].Description, "metered") {
		t.Fatalf("metered lines %+v", metered)
	}
	timed := energyLines([]contracts.EnergySample{sample(start, 0)})
	if timed[0].Quantity != 5 || timed[1].Quantity != 5 || strings.Contains(timed[1].Description, "metered") {
		t.Fatalf("time-weighted lines %+v", timed)
	}
}

func TestValidateBoundsProfileLength(t *testing.T) {
	start := ts(t, "2026-11-10T20:00:00Z")
	prof := make([]contracts.EnergySample, MaxProfileSamples+1)
	err := Request{StartedAt: start, StoppedAt: start.Add(time.Hour), EnergyWh: 1, EnergyProfile: prof}.Validate()
	if err == nil {
		t.Fatal("expected a validation error")
	}
}

func TestValidateBoundsProfileSamples(t *testing.T) {
	start := ts(t, "2026-11-10T20:00:00Z")
	stop := start.Add(time.Hour)
	req := func(prof ...contracts.EnergySample) Request {
		return Request{StartedAt: start, StoppedAt: stop, EnergyWh: 1000, EnergyProfile: prof}
	}
	v := prep(t, seedTou())
	bad := map[string]Request{
		"huge energy":     req(sample(start, 0), sample(stop, 9_000_000_000_000_000_000)),
		"negative energy": req(sample(start, -1), sample(stop, 5)),
		"before window":   req(sample(start.Add(-MaxDuration-time.Second), 0), sample(stop, 5)),
		"after window":    req(sample(start, 0), sample(stop.Add(MaxDuration+time.Second), 5)),
	}
	for name, r := range bad {
		if err := r.Validate(); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: Validate = %v, want ErrInvalidRequest", name, err)
		}
		if _, err := Quote(v, r); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: Quote = %v, want ErrInvalidRequest", name, err)
		}
	}
	if err := req(sample(start, 0), sample(stop, MaxEnergyWh)).Validate(); err != nil {
		t.Errorf("samples at the bounds must be valid: %v", err)
	}
}

// Energy after the last sample is not attributed to any band: the register is held flat there, so the billed total is
// rescaled over the sampled part only.
func TestEnergyPerBandIgnoresEnergyAfterLastSample(t *testing.T) {
	v := prep(t, seedTou())
	start := ts(t, "2026-11-10T20:00:00Z") // 22:00 Kyiv, base price until 23:00 (21:00Z), then off-peak
	stop := ts(t, "2026-11-10T22:00:00Z")
	// samples end at 21:30Z, 30 minutes into the off-peak band; nothing is known about the last 30 minutes
	prof := []contracts.EnergySample{sample(start, 0), sample(start.Add(time.Hour), 2000), sample(start.Add(90*time.Minute), 6000)}
	got, ok := v.EnergyPerBand(start, stop, prof, 8000)
	if !ok || !reflect.DeepEqual(got, []int64{2667, 5333}) {
		t.Fatalf("got %v %v, want [2667 5333] true (weights 2000:4000 rescaled to 8000)", got, ok)
	}
	res, err := Quote(v, Request{StartedAt: start, StoppedAt: stop, EnergyWh: 8000, EnergyProfile: prof})
	if err != nil || len(res.Lines) == 0 {
		t.Fatalf("a profile ending before the session end must still price: %v", err)
	}
}
