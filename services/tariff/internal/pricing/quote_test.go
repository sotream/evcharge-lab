package pricing

import (
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

var epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// Seeded tariffs as in migrations/0002_seed_tariffs.sql.
func seedStd() *Version {
	return &Version{TariffID: "std", Version: 1, ValidFrom: epoch, Currency: "EUR", Timezone: "Europe/Berlin",
		SessionFeeMinor: 49, EnergyPriceMinorPerKWh: 39}
}

func seedTou() *Version {
	return &Version{TariffID: "tou", Version: 1, ValidFrom: epoch, Currency: "EUR", Timezone: "Europe/Kyiv",
		EnergyPriceMinorPerKWh: 49,
		Windows:                []Window{{From: "23:00", To: "07:00", PriceMinorPerKWh: 29, Label: "off-peak"}}}
}

func prep(t *testing.T, v *Version) *Version {
	t.Helper()
	if err := v.Prepare(); err != nil {
		t.Fatal(err)
	}
	return v
}

func ts(t *testing.T, s string) time.Time {
	t.Helper()
	x, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

type line struct {
	kind   string
	qty    float64
	amount int64
}

func summarize(res contracts.QuoteRes) []line {
	out := make([]line, len(res.Lines))
	for i, l := range res.Lines {
		out[i] = line{l.Kind, l.Quantity, l.AmountMinor}
	}
	return out
}

func TestGolden(t *testing.T) {
	// dstWin: window inside the Kyiv spring-forward gap (03:00→04:00 on 2026-03-29).
	dstSpring := &Version{TariffID: "dst", Version: 1, ValidFrom: epoch, Currency: "EUR", Timezone: "Europe/Kyiv",
		EnergyPriceMinorPerKWh: 40, TimePriceMinorPerHour: 60,
		Windows: []Window{{From: "03:30", To: "06:00", PriceMinorPerKWh: 20}}}
	// dstFall: window ending inside the repeated hour (03:00-04:00 twice on 2026-10-25).
	dstFall := &Version{TariffID: "dst", Version: 1, ValidFrom: epoch, Currency: "EUR", Timezone: "Europe/Kyiv",
		EnergyPriceMinorPerKWh: 40, TimePriceMinorPerHour: 60,
		Windows: []Window{{From: "00:00", To: "03:30", PriceMinorPerKWh: 20}}}
	minBill := &Version{TariffID: "min", Version: 1, ValidFrom: epoch, Currency: "EUR", Timezone: "UTC",
		EnergyPriceMinorPerKWh: 39, MinBillableWh: 1000}

	cases := []struct {
		name        string
		v           *Version
		start, stop string
		wh          int64
		want        []line
		total       int64
	}{
		{"std rounding", seedStd(), "2026-01-15T10:00:00Z", "2026-01-15T10:45:00Z", 1234,
			[]line{{"session", 1, 49}, {"energy", 1.234, 49}, {"time", 45, 0}, {"idle", 0, 0}}, 98},
		{"std across midnight is one energy line", seedStd(), "2026-01-14T22:30:00Z", "2026-01-15T00:30:00Z", 3000,
			[]line{{"session", 1, 49}, {"energy", 3, 117}, {"time", 120, 0}, {"idle", 0, 0}}, 166},
		// 22:30→00:30 Kyiv (+02): 30 min peak, 90 min off-peak; 750 / 2250 Wh.
		{"tou across midnight", seedTou(), "2026-01-14T20:30:00Z", "2026-01-14T22:30:00Z", 3000,
			[]line{{"session", 1, 0}, {"energy", 0.75, 37}, {"energy", 2.25, 66}, {"time", 120, 0}, {"idle", 0, 0}}, 103},
		// 06:15→07:45 Kyiv: 45 off-peak, 45 peak.
		{"tou across window end", seedTou(), "2026-01-15T04:15:00Z", "2026-01-15T05:45:00Z", 1000,
			[]line{{"session", 1, 0}, {"energy", 0.5, 15}, {"energy", 0.5, 25}, {"time", 90, 0}, {"idle", 0, 0}}, 40},
		// 06:20→07:30 Kyiv: 40/30 min; 571.43 / 428.57 → largest remainder gives 571 / 429.
		{"tou largest remainder", seedTou(), "2026-01-15T04:20:00Z", "2026-01-15T05:30:00Z", 1000,
			[]line{{"session", 1, 0}, {"energy", 0.571, 17}, {"energy", 0.429, 22}, {"time", 70, 0}, {"idle", 0, 0}}, 39},
		// Spring forward: 02:00 (+02) → 05:00 (+03) local is 2 h real, all off-peak.
		{"tou DST spring forward", seedTou(), "2026-03-29T00:00:00Z", "2026-03-29T02:00:00Z", 1000,
			[]line{{"session", 1, 0}, {"energy", 1, 29}, {"time", 120, 0}, {"idle", 0, 0}}, 29},
		// Fall back: 22:00 (+03) → 08:00 (+02) is 11 h real; peak 22-23 and 07-08 (120 min), off-peak 540 min.
		{"tou DST fall back", seedTou(), "2026-10-24T19:00:00Z", "2026-10-25T06:00:00Z", 11000,
			[]line{{"session", 1, 0}, {"energy", 2, 98}, {"energy", 9, 261}, {"time", 660, 0}, {"idle", 0, 0}}, 359},
		// Window 03:30 does not exist on 2026-03-29: shifted to 04:30 (+03) = 01:30Z; ends 06:00 = 03:00Z.
		// Session 00:00Z-04:00Z: base 90+60 min, window 90 min.
		{"window start in DST gap", dstSpring, "2026-03-29T00:00:00Z", "2026-03-29T04:00:00Z", 2400,
			[]line{{"session", 1, 0}, {"energy", 1.5, 60}, {"energy", 0.9, 18}, {"time", 240, 240}, {"idle", 0, 0}}, 318},
		// Window end 03:30 is ambiguous on 2026-10-25: first occurrence (+03) = 00:30Z.
		// Session 23:00Z-02:00Z: window 90 min, base 90 min.
		{"window end in DST overlap", dstFall, "2026-10-24T23:00:00Z", "2026-10-25T02:00:00Z", 3000,
			[]line{{"session", 1, 0}, {"energy", 1.5, 30}, {"energy", 1.5, 60}, {"time", 180, 180}, {"idle", 0, 0}}, 270},
		{"zero energy", seedStd(), "2026-01-15T10:00:00Z", "2026-01-15T10:05:00Z", 0,
			[]line{{"session", 1, 49}, {"energy", 0, 0}, {"time", 5, 0}, {"idle", 0, 0}}, 49},
		{"zero energy tou keeps per-band lines", seedTou(), "2026-01-15T04:15:00Z", "2026-01-15T05:45:00Z", 0,
			[]line{{"session", 1, 0}, {"energy", 0, 0}, {"energy", 0, 0}, {"time", 90, 0}, {"idle", 0, 0}}, 0},
		{"minimum billable applied", minBill, "2026-01-15T10:00:00Z", "2026-01-15T10:05:00Z", 200,
			[]line{{"session", 1, 0}, {"energy", 1, 39}, {"time", 5, 0}, {"idle", 0, 0}}, 39},
		{"minimum billable not applied to zero energy", minBill, "2026-01-15T10:00:00Z", "2026-01-15T10:05:00Z", 0,
			[]line{{"session", 1, 0}, {"energy", 0, 0}, {"time", 5, 0}, {"idle", 0, 0}}, 0},
		{"minimum billable above threshold", minBill, "2026-01-15T10:00:00Z", "2026-01-15T10:05:00Z", 1500,
			[]line{{"session", 1, 0}, {"energy", 1.5, 59}, {"time", 5, 0}, {"idle", 0, 0}}, 59},
		{"zero duration", seedTou(), "2026-01-15T04:15:00Z", "2026-01-15T04:15:00Z", 100,
			[]line{{"session", 1, 0}, {"energy", 0.1, 3}, {"time", 0, 0}, {"idle", 0, 0}}, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := prep(t, c.v)
			res, err := Quote(v, Request{StartedAt: ts(t, c.start), StoppedAt: ts(t, c.stop), EnergyWh: c.wh})
			if err != nil {
				t.Fatal(err)
			}
			if got := summarize(res); !reflect.DeepEqual(got, c.want) {
				t.Errorf("lines\n got %+v\nwant %+v", got, c.want)
			}
			if res.TotalMinor != c.total {
				t.Errorf("total %d, want %d", res.TotalMinor, c.total)
			}
			if res.Currency != "EUR" || res.TariffID != v.TariffID {
				t.Errorf("header %+v", res)
			}
		})
	}
}

func TestVersionInForceAtStart(t *testing.T) {
	v1 := seedStd()
	v2 := seedStd()
	v2.Version, v2.ValidFrom, v2.EnergyPriceMinorPerKWh = 2, ts(t, "2026-06-01T00:00:00Z"), 45
	tr, err := NewTariff("std", []*Version{v2, v1})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		at   string
		want int
	}{{"2026-05-31T23:59:59Z", 1}, {"2026-06-01T00:00:00Z", 2}, {"2030-01-01T00:00:00Z", 2}} {
		v, err := tr.At(ts(t, c.at))
		if err != nil || v.Version != c.want {
			t.Errorf("At(%s) = %v %v, want v%d", c.at, v, err, c.want)
		}
	}
	if _, err := tr.At(ts(t, "2019-12-31T23:59:59Z")); err == nil {
		t.Error("expected ErrNoVersion before the first version")
	}
	// A session spanning the version change is priced entirely with v1.
	v, _ := tr.At(ts(t, "2026-05-31T23:30:00Z"))
	res, _ := Quote(v, Request{StartedAt: ts(t, "2026-05-31T23:30:00Z"), StoppedAt: ts(t, "2026-06-01T00:30:00Z"), EnergyWh: 1000})
	if res.Lines[1].AmountMinor != 39 {
		t.Errorf("energy %d, want 39 (v1 price)", res.Lines[1].AmountMinor)
	}
}

func TestPrepareRejectsBadTariffs(t *testing.T) {
	bad := []*Version{
		{TariffID: "x", Currency: "eur", Timezone: "UTC"},
		{TariffID: "x", Currency: "EUR", Timezone: "Mars/Olympus"},
		{TariffID: "x", Currency: "EUR", Timezone: "UTC", EnergyPriceMinorPerKWh: -1},
		{TariffID: "x", Currency: "EUR", Timezone: "UTC", Windows: []Window{{From: "25:00", To: "01:00"}}},
		{TariffID: "x", Currency: "EUR", Timezone: "UTC", Windows: []Window{{From: "01:00", To: "01:00"}}},
		{TariffID: "x", Currency: "EUR", Timezone: "UTC", Windows: []Window{{From: "01:00", To: "02:00", Days: []string{"xyz"}}}},
	}
	for i, v := range bad {
		if err := v.Prepare(); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestRequestValidation(t *testing.T) {
	v := prep(t, seedStd())
	t0 := ts(t, "2026-01-15T10:00:00Z")
	for _, r := range []Request{
		{StartedAt: t0, StoppedAt: t0.Add(-time.Second), EnergyWh: 1},
		{StartedAt: t0, StoppedAt: t0.Add(32 * 24 * time.Hour), EnergyWh: 1},
		{StartedAt: t0, StoppedAt: t0, EnergyWh: -1},
		{StartedAt: t0, StoppedAt: t0, EnergyWh: MaxEnergyWh + 1},
		{StoppedAt: t0},
	} {
		if _, err := Quote(v, r); err == nil {
			t.Errorf("%+v: expected error", r)
		}
	}
}

func TestWeekdayWindowsAndOverlapPriority(t *testing.T) {
	v := prep(t, &Version{TariffID: "wk", Currency: "EUR", Timezone: "UTC", EnergyPriceMinorPerKWh: 50,
		Windows: []Window{
			{Days: []string{"sat", "sun"}, From: "00:00", To: "24:00", PriceMinorPerKWh: 10},
			{From: "18:00", To: "22:00", PriceMinorPerKWh: 90},
		}})
	// Friday 2026-01-16 20:00 → Saturday 02:00: 2 h peak (window 1), 2 h base, 2 h weekend (window 0).
	bands := v.Split(ts(t, "2026-01-16T20:00:00Z"), ts(t, "2026-01-17T02:00:00Z"))
	want := []Band{{Window: 1, Ms: 2 * 3_600_000}, {Window: -1, Ms: 2 * 3_600_000}, {Window: 0, Ms: 2 * 3_600_000}}
	if !reflect.DeepEqual(bands, want) {
		t.Fatalf("got %+v want %+v", bands, want)
	}
	// Saturday 18:00-22:00 matches both; the lower index (weekend) wins.
	bands = v.Split(ts(t, "2026-01-17T17:00:00Z"), ts(t, "2026-01-17T23:00:00Z"))
	if !reflect.DeepEqual(bands, []Band{{Window: 0, Ms: 6 * 3_600_000}}) {
		t.Fatalf("got %+v", bands)
	}
}

func TestLocalInstant(t *testing.T) {
	kyiv, _ := time.LoadLocation("Europe/Kyiv")
	for _, c := range []struct {
		y, m, d, min int
		want         string
	}{
		{2026, 1, 15, 8 * 60, "2026-01-15T06:00:00Z"},
		{2026, 3, 29, 3*60 + 30, "2026-03-29T01:30:00Z"}, // gap → 04:30 +03
		{2026, 3, 29, 4 * 60, "2026-03-29T01:00:00Z"},
		{2026, 10, 25, 3*60 + 30, "2026-10-25T00:30:00Z"}, // ambiguous → first (+03)
		{2026, 10, 25, 4 * 60, "2026-10-25T02:00:00Z"},
		{2026, 1, 31, 1440, "2026-01-31T22:00:00Z"},
	} {
		got := LocalInstant(c.y, time.Month(c.m), c.d, c.min, kyiv)
		if got.UTC().Format(time.RFC3339) != c.want {
			t.Errorf("%d-%d-%d %d: got %s want %s", c.y, c.m, c.d, c.min, got.UTC().Format(time.RFC3339), c.want)
		}
	}
}

func TestLargestRemainder(t *testing.T) {
	for _, c := range []struct {
		total int64
		w     []int64
		want  []int64
	}{
		{10, []int64{1, 1, 1}, []int64{4, 3, 3}},
		{1000, []int64{40, 30}, []int64{571, 429}},
		{7, []int64{0, 0}, []int64{7, 0}},
		{0, []int64{5, 5}, []int64{0, 0}},
	} {
		if got := LargestRemainder(c.total, c.w); !reflect.DeepEqual(got, c.want) {
			t.Errorf("LR(%d,%v)=%v want %v", c.total, c.w, got, c.want)
		}
	}
}

// Property: for random tariffs and sessions, lines sum to the total, the
// energy split sums to the billable Wh, every energy line is ceil-rounded,
// and the quote is deterministic.
func TestPropertyQuoteInvariants(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 7))
	zones := []string{"Europe/Kyiv", "Europe/Berlin", "UTC", "America/New_York", "Australia/Lord_Howe"}
	days := []string{"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
	hhmm := func() string {
		m := rng.IntN(48) * 30
		return time.Date(0, 1, 1, 0, m, 0, 0, time.UTC).Format("15:04")
	}
	base := ts(t, "2025-01-01T00:00:00Z")
	for iter := range 20_000 {
		v := &Version{TariffID: "p", Currency: "EUR", Timezone: zones[rng.IntN(len(zones))],
			SessionFeeMinor: rng.Int64N(200), EnergyPriceMinorPerKWh: rng.Int64N(100),
			TimePriceMinorPerHour: rng.Int64N(300), MinBillableWh: rng.Int64N(2000)}
		for range rng.IntN(4) {
			w := Window{From: hhmm(), To: hhmm(), PriceMinorPerKWh: rng.Int64N(100)}
			if w.From == w.To {
				continue
			}
			for _, d := range days {
				if rng.IntN(2) == 0 {
					w.Days = append(w.Days, d)
				}
			}
			v.Windows = append(v.Windows, w)
		}
		if err := v.Prepare(); err != nil {
			t.Fatal(err)
		}
		start := base.Add(time.Duration(rng.Int64N(int64(2 * 365 * 24 * time.Hour))))
		stop := start.Add(time.Duration(rng.Int64N(int64(72 * time.Hour))))
		req := Request{StartedAt: start, StoppedAt: stop, EnergyWh: rng.Int64N(200_000)}
		res, err := Quote(v, req)
		if err != nil {
			t.Fatal(err)
		}
		var sum, energyWh, bandMs int64
		for _, l := range res.Lines {
			sum += l.AmountMinor
			if l.AmountMinor < 0 {
				t.Fatalf("iter %d: negative line %+v", iter, l)
			}
		}
		bands := v.Split(start, stop)
		energyLines := res.Lines[1 : len(res.Lines)-2]
		if len(energyLines) != len(bands) {
			t.Fatalf("iter %d: %d energy lines for %d bands", iter, len(energyLines), len(bands))
		}
		for i, l := range energyLines {
			wh := int64(l.Quantity*1000 + 0.5)
			energyWh += wh
			bandMs += bands[i].Ms
			price := v.EnergyPriceMinorPerKWh
			if bands[i].Window >= 0 {
				price = v.Windows[bands[i].Window].PriceMinorPerKWh
			}
			if l.AmountMinor != ceilDiv(wh*price, 1000) {
				t.Fatalf("iter %d: line %+v not ceil(%d×%d/1000)", iter, l, wh, price)
			}
		}
		wantMs := stop.Truncate(time.Millisecond).Sub(start.Truncate(time.Millisecond)).Milliseconds()
		wantWh := req.EnergyWh
		if wantWh > 0 && wantWh < v.MinBillableWh {
			wantWh = v.MinBillableWh
		}
		if sum != res.TotalMinor || energyWh != wantWh || bandMs != wantMs {
			t.Fatalf("iter %d: sum %d total %d energy %d want %d ms %d want %d (%+v %+v)",
				iter, sum, res.TotalMinor, energyWh, wantWh, bandMs, wantMs, v, req)
		}
		again, _ := Quote(v, req)
		if !reflect.DeepEqual(res, again) {
			t.Fatalf("iter %d: not deterministic", iter)
		}
	}
}

func BenchmarkQuoteTou(b *testing.B) {
	v := seedTou()
	if err := v.Prepare(); err != nil {
		b.Fatal(err)
	}
	start := time.Date(2026, 1, 14, 20, 30, 0, 0, time.UTC)
	req := Request{StartedAt: start, StoppedAt: start.Add(2 * time.Hour), EnergyWh: 30_000}
	for b.Loop() {
		if _, err := Quote(v, req); err != nil {
			b.Fatal(err)
		}
	}
}
