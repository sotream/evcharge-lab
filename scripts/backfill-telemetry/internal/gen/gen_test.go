package gen

import (
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func collect(c *Connector, from, to time.Time, slice time.Duration) []Row {
	var rows []Row
	for s := from; s.Before(to); s = s.Add(slice) {
		e := s.Add(slice)
		if e.After(to) {
			e = to
		}
		c.Advance(s, e, func(r Row) { rows = append(rows, r) })
	}
	return rows
}

func TestStationIDsAndShape(t *testing.T) {
	if got := StationID(0); got != "st-0001-001" {
		t.Fatalf("got %s", got)
	}
	if got := StationID(4999); got != "st-0500-010" {
		t.Fatalf("got %s", got)
	}
	if got := SiteID(10); got != "site-0002" {
		t.Fatalf("got %s", got)
	}
	dc := 0
	for i := 0; i < 1000; i++ {
		s := NewStation(i, DefaultDCShare)
		switch s.Kind {
		case DC:
			dc++
			if s.MaxW < 50_000 || s.MaxW > 150_000 {
				t.Fatalf("DC max %d", s.MaxW)
			}
		default:
			if s.MaxW != 11_000 && s.MaxW != 22_000 {
				t.Fatalf("AC max %d", s.MaxW)
			}
		}
	}
	if dc != 200 {
		t.Fatalf("dc share %d/1000", dc)
	}
}

func TestOccupancyProfile(t *testing.T) {
	var sum, peak float64
	for h := 0; h < 24; h++ {
		p := Occupancy(h)
		sum += p
		peak = math.Max(peak, p)
	}
	if math.Abs(sum/24-MeanUtilisation) > 1e-9 {
		t.Fatalf("mean %.4f", sum/24)
	}
	if peak < 0.37 || peak > 0.41 {
		t.Fatalf("peak %.3f", peak)
	}
}

func TestACFlatPowerAndMonotoneEnergy(t *testing.T) {
	c := NewConnector(NewStation(0, DefaultDCShare), 1, 7)
	rows := collect(c, t0, t0.Add(10*24*time.Hour), time.Hour)
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	powerByTx := map[int64]int32{}
	var prev Row
	for i, r := range rows {
		if r.SocPct != nil {
			t.Fatal("AC row has soc")
		}
		if p, ok := powerByTx[r.TransactionID]; ok && p != r.PowerW {
			t.Fatalf("AC power not flat in tx %d: %d vs %d", r.TransactionID, p, r.PowerW)
		}
		powerByTx[r.TransactionID] = r.PowerW
		if r.PowerW > int32(NewStation(0, DefaultDCShare).MaxW) {
			t.Fatalf("power %d exceeds connector max", r.PowerW)
		}
		if i > 0 {
			if r.EnergyWh < prev.EnergyWh {
				t.Fatal("energy register decreased")
			}
			if r.Time.Sub(prev.Time) < Step {
				t.Fatalf("readings closer than %s", Step)
			}
		}
		prev = r
	}
}

func TestDCPowerCurve(t *testing.T) {
	if DCPower(100_000, 50) != 100_000 || DCPower(100_000, 80) != 100_000 {
		t.Fatal("full power up to 80%")
	}
	if got := DCPower(100_000, 90); math.Abs(got-55_000) > 1 {
		t.Fatalf("90%% -> %.0f", got)
	}
	if got := DCPower(100_000, 100); math.Abs(got-10_000) > 1 {
		t.Fatalf("100%% -> %.0f", got)
	}
}

func TestDCTaperAfter80(t *testing.T) {
	c := NewConnector(NewStation(4, DefaultDCShare), 1, 3) // DC
	rows := collect(c, t0, t0.Add(30*24*time.Hour), time.Hour)
	tapered := 0
	last := map[int64]Row{}
	for _, r := range rows {
		if r.SocPct == nil {
			t.Fatal("DC row without soc")
		}
		if p, ok := last[r.TransactionID]; ok {
			if *r.SocPct < *p.SocPct {
				t.Fatal("soc decreased")
			}
			if *p.SocPct > 80 && r.PowerW > p.PowerW {
				t.Fatal("power rose after 80% soc")
			}
		}
		if *r.SocPct > 80 && r.PowerW < int32(NewStation(4, DefaultDCShare).MaxW) {
			tapered++
		}
		last[r.TransactionID] = r
	}
	if tapered == 0 {
		t.Fatal("no tapered readings observed")
	}
}

func TestSlicingDoesNotChangeOutput(t *testing.T) {
	st := NewStation(13, DefaultDCShare)
	a := collect(NewConnector(st, 2, 1), t0, t0.Add(5*24*time.Hour), time.Hour)
	b := collect(NewConnector(st, 2, 1), t0, t0.Add(5*24*time.Hour), 7*time.Hour+13*time.Minute)
	if len(a) == 0 || len(a) != len(b) {
		t.Fatalf("len %d vs %d", len(a), len(b))
	}
	for i := range a {
		x, y := a[i], b[i]
		if !x.Time.Equal(y.Time) || x.EnergyWh != y.EnergyWh || x.TransactionID != y.TransactionID || x.PowerW != y.PowerW {
			t.Fatalf("row %d differs", i)
		}
	}
}

func TestUtilisationAndSessionLength(t *testing.T) {
	const days = 14
	var active, total, sessions int
	var sessDur time.Duration
	for i := 0; i < 40; i++ {
		st := NewStation(i, DefaultDCShare)
		if st.Kind != AC {
			continue
		}
		for conn := int16(1); conn <= 2; conn++ {
			rows := collect(NewConnector(st, conn, 5), t0, t0.Add(days*24*time.Hour), 6*time.Hour)
			active += len(rows)
			total += days * 24 * 120
			first := map[int64]time.Time{}
			lastT := map[int64]time.Time{}
			for _, r := range rows {
				if _, ok := first[r.TransactionID]; !ok {
					first[r.TransactionID] = r.Time
				}
				lastT[r.TransactionID] = r.Time
			}
			for tx, f := range first {
				sessions++
				sessDur += lastT[tx].Sub(f) + Step
			}
		}
	}
	util := float64(active) / float64(total)
	if util < 0.085 || util > 0.115 {
		t.Fatalf("utilisation %.3f, want ~0.10", util)
	}
	mean := sessDur.Minutes() / float64(sessions)
	if mean < 40 || mean > 50 {
		t.Fatalf("mean AC session %.1f min, want ~45", mean)
	}
}

func TestEveningPeakBusierThanNight(t *testing.T) {
	var night, evening int
	for i := 0; i < 30; i++ {
		rows := collect(NewConnector(NewStation(i, DefaultDCShare), 1, 9), t0, t0.Add(7*24*time.Hour), 6*time.Hour)
		for _, r := range rows {
			switch h := r.Time.Hour(); {
			case h >= 18 && h <= 20:
				evening++
			case h >= 1 && h <= 3:
				night++
			}
		}
	}
	if evening < 5*night {
		t.Fatalf("evening %d vs night %d", evening, night)
	}
}

func TestFleetFixture(t *testing.T) {
	raw, err := os.ReadFile("../../../../packages/contracts/schemas/fleet-fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture []struct {
		SiteID   string `json:"siteId"`
		CapW     int    `json:"capW"`
		Stations []struct {
			StationID    string `json:"stationId"`
			SecretSha256 string `json:"secretSha256"`
			Connectors   []struct {
				ConnectorID int    `json:"connectorId"`
				CurrentType string `json:"currentType"`
				MaxPowerW   int    `json:"maxPowerW"`
				MinPowerW   int    `json:"minPowerW"`
				TariffID    string `json:"tariffId"`
			} `json:"connectors"`
		} `json:"stations"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture) != 3 {
		t.Fatalf("fixture has %d sites", len(fixture))
	}
	for si, site := range fixture {
		sum := 0
		if len(site.Stations) != StationsPerSite {
			t.Fatalf("site %s: %d stations", site.SiteID, len(site.Stations))
		}
		for n, fs := range site.Stations {
			g := si*StationsPerSite + n
			st := NewStation(g, DefaultDCShare)
			if st.ID != fs.StationID || st.Site != site.SiteID {
				t.Fatalf("g=%d ids %s/%s vs %s/%s", g, st.ID, st.Site, fs.StationID, site.SiteID)
			}
			if got := SecretSHA256("fixture-seed", st.ID); got != fs.SecretSha256 {
				t.Fatalf("%s secretSha256 %s vs %s", st.ID, got, fs.SecretSha256)
			}
			if len(fs.Connectors) != ConnectorsPerStation {
				t.Fatalf("%s connectors %d", st.ID, len(fs.Connectors))
			}
			wantType, wantMin := "AC", ACMinW
			if st.Kind == DC {
				wantType, wantMin = "DC", DCMinW
			}
			for _, c := range fs.Connectors {
				if c.CurrentType != wantType || c.MaxPowerW != st.MaxW || c.MinPowerW != wantMin ||
					c.TariffID != TariffID(g, c.ConnectorID) {
					t.Fatalf("%s connector %d mismatch: fixture %+v vs %s max %d min %d tariff %s",
						st.ID, c.ConnectorID, c, wantType, st.MaxW, wantMin, TariffID(g, c.ConnectorID))
				}
				sum += c.MaxPowerW
			}
		}
		if got := SiteCapW(site.SiteID, sum); got != site.CapW {
			t.Fatalf("%s capW %d vs %d", site.SiteID, got, site.CapW)
		}
	}
}
