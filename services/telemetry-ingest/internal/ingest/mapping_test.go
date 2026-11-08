package ingest

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

var t0 = time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)

func mv(samples ...contracts.MeterSample) contracts.MeterValues {
	tx := int64(7)
	return contracts.MeterValues{StationID: "st-1", ConnectorID: 2, TransactionID: &tx, OcppMessageID: "m1",
		Readings: []contracts.MeterReading{{Timestamp: t0, Samples: samples}}}
}

func noUnknown(t *testing.T) func(string) {
	return func(m string) { t.Errorf("unexpected unknown measurand %q", m) }
}

func TestMapFullReading(t *testing.T) {
	rows := Map(mv(
		contracts.MeterSample{Measurand: MeasurandEnergy, Value: 12.345, Unit: "kWh"},
		contracts.MeterSample{Measurand: MeasurandPower, Value: 7.4, Unit: "kW"},
		contracts.MeterSample{Measurand: MeasurandCurrent, Value: 32, Unit: "A"},
		contracts.MeterSample{Measurand: MeasurandVoltage, Value: 231.5, Unit: "V"},
		contracts.MeterSample{Measurand: MeasurandSoC, Value: 55, Unit: "Percent"},
	), noUnknown(t))
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	r := rows[0]
	if *r.EnergyWh != 12345 || *r.PowerW != 7400 || *r.CurrentA != 32 || *r.VoltageV != 231.5 || *r.SocPct != 55 {
		t.Fatalf("bad mapping: e=%d p=%d c=%v v=%v s=%v", *r.EnergyWh, *r.PowerW, *r.CurrentA, *r.VoltageV, *r.SocPct)
	}
	if r.StationID != "st-1" || r.ConnectorID != 2 || *r.TransactionID != 7 || !r.Time.Equal(t0) || r.Replayed {
		t.Fatalf("bad identity: %+v", r)
	}
}

func TestMapDefaultsAndUnits(t *testing.T) {
	// No measurand means Energy.Active.Import.Register; Wh passes through.
	rows := Map(mv(contracts.MeterSample{Value: 1500.4, Unit: "Wh"}, contracts.MeterSample{Measurand: MeasurandPower, Value: 11000, Unit: "W"}), noUnknown(t))
	if *rows[0].EnergyWh != 1500 || *rows[0].PowerW != 11000 {
		t.Fatalf("got e=%d p=%d", *rows[0].EnergyWh, *rows[0].PowerW)
	}
	if rows[0].CurrentA != nil || rows[0].VoltageV != nil || rows[0].SocPct != nil {
		t.Fatal("missing measurands must be NULL")
	}
}

func TestMapPhases(t *testing.T) {
	rows := Map(mv(
		contracts.MeterSample{Measurand: MeasurandPower, Value: 3000, Unit: "W", Phase: "L1"},
		contracts.MeterSample{Measurand: MeasurandPower, Value: 3500, Unit: "W", Phase: "L2"},
		contracts.MeterSample{Measurand: MeasurandCurrent, Value: 14, Unit: "A", Phase: "L1"},
		contracts.MeterSample{Measurand: MeasurandCurrent, Value: 16, Unit: "A", Phase: "L2"},
		contracts.MeterSample{Measurand: MeasurandVoltage, Value: 230, Unit: "V", Phase: "L1-N"},
		contracts.MeterSample{Measurand: MeasurandVoltage, Value: 232, Unit: "V", Phase: "L2-N"},
	), noUnknown(t))
	r := rows[0]
	if *r.PowerW != 6500 || *r.CurrentA != 16 || *r.VoltageV != 231 {
		t.Fatalf("phase aggregation: p=%d c=%v v=%v", *r.PowerW, *r.CurrentA, *r.VoltageV)
	}
	// An all-phase sample wins over per-phase ones.
	rows = Map(mv(
		contracts.MeterSample{Measurand: MeasurandPower, Value: 3000, Unit: "W", Phase: "L1"},
		contracts.MeterSample{Measurand: MeasurandPower, Value: 9000, Unit: "W"},
	), noUnknown(t))
	if *rows[0].PowerW != 9000 {
		t.Fatalf("total power = %d", *rows[0].PowerW)
	}
}

func TestMapUnknownDropped(t *testing.T) {
	var unknown []string
	rows := Map(mv(
		contracts.MeterSample{Measurand: "Temperature", Value: 40, Unit: "Celsius"},
		contracts.MeterSample{Measurand: MeasurandPower, Value: 1, Unit: "VA"}, // wrong unit
		contracts.MeterSample{Measurand: MeasurandEnergy, Value: 10, Unit: "Wh"},
	), func(m string) { unknown = append(unknown, m) })
	if len(unknown) != 2 || unknown[0] != "Temperature" || unknown[1] != MeasurandPower {
		t.Fatalf("unknown = %v", unknown)
	}
	if len(rows) != 1 || rows[0].PowerW != nil || *rows[0].EnergyWh != 10 {
		t.Fatalf("rows = %+v", rows)
	}
	// Only unknown samples: no row at all.
	rows = Map(mv(contracts.MeterSample{Measurand: "Frequency", Value: 50, Unit: "Hz"}), func(string) {})
	if len(rows) != 0 {
		t.Fatalf("expected no row, got %d", len(rows))
	}
}

func TestMapMultipleReadingsAndReplayed(t *testing.T) {
	m := mv(contracts.MeterSample{Measurand: MeasurandPower, Value: 1000, Unit: "W"})
	m.Replayed = true
	m.Readings = append(m.Readings, contracts.MeterReading{Timestamp: t0.Add(30 * time.Second), Samples: []contracts.MeterSample{{Measurand: MeasurandPower, Value: 2000, Unit: "W"}}})
	rows := Map(m, noUnknown(t))
	if len(rows) != 2 || !rows[0].Replayed || !rows[1].Replayed || *rows[1].PowerW != 2000 {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestKnownMeasurandLabel(t *testing.T) {
	if KnownMeasurandLabel("Temperature") != "Temperature" || KnownMeasurandLabel("x; DROP") != "other" {
		t.Fatal("label bounding")
	}
}

func TestDecode(t *testing.T) {
	good := contracts.Envelope[contracts.MeterValues]{EventID: "e", Type: contracts.EventTypeMeterValues, Version: 1, OccurredAt: t0, Producer: "gw", Key: "st-1",
		Data: mv(contracts.MeterSample{Measurand: MeasurandPower, Value: 1, Unit: "W"})}
	b, _ := json.Marshal(good)
	if _, err := Decode(b); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*contracts.Envelope[contracts.MeterValues]){
		"type":      func(e *contracts.Envelope[contracts.MeterValues]) { e.Type = "station.connected" },
		"station":   func(e *contracts.Envelope[contracts.MeterValues]) { e.Data.StationID = "" },
		"connector": func(e *contracts.Envelope[contracts.MeterValues]) { e.Data.ConnectorID = 40000 },
		"nul":       func(e *contracts.Envelope[contracts.MeterValues]) { e.Data.StationID = "st-\x00x" },
		"long": func(e *contracts.Envelope[contracts.MeterValues]) {
			e.Data.StationID = strings.Repeat("s", MaxStationIDLen+1)
		},
		"readings": func(e *contracts.Envelope[contracts.MeterValues]) { e.Data.Readings = nil },
	}
	for name, mut := range cases {
		e := good
		e.Data.Readings = append([]contracts.MeterReading{}, good.Data.Readings...)
		mut(&e)
		b, _ := json.Marshal(e)
		if _, err := Decode(b); !errors.Is(err, ErrDecode) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := Decode([]byte("{not json")); !errors.Is(err, ErrDecode) {
		t.Fatal("bad json must be ErrDecode")
	}
}

func TestNewestLive(t *testing.T) {
	p := func(v int32) *int32 { return &v }
	rows := []Row{
		{Time: t0, StationID: "a", ConnectorID: 1, PowerW: p(1)},
		{Time: t0.Add(2 * time.Second), StationID: "a", ConnectorID: 1, PowerW: p(3)},
		{Time: t0.Add(time.Second), StationID: "a", ConnectorID: 1, PowerW: p(2)},
		{Time: t0.Add(time.Hour), StationID: "a", ConnectorID: 1, PowerW: p(9), Replayed: true},
		{Time: t0, StationID: "a", ConnectorID: 2},
		{Time: t0, StationID: "b", ConnectorID: 1, PowerW: p(5)},
	}
	live := NewestLive(rows)
	if len(live) != 2 || *live[0].PowerW != 3 || live[1].StationID != "b" {
		t.Fatalf("live = %+v", live)
	}
}

func TestIngestURL(t *testing.T) {
	u, err := IngestURL("postgres://u:p@h:5432/telemetry?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if u != "postgres://u:p@h:5432/telemetry?sslmode=disable&synchronous_commit=off" {
		t.Fatalf("url = %s", u)
	}
}

func TestPowerKey(t *testing.T) {
	if PowerKey("CP-1", 2) != "power:CP-1:2" {
		t.Fatal(PowerKey("CP-1", 2))
	}
}
