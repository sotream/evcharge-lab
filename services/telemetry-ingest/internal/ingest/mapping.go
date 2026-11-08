// Package ingest turns telemetry.meter-values.v1 records into wide
// meter_readings rows and writes them in batches (ADR 0008).
package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

// OCPP 1.6 measurands mapped onto the wide row. A sample without a measurand
// is Energy.Active.Import.Register (the OCPP default).
const (
	MeasurandEnergy  = "Energy.Active.Import.Register"
	MeasurandPower   = "Power.Active.Import"
	MeasurandCurrent = "Current.Import"
	MeasurandVoltage = "Voltage"
	MeasurandSoC     = "SoC"
)

// Row is one meter_readings row: one reading (timestamp) of one connector.
// Measurands missing from the reading are nil (NULL).
type Row struct {
	Time          time.Time
	StationID     string
	ConnectorID   int16
	TransactionID *int64
	EnergyWh      *int64
	PowerW        *int32
	CurrentA      *float32
	VoltageV      *float32
	SocPct        *float32
	Replayed      bool
}

// Columns is the meter_readings column order used by COPY and INSERT.
var Columns = []string{"time", "station_id", "connector_id", "transaction_id", "energy_wh", "power_w", "current_a", "voltage_v", "soc_pct", "replayed"}

// Values returns the row in Columns order.
func (r *Row) Values() []any {
	return []any{r.Time, r.StationID, r.ConnectorID, r.TransactionID, r.EnergyWh, r.PowerW, r.CurrentA, r.VoltageV, r.SocPct, r.Replayed}
}

// MaxStationIDLen caps the station id stored in meter_readings.station_id.
const MaxStationIDLen = 64

// ErrDecode marks a record that can never be ingested (bad JSON, wrong type,
// invalid fields). Such records are dead-lettered, never retried.
var ErrDecode = errors.New("undecodable meter values record")

// Decode parses an envelope value into the MeterValuesReceived payload.
func Decode(value []byte) (contracts.MeterValues, error) {
	var env contracts.Envelope[contracts.MeterValues]
	if err := json.Unmarshal(value, &env); err != nil {
		return contracts.MeterValues{}, fmt.Errorf("%w: %w", ErrDecode, err)
	}
	if env.Type != contracts.EventTypeMeterValues {
		return contracts.MeterValues{}, fmt.Errorf("%w: unexpected type %q", ErrDecode, env.Type)
	}
	mv := env.Data
	switch {
	case mv.StationID == "":
		return mv, fmt.Errorf("%w: empty stationId", ErrDecode)
	case len(mv.StationID) > MaxStationIDLen:
		return mv, fmt.Errorf("%w: stationId longer than %d bytes", ErrDecode, MaxStationIDLen)
	case strings.ContainsRune(mv.StationID, 0):
		// PostgreSQL text cannot hold NUL; one such row would fail every flush that contains it.
		return mv, fmt.Errorf("%w: stationId contains a NUL byte", ErrDecode)
	case mv.ConnectorID < 0 || mv.ConnectorID > math.MaxInt16:
		return mv, fmt.Errorf("%w: connectorId %d out of range", ErrDecode, mv.ConnectorID)
	case len(mv.Readings) == 0:
		return mv, fmt.Errorf("%w: no readings", ErrDecode)
	}
	return mv, nil
}

// phased accumulates per-phase values of one measurand next to an optional
// all-phase (no phase attribute) value.
type phased struct {
	total    float64
	hasTotal bool
	sum, max float64
	n        int
}

func (p *phased) add(v float64, phase string) {
	if phase == "" {
		p.total, p.hasTotal = v, true
		return
	}
	if p.n == 0 || v > p.max {
		p.max = v
	}
	p.sum += v
	p.n++
}

func (p *phased) ok() bool { return p.hasTotal || p.n > 0 }

// Map converts one MeterValuesReceived payload to rows, one per reading.
// Unknown measurands (or known ones with an unexpected unit) are reported to
// unknown and dropped. A reading with no usable sample yields no row.
//
// Phase handling when no all-phase sample is present: power is the sum of the
// phases, current the maximum phase current, voltage the mean phase voltage.
func Map(mv contracts.MeterValues, unknown func(measurand string)) []Row {
	rows := make([]Row, 0, len(mv.Readings))
	for _, rd := range mv.Readings {
		if rd.Timestamp.IsZero() {
			continue
		}
		var energy, power, current, voltage, soc phased
		for _, s := range rd.Samples {
			if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) {
				unknown(s.Measurand)
				continue
			}
			m := s.Measurand
			if m == "" {
				m = MeasurandEnergy
			}
			switch {
			case m == MeasurandEnergy && (s.Unit == "Wh" || s.Unit == ""):
				energy.add(s.Value, s.Phase)
			case m == MeasurandEnergy && s.Unit == "kWh":
				energy.add(s.Value*1000, s.Phase)
			case m == MeasurandPower && (s.Unit == "W" || s.Unit == ""):
				power.add(s.Value, s.Phase)
			case m == MeasurandPower && s.Unit == "kW":
				power.add(s.Value*1000, s.Phase)
			case m == MeasurandCurrent && (s.Unit == "A" || s.Unit == ""):
				current.add(s.Value, s.Phase)
			case m == MeasurandVoltage && (s.Unit == "V" || s.Unit == ""):
				voltage.add(s.Value, s.Phase)
			case m == MeasurandSoC && (s.Unit == "Percent" || s.Unit == ""):
				soc.add(s.Value, s.Phase)
			default:
				unknown(m)
			}
		}
		if !energy.ok() && !power.ok() && !current.ok() && !voltage.ok() && !soc.ok() {
			continue
		}
		row := Row{
			Time:          rd.Timestamp.UTC(),
			StationID:     mv.StationID,
			ConnectorID:   int16(mv.ConnectorID), //nolint:gosec // range checked in Decode
			TransactionID: mv.TransactionID,
			Replayed:      mv.Replayed,
		}
		if energy.ok() {
			v := int64(math.Round(pick(energy, energy.sum)))
			row.EnergyWh = &v
		}
		if power.ok() {
			v := int32(math.Round(clamp(pick(power, power.sum), math.MinInt32, math.MaxInt32)))
			row.PowerW = &v
		}
		if current.ok() {
			v := float32(pick(current, current.max))
			row.CurrentA = &v
		}
		if voltage.ok() {
			v := float32(pick(voltage, voltage.sum/float64(max(voltage.n, 1))))
			row.VoltageV = &v
		}
		if soc.ok() {
			v := float32(pick(soc, soc.max))
			row.SocPct = &v
		}
		rows = append(rows, row)
	}
	return rows
}

func pick(p phased, phaseAggregate float64) float64 {
	if p.hasTotal {
		return p.total
	}
	return phaseAggregate
}

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

// KnownMeasurandLabel bounds the label cardinality of
// telemetry_unknown_measurand_total: OCPP 1.6 measurand names are kept, any
// other string becomes "other".
func KnownMeasurandLabel(m string) string {
	switch m {
	case MeasurandEnergy, MeasurandPower, MeasurandCurrent, MeasurandVoltage, MeasurandSoC,
		"Energy.Active.Export.Register", "Energy.Reactive.Import.Register", "Energy.Reactive.Export.Register",
		"Energy.Active.Export.Interval", "Energy.Active.Import.Interval", "Energy.Reactive.Export.Interval",
		"Energy.Reactive.Import.Interval", "Power.Active.Export", "Power.Reactive.Export", "Power.Reactive.Import",
		"Power.Offered", "Power.Factor", "Current.Export", "Current.Offered", "Frequency", "Temperature", "RPM":
		return m
	}
	return "other"
}
