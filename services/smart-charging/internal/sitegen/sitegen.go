// Package sitegen builds realistic AllocateReq sites from the ADR 0000 fleet
// model (5-40 stations × 2 connectors, 80% AC 11/22 kW, 20% DC 50-150 kW,
// 10-60% utilisation, undersized grid connections). The benchmarks and the
// allocbench load tool share it.
package sitegen

import (
	"fmt"
	"math/rand/v2"
	"time"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

// Volts is the nominal phase voltage; MinAmps the IEC 61851 minimum current.
const (
	Volts   = 230
	MinAmps = 6
	// StartW3Phase is 6 A × 230 V × 3, the start profile ceiling (ADR 0006).
	StartW3Phase = MinAmps * Volts * 3
)

// Site returns one site with id siteID, sessions started before now.
func Site(r *rand.Rand, siteID string, now time.Time) contracts.AllocateReq {
	stations := 5 + r.IntN(36) // 5..40 → 10..80 connectors
	connectors := 2 * stations
	type conn struct {
		station string
		id      int
		maxW    int64
		dc      bool
	}
	all := make([]conn, 0, connectors)
	var nameplate int64
	for s := range stations {
		dc := r.Float64() < 0.2
		var maxW int64
		switch {
		case dc:
			maxW = int64(50_000 + 25_000*r.IntN(5)) // 50..150 kW
		case r.Float64() < 0.5:
			maxW = 11_000
		default:
			maxW = 22_000
		}
		for c := 1; c <= 2; c++ {
			all = append(all, conn{station: fmt.Sprintf("%s-st%02d", siteID, s), id: c, maxW: maxW, dc: dc})
			nameplate += maxW
		}
	}
	// Grid connection sized at 25-70% of nameplate.
	capW := nameplate * int64(25+r.IntN(46)) / 100
	startW := min(int64(StartW3Phase), capW/int64(connectors))

	r.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	active := max(1, connectors*(10+r.IntN(51))/100)
	sessions := make([]contracts.AllocateSession, active)
	for i := range sessions {
		c := all[i]
		var minW, maxW int64
		if c.dc {
			minW, maxW = 5_000, min(c.maxW, int64(40_000+10_000*r.IntN(12)))
		} else {
			phases := int64(3)
			if r.Float64() < 0.2 {
				phases = 1
			}
			minW = MinAmps * Volts * phases
			vehicle := phases * Volts * int64([]int{16, 32}[r.IntN(2)])
			maxW = max(minW, min(c.maxW, vehicle))
		}
		s := contracts.AllocateSession{
			SessionID: uuid(r), StationID: c.station, ConnectorID: c.id,
			MaxW: maxW, MinW: minW,
			StartedAt: now.Add(-time.Duration(r.IntN(3*3600)) * time.Second).UTC(),
		}
		if r.Float64() < 0.1 {
			s.Priority = 1 + r.IntN(10)
		}
		if r.Float64() < 0.7 {
			m := maxW * int64(30+r.IntN(71)) / 100
			s.MeasuredW = &m
		}
		sessions[i] = s
	}
	at := now.UTC()
	return contracts.AllocateReq{
		SiteID: siteID, SiteCapW: capW, IdleConnectors: int64(connectors - active), StartW: startW,
		ComputedAt: &at, Sessions: sessions,
	}
}

func uuid(r *rand.Rand) string {
	a, b := r.Uint64(), r.Uint64()
	a = a&^0xf000 | 0x4000         // version 4
	b = b&^(0xc<<60) | (0x8 << 60) // RFC 4122 variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", a>>32, (a>>16)&0xffff, a&0xffff, b>>48, b&0xffffffffffff)
}
