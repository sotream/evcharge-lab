package alloc

import (
	"fmt"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

// Input bounds. They keep every intermediate product far inside int64
// (MaxSessions × MaxPowerW ≈ 10^12, MaxIdleConnectors × MaxPowerW ≈ 10^14)
// and are generous against ADR 0000 (≤ 80 connectors, ≤ 150 kW per connector).
const (
	MaxSessions       = 1000
	MaxIdleConnectors = 100_000
	MaxPowerW         = 1_000_000_000      // 1 GW per connector / startW
	MaxSiteCapW       = 10_000_000_000_000 // 10 TW
	MaxPriority       = 10
	MaxIDLen          = 128
)

// Validate returns every semantic problem with req as "path: message".
// Shape problems (types, missing fields) are the decoder's job.
func Validate(req *contracts.AllocateReq) []string {
	var p []string
	add := func(path, format string, args ...any) {
		p = append(p, path+": "+fmt.Sprintf(format, args...))
	}
	if req.SiteID == "" || len(req.SiteID) > MaxIDLen {
		add("siteId", "must be 1..%d characters", MaxIDLen)
	}
	if req.SiteCapW < 0 || req.SiteCapW > MaxSiteCapW {
		add("siteCapW", "must be in 0..%d", int64(MaxSiteCapW))
	}
	if req.IdleConnectors < 0 || req.IdleConnectors > MaxIdleConnectors {
		add("idleConnectors", "must be in 0..%d", MaxIdleConnectors)
	}
	if req.StartW < 0 || req.StartW > MaxPowerW {
		add("startW", "must be in 0..%d", MaxPowerW)
	}
	if len(req.Sessions) > MaxSessions {
		add("sessions", "at most %d sessions", MaxSessions)
		return p // do not walk an oversized list
	}
	seen := make(map[string]int, len(req.Sessions))
	for i := range req.Sessions {
		s := &req.Sessions[i]
		at := fmt.Sprintf("sessions[%d]", i)
		if !isUUID(s.SessionID) {
			add(at+".sessionId", "must be a UUID")
		} else if j, dup := seen[s.SessionID]; dup {
			add(at+".sessionId", "duplicates sessions[%d]", j)
		} else {
			seen[s.SessionID] = i
		}
		if s.StationID == "" || len(s.StationID) > MaxIDLen {
			add(at+".stationId", "must be 1..%d characters", MaxIDLen)
		}
		if s.MaxW < 0 || s.MaxW > MaxPowerW {
			add(at+".maxW", "must be in 0..%d", MaxPowerW)
		}
		if s.MinW < 0 || s.MinW > MaxPowerW {
			add(at+".minW", "must be in 0..%d", MaxPowerW)
		} else if s.MinW > s.MaxW {
			add(at+".minW", "must not exceed maxW (%d > %d)", s.MinW, s.MaxW)
		}
		if s.MeasuredW != nil && (*s.MeasuredW < 0 || *s.MeasuredW > MaxPowerW) {
			add(at+".measuredW", "must be in 0..%d", MaxPowerW)
		}
		if s.Priority < 0 || s.Priority > MaxPriority {
			add(at+".priority", "must be in 0..%d", MaxPriority)
		}
		if s.StartedAt.IsZero() {
			add(at+".startedAt", "is required")
		}
	}
	return p
}

// isUUID accepts the canonical 8-4-4-4-12 hex form.
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !isHex(c) {
				return false
			}
		}
	}
	return true
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}
