// Package pricing is the deterministic price calculation of ADR 0011: a pure
// function of (tariff version, request) in int64 minor units.
package pricing

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	// Embedded IANA database as a fallback when the host has no zoneinfo
	// (LoadLocation prefers ZONEINFO and the system database).
	_ "time/tzdata"
)

// Window is a time-of-use energy price window in the tariff's timezone.
// Days names the local weekdays on which the window starts ("mon".."sun";
// empty means every day). To <= From wraps past midnight into the next day.
// "24:00" is accepted as To.
type Window struct {
	Days             []string `json:"days,omitempty"`
	From             string   `json:"from"`
	To               string   `json:"to"`
	PriceMinorPerKWh int64    `json:"price_minor_per_kwh"`
	Label            string   `json:"label,omitempty"`

	dayMask  uint8 // bit time.Weekday
	fromMin  int   // minutes after local midnight, 0..1439
	toMin    int   // 1..1440
	wrapping bool
}

// Version is one tariff version (ADR 0011). A version applies to every
// session whose startedAt is >= ValidFrom and before the next version.
type Version struct {
	TariffID               string
	Version                int
	ValidFrom              time.Time
	Currency               string
	Timezone               string
	SessionFeeMinor        int64
	EnergyPriceMinorPerKWh int64
	Windows                []Window
	TimePriceMinorPerHour  int64
	IdleFeeMinorPerMin     int64
	IdleGraceMin           int64
	MinBillableWh          int64

	loc *time.Location
}

// Tariff is a tariff id with its versions sorted by ValidFrom ascending.
type Tariff struct {
	ID       string
	Versions []*Version
}

var (
	// ErrInvalidTariff reports a tariff definition that cannot be priced.
	ErrInvalidTariff = errors.New("invalid tariff")
	// ErrNoVersion means no version is in force at the requested instant.
	ErrNoVersion = errors.New("no tariff version in force")
)

var (
	currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)
	hhmmRe     = regexp.MustCompile(`^([01]\d|2[0-3]):([0-5]\d)$|^24:00$`)
	weekdays   = map[string]time.Weekday{
		"sun": time.Sunday, "mon": time.Monday, "tue": time.Tuesday, "wed": time.Wednesday,
		"thu": time.Thursday, "fri": time.Friday, "sat": time.Saturday,
	}
)

func parseHHMM(s string) (int, bool) {
	if !hhmmRe.MatchString(s) {
		return 0, false
	}
	var h, m int
	_, _ = fmt.Sscanf(s, "%d:%d", &h, &m)
	return h*60 + m, true
}

// Prepare validates v and precomputes the location and window bounds. It must
// be called once before Quote.
func (v *Version) Prepare() error {
	var probs []string
	if v.TariffID == "" {
		probs = append(probs, "tariff id is empty")
	}
	if !currencyRe.MatchString(v.Currency) {
		probs = append(probs, fmt.Sprintf("currency %q is not ISO 4217", v.Currency))
	}
	loc, err := time.LoadLocation(v.Timezone)
	if err != nil || v.Timezone == "" || v.Timezone == "Local" {
		probs = append(probs, fmt.Sprintf("timezone %q is not an IANA zone", v.Timezone))
	}
	v.loc = loc
	for name, x := range map[string]int64{
		"session_fee_minor": v.SessionFeeMinor, "energy_price_minor_per_kwh": v.EnergyPriceMinorPerKWh,
		"time_price_minor_per_hour": v.TimePriceMinorPerHour, "idle_fee_minor_per_min": v.IdleFeeMinorPerMin,
		"idle_grace_min": v.IdleGraceMin, "min_billable_wh": v.MinBillableWh,
	} {
		if x < 0 || x > maxPrice {
			probs = append(probs, fmt.Sprintf("%s must be in 0..%d", name, int64(maxPrice)))
		}
	}
	for i := range v.Windows {
		w := &v.Windows[i]
		from, ok1 := parseHHMM(w.From)
		to, ok2 := parseHHMM(w.To)
		switch {
		case !ok1 || !ok2 || from == 1440:
			probs = append(probs, fmt.Sprintf("window %d: from/to must be HH:MM (to may be 24:00)", i))
			continue
		case from == to:
			probs = append(probs, fmt.Sprintf("window %d: from == to", i))
			continue
		}
		if w.PriceMinorPerKWh < 0 || w.PriceMinorPerKWh > maxPrice {
			probs = append(probs, fmt.Sprintf("window %d: price must be in 0..%d", i, int64(maxPrice)))
		}
		w.fromMin, w.toMin, w.wrapping = from, to, to < from
		if to == 0 {
			w.toMin, w.wrapping = 1440, false
		}
		w.dayMask = 0
		if len(w.Days) == 0 {
			w.dayMask = 0x7f
		}
		for _, d := range w.Days {
			wd, ok := weekdays[strings.ToLower(d)]
			if !ok {
				probs = append(probs, fmt.Sprintf("window %d: unknown day %q", i, d))
				continue
			}
			w.dayMask |= 1 << wd
		}
	}
	if len(probs) > 0 {
		return fmt.Errorf("%w %s v%d: %s", ErrInvalidTariff, v.TariffID, v.Version, strings.Join(probs, "; "))
	}
	return nil
}

// Location is the prepared tariff timezone.
func (v *Version) Location() *time.Location { return v.loc }

// NewTariff validates and sorts versions.
func NewTariff(id string, versions []*Version) (*Tariff, error) {
	if len(versions) == 0 {
		return nil, fmt.Errorf("%w %s: no versions", ErrInvalidTariff, id)
	}
	for _, v := range versions {
		if v.TariffID != id {
			return nil, fmt.Errorf("%w: version of %s under %s", ErrInvalidTariff, v.TariffID, id)
		}
		if err := v.Prepare(); err != nil {
			return nil, err
		}
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].ValidFrom.Before(versions[j].ValidFrom) })
	return &Tariff{ID: id, Versions: versions}, nil
}

// At returns the version in force at ts: the latest with ValidFrom <= ts.
func (t *Tariff) At(ts time.Time) (*Version, error) {
	i := sort.Search(len(t.Versions), func(i int) bool { return t.Versions[i].ValidFrom.After(ts) })
	if i == 0 {
		return nil, fmt.Errorf("%w: %s at %s", ErrNoVersion, t.ID, ts.UTC().Format(time.RFC3339))
	}
	return t.Versions[i-1], nil
}

func (w *Window) describe() string {
	days := "daily"
	if len(w.Days) > 0 && w.dayMask != 0x7f {
		days = strings.Join(w.Days, ",")
	}
	s := fmt.Sprintf("%s %s-%s", days, w.From, w.To)
	if w.Label != "" {
		s = w.Label + " " + s
	}
	return s
}
