// Package gen generates deterministic meter readings with the same physics
// as the station simulator (ADR 0013): AC flat power, DC SoC taper, a daily
// utilisation profile and ~45 min sessions. The fleet layout follows the
// shared convention in packages/contracts/src/fleet.ts.
package gen

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
)

// StationsPerSite is the site layout shared with sessions and the simulator.
const StationsPerSite = 10

// ConnectorsPerStation is fixed by the fleet convention.
const ConnectorsPerStation = 2

// Fleet convention constants, mirrored from packages/contracts/src/fleet.ts and checked against
// packages/contracts/schemas/fleet-fixture.json by TestFleetFixture.
const (
	ACMinW = 6 * 230 * 3
	DCMinW = 5000
	// DefaultDCShare is the lab fleet DC share (docker-compose SIM_DC_SHARE and SEED_DC_SHARE).
	DefaultDCShare = 0.2
)

var (
	acPowersW = [...]int{11_000, 22_000}
	dcPowersW = [...]int{50_000, 75_000, 100_000, 150_000}
	tariffIDs = [...]string{"std", "tou", "fleet"}
)

// Kind is the charger type.
type Kind uint8

// Charger kinds.
const (
	AC Kind = iota
	DC
)

// Station describes one station by its 0-based global index.
type Station struct {
	Index int
	ID    string // st-<site#4>-<n#3>, both 1-based
	Site  string // site-<#4>
	Kind  Kind
	MaxW  int // connector maximum power
}

// StationID returns the id for a 0-based station index.
func StationID(i int) string {
	return fmt.Sprintf("st-%04d-%03d", i/StationsPerSite+1, i%StationsPerSite+1)
}

// SiteID returns the site id for a 0-based station index.
func SiteID(i int) string { return fmt.Sprintf("site-%04d", i/StationsPerSite+1) }

// Unit returns a uniform value in [0, 1) from the first 32 bits of SHA-256(s).
func Unit(s string) float64 {
	h := sha256.Sum256([]byte(s))
	return float64(binary.BigEndian.Uint32(h[:4])) / 4294967296
}

// IsDC reports whether station g is DC: the running DC count steps up at g.
func IsDC(g int, dcShare float64) bool {
	return math.Floor(float64(g+1)*dcShare) > math.Floor(float64(g)*dcShare)
}

// SecretSHA256 is hex(sha256(hex(HMAC-SHA256(seed, stationID))[:32])).
func SecretSHA256(seed, stationID string) string {
	m := hmac.New(sha256.New, []byte(seed))
	m.Write([]byte(stationID))
	secret := hex.EncodeToString(m.Sum(nil))[:32]
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// TariffID is the tariff of connector connID (1-based) of station g.
func TariffID(g, connID int) string { return tariffIDs[(2*g+connID-1)%len(tariffIDs)] }

// SiteCapW is 40-60% of the summed connector maximum, rounded down to 1 kW.
func SiteCapW(siteID string, sumMaxW int) int {
	fraction := 0.4 + 0.2*Unit(siteID+":cap")
	return int(math.Floor(float64(sumMaxW)*fraction/1000)) * 1000
}

// NewStation derives the deterministic station shape from the fleet convention.
func NewStation(i int, dcShare float64) Station {
	s := Station{Index: i, ID: StationID(i), Site: SiteID(i)}
	if IsDC(i, dcShare) {
		s.Kind = DC
		s.MaxW = dcPowersW[i%len(dcPowersW)]
	} else {
		s.Kind = AC
		s.MaxW = acPowersW[i%len(acPowersW)]
	}
	return s
}
