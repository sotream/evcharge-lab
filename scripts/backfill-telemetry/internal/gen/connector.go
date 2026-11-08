package gen

import (
	"hash/fnv"
	"math"
	"math/rand/v2"
	"time"
)

// Step is the reading interval of an active connector.
const Step = 30 * time.Second

// Row is one meter_readings row. SocPct is nil for AC connectors.
type Row struct {
	Time          time.Time
	StationID     string
	ConnectorID   int16
	TransactionID int64
	EnergyWh      int64
	PowerW        int32
	CurrentA      float32
	VoltageV      float32
	SocPct        *float32
}

type session struct {
	txID    int64
	end     time.Time
	vehMaxW float64
	// DC only
	soc, target, batteryWh float64
}

// Connector is a resumable generator: Advance may be called for consecutive
// time windows and yields the same readings however the range is sliced.
type Connector struct {
	st     Station
	connID int16
	rng    *rand.Rand
	offset time.Duration // grid phase within Step
	energy float64       // Wh register
	seq    int64
	cur    *session
	next   time.Time // next grid tick not yet processed (zero until first Advance)
}

// NewConnector creates the generator for connector connID (1-based) of st.
func NewConnector(st Station, connID int16, seed uint64) *Connector {
	h := fnv.New64a()
	_, _ = h.Write([]byte(st.ID))
	k := h.Sum64() ^ (seed * 0x9E3779B97F4A7C15) ^ uint64(connID)<<56
	rng := rand.New(rand.NewPCG(k, k>>7|1))
	return &Connector{
		st: st, connID: connID, rng: rng,
		offset: time.Duration(rng.Int64N(int64(Step))),
		energy: float64(rng.Int64N(2_000_000)), // register starts anywhere below 2 MWh
	}
}

func (c *Connector) meanMin() float64 {
	if c.st.Kind == DC {
		return 25
	}
	return 45
}

func (c *Connector) firstTick(from time.Time) time.Time {
	base := from.Truncate(Step).Add(c.offset)
	if base.Before(from) {
		base = base.Add(Step)
	}
	return base
}

func (c *Connector) startSession(t time.Time) {
	c.seq++
	s := &session{txID: (int64(c.st.Index)*int64(ConnectorsPerStation)+int64(c.connID))*1_000_000 + c.seq}
	if c.st.Kind == DC {
		vmax := 50_000 + c.rng.Float64()*100_000
		s.vehMaxW = math.Min(float64(c.st.MaxW), vmax)
		s.batteryWh = 40_000 + c.rng.Float64()*60_000
		s.soc = 10 + c.rng.Float64()*40
		s.target = 80 + c.rng.Float64()*20
		s.end = t.Add(time.Duration((15 + c.rng.Float64()*20) * float64(time.Minute)))
	} else {
		veh := [...]float64{7_400, 11_000, 22_000}[c.rng.IntN(3)]
		s.vehMaxW = math.Min(float64(c.st.MaxW), veh)
		s.end = t.Add(time.Duration((20 + c.rng.Float64()*50) * float64(time.Minute)))
	}
	c.cur = s
}

// DCPower is the DC charging curve: full power up to 80% SoC, then a linear
// taper to 10% of full power at 100%.
func DCPower(maxW, soc float64) float64 {
	if soc <= 80 {
		return maxW
	}
	if soc >= 100 {
		return 0.1 * maxW
	}
	return maxW * (1 - 0.9*(soc-80)/20)
}

// Advance emits the readings with from <= time < to.
func (c *Connector) Advance(from, to time.Time, emit func(Row)) {
	t := c.next
	if t.IsZero() || t.Before(from) {
		t = c.firstTick(from)
	}
	for ; t.Before(to); t = t.Add(Step) {
		if c.cur == nil {
			if c.rng.Float64() >= startProb(t.UTC().Hour(), c.meanMin(), Step) {
				continue
			}
			c.startSession(t)
		}
		s := c.cur
		if !t.Before(s.end) || (c.st.Kind == DC && s.soc >= s.target) {
			c.cur = nil
			continue
		}
		row := Row{Time: t, StationID: c.st.ID, ConnectorID: c.connID, TransactionID: s.txID}
		var power float64
		if c.st.Kind == DC {
			power = DCPower(s.vehMaxW, s.soc)
			volt := 350 + s.soc*0.8
			row.VoltageV = float32(volt)
			row.CurrentA = float32(power / volt)
			soc := float32(s.soc)
			row.SocPct = &soc
		} else {
			power = s.vehMaxW
			volt := 228 + c.rng.Float64()*4
			row.VoltageV = float32(volt)
			row.CurrentA = float32(power / (3 * volt))
		}
		wh := power * Step.Hours()
		c.energy += wh
		if c.st.Kind == DC {
			s.soc += wh / s.batteryWh * 100
		}
		row.PowerW = int32(math.Round(power))
		row.EnergyWh = int64(c.energy)
		emit(row)
	}
	c.next = t
}
