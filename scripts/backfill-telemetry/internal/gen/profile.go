package gen

import "time"

// hourShape is the relative demand per UTC hour of day: quiet night, a flat
// daytime, and an evening peak at 18-20.
var hourShape = [24]float64{
	0.04, 0.04, 0.04, 0.04, 0.04, 0.04, // 00-05
	0.10, 0.10, // 06-07
	0.10, 0.10, 0.10, 0.10, 0.10, 0.10, 0.10, 0.10, // 08-15
	0.40, 0.70, // 16-17
	1.00, 1.00, 1.00, // 18-20 peak
	0.50, 0.20, 0.08, // 21-23
}

// MeanUtilisation is the daily mean connector occupancy.
const MeanUtilisation = 0.10

// Occupancy returns the target probability that a connector is in a session
// during the given UTC hour. Mean over the day is MeanUtilisation; the peak
// is about 0.39.
func Occupancy(hour int) float64 {
	var sum float64
	for _, v := range hourShape {
		sum += v
	}
	return hourShape[hour] * MeanUtilisation * 24 / sum
}

// startProb is the probability that an idle connector starts a session in a
// step of dt, so that steady-state occupancy equals Occupancy(hour) for a
// mean session length of meanMin minutes: p = λD/(1+λD).
func startProb(hour int, meanMin float64, dt time.Duration) float64 {
	p := Occupancy(hour)
	lambdaPerMin := p / (meanMin * (1 - p))
	return lambdaPerMin * dt.Minutes()
}
