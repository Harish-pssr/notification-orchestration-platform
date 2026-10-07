// Package backoff computes retry delays: capped exponential growth with equal
// jitter (D7).
package backoff

import "time"

// Delay returns how long to wait before retry number attempt (1 = first retry).
//
// The un-jittered delay is base * 2^(attempt-1), capped at max. Equal jitter then
// returns half of it plus a random amount up to the other half, so retries from
// many workers spread out but never wait less than half the intended delay.
//
// rnd must return a value in [0, 1). It is a parameter so tests can be
// deterministic; production passes math/rand/v2's Float64.
func Delay(attempt int, base, max time.Duration, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	return half + time.Duration(rnd()*float64(d-half))
}
