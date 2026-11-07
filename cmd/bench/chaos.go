package main

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"time"

	"notifplatform/internal/bench"
)

// Kinds of failure the reliability run can inject by killing a container.
const (
	chaosRelay    = "relay"
	chaosConsumer = "consumer"
	chaosPostgres = "postgres"
)

// chaosEvent is one planned container kill, as an offset from the start of the load.
type chaosEvent struct {
	Kind string
	At   time.Duration
}

// planChaos schedules the relay, consumer and platform-Postgres kills of one run at random
// times inside the middle 80% of the load, and returns them in time order. Each kind keeps
// its own minimum spacing (a relay or a consumer comes back in seconds; Postgres needs
// crash recovery, so its kills are spread further apart).
func planChaos(rng *rand.Rand, window time.Duration, relayKills, consumerKills, postgresKills int) []chaosEvent {
	var plan []chaosEvent
	add := func(kind string, n int, minGap time.Duration) {
		if n <= 0 {
			return
		}
		for _, at := range bench.CrashTimes(rng, n, window, minGap) {
			plan = append(plan, chaosEvent{Kind: kind, At: at})
		}
	}
	add(chaosRelay, relayKills, 45*time.Second)
	add(chaosConsumer, consumerKills, 30*time.Second)
	add(chaosPostgres, postgresKills, 90*time.Second)
	slices.SortStableFunc(plan, func(a, b chaosEvent) int { return cmp.Compare(a.At, b.At) })
	return plan
}
