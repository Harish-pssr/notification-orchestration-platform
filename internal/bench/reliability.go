package bench

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Counts is the exact state of both databases after a run (D17). Everything here is
// counted from rows, never estimated.
type Counts struct {
	Events          int64            `json:"events"`
	Deliveries      int64            `json:"deliveries"`
	ByStatus        map[string]int64 `json:"deliveries_by_status"`
	Delivered       int64            `json:"delivered"`
	DeadLettered    int64            `json:"dead_lettered"`
	NotFinished     int64            `json:"not_finished"`         // neither DELIVERED nor DLQ
	Retried         int64            `json:"retried_deliveries"`   // deliveries that needed more than one attempt
	IgnoredByConsum int64            `json:"ignored_by_consumers"` // duplicates a consumer recognised as already delivered

	ProviderRequests    int64 `json:"provider_requests"`
	DistinctRequestKeys int64 `json:"provider_distinct_request_keys"`
	Effects             int64 `json:"provider_effects"`
	DistinctEffectKeys  int64 `json:"provider_distinct_effect_keys"`
	InjectedFailures    int64 `json:"provider_injected_failures"` // requests answered with an injected error

	// Requests that were answered with an injected error are retries, not duplicates, so
	// they are subtracted from both.
	DuplicateDeliveries  int64 `json:"duplicate_deliveries"`   // requests - injected failures - distinct request keys
	DuplicateSideEffects int64 `json:"duplicate_side_effects"` // effects minus distinct effect keys
	SuppressedByProvider int64 `json:"suppressed_by_provider"` // requests - injected failures - effects
}

// ReadCounts reads the platform and provider databases.
func ReadCounts(ctx context.Context, platform, provider *pgxpool.Pool) (Counts, error) {
	c := Counts{ByStatus: map[string]int64{}}
	if err := platform.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&c.Events); err != nil {
		return c, fmt.Errorf("count events: %w", err)
	}
	rows, err := platform.Query(ctx, `SELECT status, count(*) FROM channel_deliveries GROUP BY status`)
	if err != nil {
		return c, fmt.Errorf("count deliveries: %w", err)
	}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return c, err
		}
		c.ByStatus[status] = n
		c.Deliveries += n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return c, err
	}
	c.Delivered, c.DeadLettered = c.ByStatus["DELIVERED"], c.ByStatus["DLQ"]
	c.NotFinished = c.Deliveries - c.Delivered - c.DeadLettered

	if err := platform.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE attempts > 1), coalesce(sum(duplicate_count), 0) FROM channel_deliveries`).
		Scan(&c.Retried, &c.IgnoredByConsum); err != nil {
		return c, fmt.Errorf("count retries: %w", err)
	}
	if err := provider.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM requests), (SELECT count(DISTINCT key) FROM requests),
		       (SELECT count(*) FROM effects),  (SELECT count(DISTINCT key) FROM effects),
		       (SELECT count(*) FROM injected_failures)`).
		Scan(&c.ProviderRequests, &c.DistinctRequestKeys, &c.Effects, &c.DistinctEffectKeys, &c.InjectedFailures); err != nil {
		return c, fmt.Errorf("count provider rows: %w", err)
	}
	c.DuplicateDeliveries = c.ProviderRequests - c.InjectedFailures - c.DistinctRequestKeys
	c.DuplicateSideEffects = c.Effects - c.DistinctEffectKeys
	c.SuppressedByProvider = c.ProviderRequests - c.InjectedFailures - c.Effects
	return c, nil
}

// ReliabilityVerdict is whether a run met its pass criteria, and every criterion that failed.
type ReliabilityVerdict struct {
	Pass     bool     `json:"pass"`
	Failures []string `json:"failures,omitempty"`
}

// JudgeReliability applies the pass criteria of D17. accepted is how many events the API
// acknowledged; perEvent is how many channel deliveries each event has.
//
// A normal run passes only if nothing was lost and nothing happened twice: every
// accepted event is stored, every delivery is DELIVERED, none is dead-lettered, there is
// exactly one provider effect per delivery, and no duplicate side effect exists.
//
// The negative control (protections off) must instead show duplicate side effects:
// otherwise the counters cannot be trusted to see them. It must still lose nothing.
func JudgeReliability(accepted int, perEvent int, c Counts, control bool) ReliabilityVerdict {
	return JudgeReliabilityUnknown(accepted, 0, perEvent, c, control)
}

// JudgeReliabilityUnknown is JudgeReliability for a run in which the API itself was made
// unavailable (the platform Postgres was killed). A request that failed with an error may
// still have committed before the reply was lost, so its outcome is unknown to the client:
// unknown is how many such requests there were. The number of stored events must then lie
// between accepted and accepted+unknown. Every stored event must still be fully delivered
// exactly once, so nothing else is relaxed.
func JudgeReliabilityUnknown(accepted, unknown, perEvent int, c Counts, control bool) ReliabilityVerdict {
	var fails []string
	add := func(format string, args ...any) { fails = append(fails, fmt.Sprintf(format, args...)) }

	switch {
	case c.Events < int64(accepted):
		add("%d events accepted by the API but only %d stored", accepted, c.Events)
	case c.Events > int64(accepted+unknown):
		add("%d events accepted by the API (and %d with an unknown outcome) but %d stored", accepted, unknown, c.Events)
	}
	if want := c.Events * int64(perEvent); c.Deliveries != want {
		add("%d deliveries exist, want %d (%d per stored event)", c.Deliveries, want, perEvent)
	}
	if c.NotFinished != 0 {
		add("%d deliveries are neither delivered nor dead-lettered", c.NotFinished)
	}
	if control {
		if c.DuplicateSideEffects == 0 {
			add("the negative control produced no duplicate side effects, so the counters have not been shown to detect them")
		}
		if c.DeadLettered != 0 {
			add("%d deliveries were dead-lettered", c.DeadLettered)
		}
		return ReliabilityVerdict{Pass: len(fails) == 0, Failures: fails}
	}
	if c.DeadLettered != 0 {
		add("%d deliveries were dead-lettered", c.DeadLettered)
	}
	if c.DuplicateSideEffects != 0 {
		add("%d duplicate side effects", c.DuplicateSideEffects)
	}
	if c.Effects != c.Delivered {
		add("%d provider effects for %d delivered deliveries (want exactly one each)", c.Effects, c.Delivered)
	}
	return ReliabilityVerdict{Pass: len(fails) == 0, Failures: fails}
}

// CrashTimes returns n random offsets inside the middle 80% of window, in order and at
// least minGap apart, so each crash happens while the load is steady and the system has
// time to recover before the next. If random placement cannot satisfy the gap it falls
// back to evenly spaced offsets, which is always valid when the window is long enough.
func CrashTimes(rng *rand.Rand, n int, window, minGap time.Duration) []time.Duration {
	lo, hi := window/10, window*9/10
	span := hi - lo
	for attempt := 0; attempt < 1000; attempt++ {
		ts := make([]time.Duration, n)
		for i := range ts {
			ts[i] = lo + time.Duration(rng.Int64N(int64(span)))
		}
		slices.Sort(ts)
		ok := true
		for i := 1; i < n; i++ {
			if ts[i]-ts[i-1] < minGap {
				ok = false
				break
			}
		}
		if ok {
			return ts
		}
	}
	ts := make([]time.Duration, n)
	for i := range ts {
		ts[i] = lo + span*time.Duration(i+1)/time.Duration(n+1)
	}
	return ts
}
