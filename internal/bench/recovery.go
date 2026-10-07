package bench

import (
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"strconv"
)

// Sample is one row of the one-second monitor CSV (see Monitor), on the database clock.
type Sample struct {
	T                 float64 // epoch seconds
	Events            int64
	OutboxWaiting     int64
	Delivered         int64
	DeliveriesWaiting int64
	Dead              int64
}

// ReadSamples parses a monitor CSV written by Monitor.
func ReadSamples(path string) ([]Sample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}
	var out []Sample
	for i, r := range rows {
		if i == 0 || len(r) < 7 {
			continue // header or a partial row
		}
		t, err := strconv.ParseFloat(r[0], 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		n := func(col int) int64 { v, _ := strconv.ParseInt(r[col], 10, 64); return v }
		out = append(out, Sample{T: t, Events: n(1), OutboxWaiting: n(3), Delivered: n(4), DeliveriesWaiting: n(5), Dead: n(6)})
	}
	return out, nil
}

// RecoveryResult is what one outage test measured.
type RecoveryResult struct {
	OutageS               float64 `json:"outage_s"`                    // service stopped to service started
	SteadyDeliveriesWait  float64 `json:"steady_deliveries_waiting"`   // median before the outage
	PeakOutboxWaiting     int64   `json:"peak_outbox_waiting_events"`  // events not yet published
	PeakDeliveriesWaiting int64   `json:"peak_deliveries_waiting"`     // deliveries not yet delivered
	ThresholdDeliveries   float64 `json:"recovered_below_deliveries"`  // "recovered" means at or below this
	RecoveryS             float64 `json:"recovery_s"`                  // restart until recovered; -1 if never
	PeakDeliveryRatePerS  float64 `json:"peak_delivery_rate_per_s"`    // best 5 s average after restart
	DeadAtEnd             int64   `json:"dead_lettered_by_end_of_run"` // dead letters seen in the last sample
	SamplesBeforeOutage   int     `json:"samples_before_outage"`
	SamplesAfterRestart   int     `json:"samples_after_restart"`
}

// AnalyzeRecovery reads backlog and recovery off the one-second samples (D17).
//
// stopAt and startAt are the database-clock times the service was stopped and started.
// "Recovered" is the first sample at or after startAt whose undelivered-delivery count is
// within two seconds of arrivals (the same bound D13 uses for "bounded backlog"):
// 2 * eventRate * perEvent. The peak delivery rate is the best five-second average of
// deliveries completed per second after the restart.
func AnalyzeRecovery(samples []Sample, stopAt, startAt, eventRate float64, perEvent int) RecoveryResult {
	res := RecoveryResult{OutageS: startAt - stopAt, RecoveryS: -1, ThresholdDeliveries: 2 * eventRate * float64(perEvent)}

	var before []float64
	for _, s := range samples {
		if s.T < stopAt {
			before = append(before, float64(s.DeliveriesWaiting))
			res.SamplesBeforeOutage++
		}
		if s.T >= stopAt {
			res.PeakOutboxWaiting = max(res.PeakOutboxWaiting, s.OutboxWaiting)
			res.PeakDeliveriesWaiting = max(res.PeakDeliveriesWaiting, s.DeliveriesWaiting)
		}
		res.DeadAtEnd = s.Dead
	}
	if len(before) > 0 {
		slices.Sort(before)
		res.SteadyDeliveriesWait = before[len(before)/2]
	}

	var after []Sample
	for _, s := range samples {
		if s.T >= startAt {
			after = append(after, s)
		}
	}
	res.SamplesAfterRestart = len(after)
	for _, s := range after {
		if float64(s.DeliveriesWaiting) <= res.ThresholdDeliveries {
			res.RecoveryS = s.T - startAt
			break
		}
	}
	const window = 5
	for i := window; i < len(after); i++ {
		dt := after[i].T - after[i-window].T
		if dt <= 0 {
			continue
		}
		res.PeakDeliveryRatePerS = max(res.PeakDeliveryRatePerS, float64(after[i].Delivered-after[i-window].Delivered)/dt)
	}
	return res
}
