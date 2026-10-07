package test

import (
	"context"
	"testing"

	"notifplatform/internal/experiment"
	"notifplatform/internal/health"
)

func TestExperimentMarkersShowTheLatestRunAndWhetherItIsActive(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	_, pool := isolatedStore(t)
	ctx := context.Background()

	st, err := experiment.Current(ctx, pool)
	if err != nil || st.Run != "" || st.Active || len(st.Markers) != 0 {
		t.Fatalf("with no markers got %+v, err %v; want an empty, inactive state", st, err)
	}

	if err := experiment.Mark(ctx, pool, "old-run", experiment.Start, "old", nil); err != nil {
		t.Fatal(err)
	}
	if err := experiment.Mark(ctx, pool, "old-run", experiment.End, "done", nil); err != nil {
		t.Fatal(err)
	}
	if err := experiment.Mark(ctx, pool, "new-run", experiment.Start, "start", map[string]any{"rate": 150}); err != nil {
		t.Fatal(err)
	}
	if err := experiment.Mark(ctx, pool, "new-run", experiment.Phase, "relay crash 1 of 3", nil); err != nil {
		t.Fatal(err)
	}

	st, err = experiment.Current(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if st.Run != "new-run" || !st.Active || len(st.Markers) != 2 {
		t.Fatalf("got %+v, want the latest run, active, with its 2 markers (not the old run's)", st)
	}
	if st.Markers[0].Params["rate"] != float64(150) {
		t.Errorf("params = %v, want rate 150", st.Markers[0].Params)
	}

	if err := experiment.Mark(ctx, pool, "new-run", experiment.End, "finished", nil); err != nil {
		t.Fatal(err)
	}
	if st, _ = experiment.Current(ctx, pool); st.Active {
		t.Error("a run with an end marker must not be active")
	}
}

func TestHeartbeatStoresTheInstancesSettings(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	_, pool := isolatedStore(t)
	ctx := context.Background()
	if err := health.BeatWithConfig(ctx, pool, "consumer-email-1", "consumer-email", health.StateRunning,
		map[string]any{"concurrency": 8}); err != nil {
		t.Fatal(err)
	}
	list, err := health.List(ctx, pool)
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %v, err %v", list, err)
	}
	if list[0].Config["concurrency"] != float64(8) {
		t.Errorf("config = %v, want concurrency 8", list[0].Config)
	}
}
