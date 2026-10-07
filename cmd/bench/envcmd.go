package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"notifplatform/internal/bench"
)

// envCommand is `bench env`: record the machine's environment next to a benchmark's
// results and refuse to continue on battery power, for scripts that do not use the
// other subcommands (such as the relay-scaling script).
func envCommand(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("env", flag.ExitOnError)
	out := fs.String("out", "benchmark-results", "directory to write environment.json into")
	allowBattery := fs.Bool("allow-battery", false, "continue even though the machine is on battery")
	_ = fs.Parse(args)

	machine := bench.CaptureEnvironment(ctx)
	fmt.Printf("environment: %s, %d logical CPUs, power %s (%s), clock %d/%d MHz, Docker VM %d CPUs %.1f GB\n",
		machine.CPUModel, machine.LogicalCPUs, machine.PowerSource, machine.PowerPlan, machine.ClockMHz, machine.MaxClockMHz, machine.DockerCPUs, machine.DockerMemGB)
	if w := machine.Warning(); w != "" && !*allowBattery {
		return fmt.Errorf("%s (override with -allow-battery)", w)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(machine, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*out, "environment.json"), raw, 0o644)
}
