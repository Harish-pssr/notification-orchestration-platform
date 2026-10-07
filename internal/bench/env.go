package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Environment is the machine state a result was measured on. Benchmark numbers only
// mean something together with it: a laptop on battery or on a power-saving plan can
// deliver a fraction of its plugged-in performance, which is exactly how an hour of
// results can quietly become meaningless.
type Environment struct {
	CapturedAt  time.Time `json:"captured_at"`
	OS          string    `json:"os"`
	CPUModel    string    `json:"cpu_model,omitempty"`
	LogicalCPUs int       `json:"logical_cpus"`
	ClockMHz    int       `json:"clock_mhz,omitempty"`     // current clock when captured
	MaxClockMHz int       `json:"max_clock_mhz,omitempty"` // nominal maximum
	PowerSource string    `json:"power_source"`            // "ac", "battery" or "unknown"
	PowerPlan   string    `json:"power_plan,omitempty"`
	DockerCPUs  int       `json:"docker_cpus,omitempty"`
	DockerMemGB float64   `json:"docker_mem_gb,omitempty"`
}

// batteryPowerSource maps the Windows Win32_Battery BatteryStatus code to a power source.
// 1 means discharging (on battery); 2 and the "charging" codes mean mains power.
func batteryPowerSource(code int) string {
	switch code {
	case 1, 4, 5: // discharging, low, critical
		return "battery"
	case 0:
		return "unknown"
	default: // 2 AC, 3 fully charged, 6-9 charging variants, 10 undefined partially charged, 11
		return "ac"
	}
}

// planName extracts the name from `powercfg /getactivescheme` output, for example
// "Power Scheme GUID: 381b...  (Balanced)" gives "Balanced".
func planName(line string) string {
	open, closeIdx := strings.LastIndex(line, "("), strings.LastIndex(line, ")")
	if open >= 0 && closeIdx > open {
		return strings.TrimSpace(line[open+1 : closeIdx])
	}
	return strings.TrimSpace(line)
}

// OnBattery reports whether the machine is known to be running on battery.
func (e Environment) OnBattery() bool { return e.PowerSource == "battery" }

// Warning returns a message when the environment is likely to distort results, or "".
func (e Environment) Warning() string {
	var problems []string
	if e.OnBattery() {
		problems = append(problems, "the machine is running on battery, which power-limits the CPU")
	}
	if strings.Contains(strings.ToLower(e.PowerPlan), "saver") {
		problems = append(problems, fmt.Sprintf("the power plan is %q", e.PowerPlan))
	}
	if len(problems) == 0 {
		return ""
	}
	return "benchmark results would be unreliable: " + strings.Join(problems, " and ") + ". Plug in and choose a high-performance power plan."
}

// CaptureEnvironment reads what it can about the host and the Docker VM. Anything it
// cannot read is left blank or "unknown" rather than guessed.
func CaptureEnvironment(ctx context.Context) Environment {
	e := Environment{CapturedAt: time.Now().UTC(), OS: runtime.GOOS, LogicalCPUs: runtime.NumCPU(), PowerSource: "unknown"}

	if runtime.GOOS == "windows" {
		script := `$p = Get-CimInstance Win32_Processor | Select-Object -First 1;
$b = Get-CimInstance Win32_Battery -ErrorAction SilentlyContinue;
$plan = (powercfg /getactivescheme) -join ' ';
[pscustomobject]@{ cpu = $p.Name; clock = $p.CurrentClockSpeed; max = $p.MaxClockSpeed;
  battery = $(if ($b) { [int]$b.BatteryStatus } else { -1 }); plan = $plan } | ConvertTo-Json -Compress`
		if out, err := exec.CommandContext(ctx, "powershell", "-NoProfile", "-Command", script).Output(); err == nil {
			var raw struct {
				CPU     string `json:"cpu"`
				Clock   int    `json:"clock"`
				Max     int    `json:"max"`
				Battery int    `json:"battery"`
				Plan    string `json:"plan"`
			}
			if json.Unmarshal(bytes.TrimSpace(out), &raw) == nil {
				e.CPUModel, e.ClockMHz, e.MaxClockMHz, e.PowerPlan = strings.TrimSpace(raw.CPU), raw.Clock, raw.Max, planName(raw.Plan)
				if raw.Battery == -1 {
					e.PowerSource = "ac" // no battery: a desktop
				} else {
					e.PowerSource = batteryPowerSource(raw.Battery)
				}
			}
		}
	}

	if out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}").Output(); err == nil {
		if f := strings.Fields(string(out)); len(f) == 2 {
			e.DockerCPUs, _ = strconv.Atoi(f[0])
			if mem, err := strconv.ParseFloat(f[1], 64); err == nil {
				e.DockerMemGB = mem / (1 << 30)
			}
		}
	}
	return e
}
