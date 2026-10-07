package bench

import (
	"strings"
	"testing"
)

func TestBatteryStatusCodesMapToAPowerSource(t *testing.T) {
	cases := map[int]string{
		1: "battery", // discharging
		4: "battery", // low
		5: "battery", // critical
		2: "ac",      // on AC
		3: "ac",      // fully charged
		6: "ac",      // charging
		0: "unknown",
	}
	for code, want := range cases {
		if got := batteryPowerSource(code); got != want {
			t.Errorf("BatteryStatus %d = %q, want %q", code, got, want)
		}
	}
}

func TestPlanNameIsTakenFromTheParentheses(t *testing.T) {
	cases := map[string]string{
		"Power Scheme GUID: 381b4222-f694-41f0-9685-ff5bb260df2e  (Balanced)":         "Balanced",
		"Power Scheme GUID: 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c  (High performance)": "High performance",
		"something without parentheses":                                               "something without parentheses",
	}
	for in, want := range cases {
		if got := planName(in); got != want {
			t.Errorf("planName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWarningFlagsBatteryAndPowerSaver(t *testing.T) {
	if w := (Environment{PowerSource: "ac", PowerPlan: "High performance"}).Warning(); w != "" {
		t.Errorf("a plugged-in, high-performance machine got a warning: %s", w)
	}
	w := Environment{PowerSource: "battery", PowerPlan: "Balanced"}.Warning()
	if !strings.Contains(w, "battery") {
		t.Errorf("battery not flagged: %q", w)
	}
	w = Environment{PowerSource: "ac", PowerPlan: "Power saver"}.Warning()
	if !strings.Contains(w, "Power saver") {
		t.Errorf("power saver not flagged: %q", w)
	}
	// Balanced on mains power is acceptable: Windows still allows full speed.
	if w := (Environment{PowerSource: "ac", PowerPlan: "Balanced"}).Warning(); w != "" {
		t.Errorf("Balanced on AC should not warn: %s", w)
	}
	// An unknown power source must not block anything (we do not guess).
	if w := (Environment{PowerSource: "unknown"}).Warning(); w != "" {
		t.Errorf("unknown power source warned: %s", w)
	}
}
