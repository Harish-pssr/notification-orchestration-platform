package main

import "testing"

func TestGeometricRampRoundsAndStaysAscending(t *testing.T) {
	got := geometric(50, 1.25, 200)
	want := []float64{50, 63, 78, 98, 122, 153, 191}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("step %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestGeometricRampNeverRepeatsARate(t *testing.T) {
	got := geometric(2, 1.1, 10) // 2, 2.2, 2.42 ... round to the same integer early on
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("rates must strictly increase, got %v", got)
		}
	}
}

func TestParseRatesRejectsGarbage(t *testing.T) {
	if _, err := parseRates("50,abc"); err == nil {
		t.Error("a non-numeric rate must be rejected")
	}
	if _, err := parseRates("50,-1"); err == nil {
		t.Error("a negative rate must be rejected")
	}
	if got, err := parseRates("50, 100"); err != nil || len(got) != 2 || got[1] != 100 {
		t.Errorf("parseRates = %v, %v", got, err)
	}
}
