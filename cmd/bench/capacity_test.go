package main

import "testing"

// ladder returns a pass function for a ladder where rates 0..limit pass.
func ladder(limit int, tested *[]int) func(int) bool {
	return func(i int) bool {
		*tested = append(*tested, i)
		return i <= limit
	}
}

func TestConfirmCapacityClimbsFromAConfirmedStart(t *testing.T) {
	var tested []int
	got := confirmCapacity(8, 3, ladder(5, &tested)) // true capacity is index 5
	if got != 5 {
		t.Fatalf("capacity index = %d, want 5", got)
	}
	want := []int{3, 4, 5, 6} // confirm the start, climb to 5, stop at the first failure
	if len(tested) != len(want) {
		t.Fatalf("tested %v, want %v", tested, want)
	}
}

func TestConfirmCapacityWalksDownWhenTheStartDoesNotHold(t *testing.T) {
	var tested []int
	got := confirmCapacity(8, 6, ladder(3, &tested)) // the ramp over-estimated
	if got != 3 {
		t.Fatalf("capacity index = %d, want 3", got)
	}
	want := []int{6, 5, 4, 3}
	for i, w := range want {
		if i >= len(tested) || tested[i] != w {
			t.Fatalf("tested %v, want %v", tested, want)
		}
	}
}

func TestConfirmCapacityReportsNoneWhenNothingPasses(t *testing.T) {
	var tested []int
	if got := confirmCapacity(5, 2, ladder(-1, &tested)); got != -1 {
		t.Errorf("capacity index = %d, want -1", got)
	}
	if len(tested) != 3 { // 2, 1, 0
		t.Errorf("tested %v, want each lower rate once", tested)
	}
}

func TestConfirmCapacityNeverTestsARateTwiceOrPastTheEnds(t *testing.T) {
	var tested []int
	confirmCapacity(4, 0, ladder(10, &tested)) // everything passes
	seen := map[int]bool{}
	for _, i := range tested {
		if i < 0 || i > 3 || seen[i] {
			t.Fatalf("bad test sequence %v", tested)
		}
		seen[i] = true
	}
	// A ramp that found nothing starts at the lowest rate.
	tested = nil
	if got := confirmCapacity(4, -1, ladder(1, &tested)); got != 1 {
		t.Errorf("start -1 gave %d, want 1", got)
	}
	if got := confirmCapacity(0, 0, ladder(1, &tested)); got != -1 {
		t.Errorf("an empty ladder gave %d, want -1", got)
	}
}
