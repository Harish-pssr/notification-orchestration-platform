package consumer

import (
	"errors"
	"testing"
	"time"
)

func TestPermanentClassification(t *testing.T) {
	cases := []struct {
		status    int
		permanent bool
	}{
		{500, false}, {502, false}, {503, false}, {504, false}, // server trouble: retry
		{408, false}, {429, false}, // asked to slow down or time out: retry
		{400, true}, {401, true}, {403, true}, {404, true}, {409, true}, {422, true}, // our request is wrong
	}
	for _, c := range cases {
		if got := (&CallError{Status: c.status}).Permanent(); got != c.permanent {
			t.Errorf("status %d: permanent = %v, want %v", c.status, got, c.permanent)
		}
	}
}

func TestTransportErrorsAreRetryable(t *testing.T) {
	e := &CallError{Err: errors.New("connection refused")}
	if e.Permanent() {
		t.Error("a connection error must be retried")
	}
	if e.Error() != "connection refused" {
		t.Errorf("message = %q", e.Error())
	}
	if !errors.Is(e, e.Err) {
		t.Error("Unwrap should expose the transport error")
	}
}

func TestParseRetryAfter(t *testing.T) {
	cases := map[string]time.Duration{
		"3": 3 * time.Second, " 10 ": 10 * time.Second, "0": 0,
		"": 0, "soon": 0, "-4": 0, "Wed, 21 Oct 2026 07:28:00 GMT": 0,
	}
	for in, want := range cases {
		if got := parseRetryAfter(in); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}
