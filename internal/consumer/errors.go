package consumer

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CallError describes a failed provider call.
type CallError struct {
	Status     int           // HTTP status; 0 when there was no response (network error, timeout)
	RetryAfter time.Duration // from a Retry-After header, if the provider sent one
	Err        error         // the underlying transport error when Status is 0
}

func (e *CallError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("provider returned %d", e.Status)
	}
	return e.Err.Error()
}

func (e *CallError) Unwrap() error { return e.Err }

// Permanent reports whether sending the identical request again cannot succeed (D7).
// That is any 4xx except 408 (request timeout) and 429 (too many requests): the
// request itself is wrong or not allowed, so the provider will reject it again.
// 5xx, timeouts and connection errors are transient and are retried.
func (e *CallError) Permanent() bool {
	return e.Status >= 400 && e.Status < 500 && e.Status != 408 && e.Status != 429
}

// parseRetryAfter reads a Retry-After header given in whole seconds. The HTTP-date
// form is ignored, which only means we fall back to our own backoff.
func parseRetryAfter(h string) time.Duration {
	secs, err := strconv.Atoi(strings.TrimSpace(h))
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}
