package provider

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"notifplatform/internal/events"
)

// failure is an injected misbehaviour for one channel (D8): an HTTP error status, an
// added delay, or both. Remaining limits how many requests it affects (nil = all).
type failure struct {
	Status    int           `json:"status"`
	Delay     time.Duration `json:"-"`
	DelayMs   int           `json:"delay_ms"`
	Remaining *int          `json:"remaining"`
}

// failures holds the provider's own fault state. It lives in memory: the provider is
// a separate "external" system with its own control surface, and a restart clears it.
type failures struct {
	mu     sync.Mutex
	byChan map[events.Channel]*failure
}

func newFailures() *failures { return &failures{byChan: map[events.Channel]*failure{}} }

// take returns the failure that applies to this request, consuming one use of a
// limited failure. It returns nil when the channel behaves normally.
func (f *failures) take(ch events.Channel) *failure {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.byChan[ch]
	if !ok {
		return nil
	}
	applied := *cur
	if cur.Remaining != nil {
		*cur.Remaining--
		if *cur.Remaining <= 0 {
			delete(f.byChan, ch)
		}
	}
	return &applied
}

func (f *failures) set(ch events.Channel, fl failure) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fl.Delay = time.Duration(fl.DelayMs) * time.Millisecond
	f.byChan[ch] = &fl
}

func (f *failures) clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byChan = map[events.Channel]*failure{}
}

func (f *failures) snapshot() map[events.Channel]failure {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[events.Channel]failure, len(f.byChan))
	for ch, fl := range f.byChan {
		out[ch] = *fl
	}
	return out
}

// setFailure handles POST /admin/failures:
// {"channel": "webhook", "status": 500, "delay_ms": 0, "remaining": 3}.
func (p *Provider) setFailure(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Channel   events.Channel `json:"channel"`
		Status    int            `json:"status"`
		DelayMs   int            `json:"delay_ms"`
		Remaining *int           `json:"remaining"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&in); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	switch {
	case !in.Channel.Valid():
		http.Error(w, "channel must be email, push or webhook", http.StatusBadRequest)
	case in.Status != 0 && (in.Status < 400 || in.Status > 599):
		http.Error(w, "status must be 0 (no error) or between 400 and 599", http.StatusBadRequest)
	case in.DelayMs < 0 || in.DelayMs > 60000:
		http.Error(w, "delay_ms must be between 0 and 60000", http.StatusBadRequest)
	case in.Status == 0 && in.DelayMs == 0:
		http.Error(w, "set a status, a delay, or both", http.StatusBadRequest)
	case in.Remaining != nil && *in.Remaining < 1:
		http.Error(w, "remaining must be at least 1", http.StatusBadRequest)
	default:
		p.fails.set(in.Channel, failure{Status: in.Status, DelayMs: in.DelayMs, Remaining: in.Remaining})
		p.log.Warn("provider failure injected", "channel", in.Channel, "status", in.Status, "delay_ms", in.DelayMs)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.fails.snapshot())
	}
}

func (p *Provider) clearFailures(w http.ResponseWriter, r *http.Request) {
	p.fails.clear()
	p.log.Warn("provider failures cleared")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte("{}"))
}

// applyFailure runs the injected failure for this request, if any. It reports whether
// the request was answered (an error status) and so must not be processed further.
func (p *Provider) applyFailure(w http.ResponseWriter, r *http.Request, ch events.Channel) (answered bool) {
	fl := p.fails.take(ch)
	if fl == nil {
		return false
	}
	if fl.Delay > 0 {
		select {
		case <-time.After(fl.Delay):
		case <-r.Context().Done():
			return true // the caller gave up (for example a consumer timeout); no effect runs
		}
	}
	if fl.Status != 0 {
		if fl.Status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", strconv.Itoa(1))
		}
		http.Error(w, "injected failure", fl.Status)
		return true
	}
	return false
}
