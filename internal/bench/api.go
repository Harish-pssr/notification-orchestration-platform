package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Admin drives the platform's admin endpoints (fault injection and the provider's
// failure controls) the way the dashboard does. It needs a dev deployment with
// FAULTS_ENABLED=true and the admin token.
type Admin struct {
	URL    string // API base URL as seen from this machine
	Token  string
	Client *http.Client
}

func (a Admin) do(ctx context.Context, method, path string, body any) error {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.URL+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// SetFault creates or replaces the fault at point (body fields: action, remaining, probability).
func (a Admin) SetFault(ctx context.Context, point string, body map[string]any) error {
	return a.do(ctx, http.MethodPut, "/api/faults/"+point, body)
}

// ClearFaults removes every configured fault.
func (a Admin) ClearFaults(ctx context.Context) error {
	return a.do(ctx, http.MethodDelete, "/api/faults", nil)
}

// SetProviderFailure makes the provider answer status for a fraction of requests on a channel.
func (a Admin) SetProviderFailure(ctx context.Context, channel string, status int, probability float64) error {
	return a.do(ctx, http.MethodPost, "/api/provider/failure",
		map[string]any{"channel": channel, "status": status, "probability": probability})
}

// ClearProviderFailures removes every injected provider failure.
func (a Admin) ClearProviderFailures(ctx context.Context) error {
	return a.do(ctx, http.MethodPost, "/api/provider/failure/clear", map[string]any{})
}

// SetProviderDedupe switches the provider's idempotency protection on or off (the
// negative control turns it off).
func (a Admin) SetProviderDedupe(ctx context.Context, on bool) error {
	return a.do(ctx, http.MethodPost, "/api/provider/dedupe", map[string]any{"enabled": on})
}

// Restore puts every injected setting back to normal. It is safe to call repeatedly and
// is meant for deferred cleanup, so it uses its own context and ignores individual errors
// after trying everything.
func (a Admin) Restore() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var first error
	for _, step := range []func(context.Context) error{
		a.ClearFaults, a.ClearProviderFailures, func(c context.Context) error { return a.SetProviderDedupe(c, true) },
	} {
		if err := step(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}
