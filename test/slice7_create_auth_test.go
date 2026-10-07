package test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"notifplatform/internal/httpapi"
)

func postCreate(t *testing.T, base, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/api/events/payment-succeeded",
		strings.NewReader(`{"account_id":"a","amount_cents":100,"currency":"USD"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", uuid.NewString())
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// D19: on a public deployment every write uses the one admin token and reading stays open.
func TestCreatingEventsCanRequireTheAdminToken(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, _ := isolatedStore(t)
	srv := httptest.NewServer(httpapi.NewHandler(httpapi.Deps{Store: store, AdminToken: "s3cret", CreateRequiresToken: true}))
	defer srv.Close()

	if got := postCreate(t, srv.URL, ""); got != http.StatusUnauthorized {
		t.Errorf("create without a token = %d, want 401", got)
	}
	if got := postCreate(t, srv.URL, "wrong"); got != http.StatusUnauthorized {
		t.Errorf("create with a wrong token = %d, want 401", got)
	}
	if got := postCreate(t, srv.URL, "s3cret"); got != http.StatusCreated {
		t.Errorf("create with the token = %d, want 201", got)
	}

	// Reading stays open without any credential, and the config tells the UI what to ask for.
	for _, path := range []string{"/api/summary", "/api/events", "/api/config"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d, want 200 without a token", path, resp.StatusCode)
		}
	}
}

// The default (local development, the benchmarks, the other tests) is unchanged.
func TestCreatingEventsNeedsNoTokenByDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, _ := isolatedStore(t)
	srv := httptest.NewServer(httpapi.NewHandler(httpapi.Deps{Store: store, AdminToken: "s3cret"}))
	defer srv.Close()

	if got := postCreate(t, srv.URL, ""); got != http.StatusCreated {
		t.Errorf("create without a token and without the setting = %d, want 201", got)
	}
}
