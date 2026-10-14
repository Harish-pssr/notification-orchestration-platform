package bench

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmailMustBeOffRefusesWhileRealSendingIsOn(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"configured":true,"real":true}`))
	}))
	defer srv.Close()
	err := EmailMustBeOff(context.Background(), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "real email sending is switched ON") {
		t.Fatalf("err = %v, want a refusal that names the switch", err)
	}
}

func TestEmailMustBeOffAllowsTheMockAndUnknownEndpoints(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"switched off": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"configured":true,"real":false}`))
		},
		"not configured": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"configured":false,"real":false}`))
		},
		"older API without the endpoint": func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) },
		"unreadable answer":              func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`not json`)) },
	} {
		srv := httptest.NewServer(handler)
		if err := EmailMustBeOff(context.Background(), srv.URL); err != nil {
			t.Errorf("%s: unexpected refusal: %v", name, err)
		}
		srv.Close()
	}
	if err := EmailMustBeOff(context.Background(), "http://127.0.0.1:1"); err != nil {
		t.Errorf("an unreachable API must not be reported here: %v", err)
	}
}
