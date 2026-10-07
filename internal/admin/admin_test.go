package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func call(enabled bool, token, header string) int {
	h := Require(enabled, token, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestRequireTokenWorksWithoutTheFaultsSwitch(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	try := func(token, header string) int {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rec := httptest.NewRecorder()
		RequireToken(token, ok).ServeHTTP(rec, req)
		return rec.Code
	}
	if got := try("s3cret", "Bearer s3cret"); got != http.StatusNoContent {
		t.Errorf("valid token: %d", got)
	}
	if got := try("s3cret", "Bearer wrong"); got != http.StatusUnauthorized {
		t.Errorf("wrong token: %d", got)
	}
	if got := try("", "Bearer "); got != http.StatusForbidden {
		t.Errorf("no token configured: %d", got)
	}
}

func TestRequire(t *testing.T) {
	cases := []struct {
		name          string
		enabled       bool
		token, header string
		want          int
	}{
		{"disabled beats a valid token", false, "s3cret", "Bearer s3cret", http.StatusForbidden},
		{"no token configured denies everything", true, "", "Bearer ", http.StatusForbidden},
		{"missing header", true, "s3cret", "", http.StatusUnauthorized},
		{"wrong token", true, "s3cret", "Bearer nope", http.StatusUnauthorized},
		{"correct token", true, "s3cret", "Bearer s3cret", http.StatusNoContent},
	}
	for _, c := range cases {
		if got := call(c.enabled, c.token, c.header); got != c.want {
			t.Errorf("%s: status %d, want %d", c.name, got, c.want)
		}
	}
}
