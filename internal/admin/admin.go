// Package admin protects operator-only endpoints (D8): fault injection exists only
// when enabled, and every operator action requires the shared admin token.
package admin

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// Require wraps next so it only runs when enabled is true and the request carries
// "Authorization: Bearer <token>". The comparison is constant-time. Use it for fault
// injection, which must not exist at all unless explicitly enabled.
func Require(enabled bool, token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !enabled {
			deny(w, http.StatusForbidden, "fault injection is disabled (set FAULTS_ENABLED=true)")
			return
		}
		if !authorized(w, r, token) {
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequireToken is like Require without the enabled switch. Use it for operator
// actions that are not fault injection, such as replaying dead-lettered deliveries,
// which must work on a deployment where faults are off.
func RequireToken(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r, token) {
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authorized checks the bearer token and writes the refusal itself when it fails.
func authorized(w http.ResponseWriter, r *http.Request, token string) bool {
	switch {
	case token == "":
		deny(w, http.StatusForbidden, "no admin token is configured (set ADMIN_TOKEN)")
	case subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(token)) != 1:
		deny(w, http.StatusUnauthorized, "missing or invalid admin token")
	default:
		return true
	}
	return false
}

func deny(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
