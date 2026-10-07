// Package config reads process configuration from environment variables.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds settings shared by every binary. Each binary uses the subset it needs.
type Config struct {
	PlatformDSN string // platform Postgres (business rows, outbox, deliveries)
	ProviderDSN string // provider Postgres (mock external system)
	NATSURL     string
	HTTPAddr    string
}

// Load reads the environment, falling back to local-development defaults.
func Load() Config {
	return Config{
		PlatformDSN: Getenv("PLATFORM_DSN", "postgres://notif:notif@localhost:15433/platform?sslmode=disable"),
		ProviderDSN: Getenv("PROVIDER_DSN", "postgres://notif:notif@localhost:15434/provider?sslmode=disable"),
		NATSURL:     Getenv("NATS_URL", "nats://localhost:4222"),
		HTTPAddr:    Getenv("HTTP_ADDR", ":8080"),
	}
}

// Getenv returns the environment variable key, or def when it is unset or empty.
func Getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// GetenvInt is like Getenv but parses an integer; an unparsable value yields def.
func GetenvInt(key string, def int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return def
	}
	return v
}

// GetenvDuration is like Getenv but parses a duration such as "30s".
func GetenvDuration(key string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(os.Getenv(key))
	if err != nil {
		return def
	}
	return v
}
