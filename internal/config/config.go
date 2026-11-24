package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL    string // DATABASE_URL (required, no default)
	HTTPAddr       string // HTTP_ADDR
	ProducerAPIKey string // PRODUCER_API_KEY (required)
	Policy         string // POLICY: fifo | roundrobin | drr

	// Per-tenant limits (max_inflight, weight) live in the tenants table, not here,
	// so each tenant can have its own. Each endpoint signs with its own secret.

	Workers         int // WORKERS: delivery goroutines per dispatcher process
	MaxAttempts     int // MAX_ATTEMPTS
	BreakerFailures int // BREAKER_FAILURES: failures in a row that open an endpoint's breaker

	LeaseTTL        time.Duration // LEASE_TTL
	ConnectTimeout  time.Duration // CONNECT_TIMEOUT
	RequestTimeout  time.Duration // REQUEST_TIMEOUT
	BackoffBase     time.Duration // BACKOFF_BASE
	BackoffMax      time.Duration // BACKOFF_MAX
	BreakerCooldown time.Duration // BREAKER_COOLDOWN
}

// ---------------- helpers ----------------

func getInt(name string, def int) (int, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: must be a whole number, got %q", name, raw)
	}
	return n, nil
}

func getString(name string, def string) string {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return def
	}
	return raw
}

func getDuration(name string, def time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: must be a duration like 30s or 2m, got %q", name, raw)
	}
	return d, nil
}

// ---------------- Load ----------------

func Load() (Config, error) {
	var cfg Config
	var errs []error
	var err error

	// text settings
	cfg.DatabaseURL = getString("DATABASE_URL", "")
	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL: required"))
	}
	cfg.HTTPAddr = getString("HTTP_ADDR", ":8080")
	cfg.ProducerAPIKey = getString("PRODUCER_API_KEY", "")
	if cfg.ProducerAPIKey == "" {
		errs = append(errs, errors.New("PRODUCER_API_KEY: required"))
	}
	cfg.Policy = getString("POLICY", "drr")

	// number settings
	cfg.Workers, err = getInt("WORKERS", 200)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.MaxAttempts, err = getInt("MAX_ATTEMPTS", 8)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.BreakerFailures, err = getInt("BREAKER_FAILURES", 5)
	if err != nil {
		errs = append(errs, err)
	}

	// time settings
	cfg.LeaseTTL, err = getDuration("LEASE_TTL", 30*time.Second)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.ConnectTimeout, err = getDuration("CONNECT_TIMEOUT", 2*time.Second)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.RequestTimeout, err = getDuration("REQUEST_TIMEOUT", 10*time.Second)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.BackoffBase, err = getDuration("BACKOFF_BASE", 1*time.Second)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.BackoffMax, err = getDuration("BACKOFF_MAX", 10*time.Minute)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.BreakerCooldown, err = getDuration("BREAKER_COOLDOWN", 30*time.Second)
	if err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("config: %w", errors.Join(errs...))
	}
	if err := validate(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ---------------- validate ----------------

func validate(cfg Config) error {
	var errs []error

	// valid when: policy is one of the three names
	if cfg.Policy != "fifo" && cfg.Policy != "roundrobin" && cfg.Policy != "drr" {
		errs = append(errs, fmt.Errorf("POLICY: must be fifo, roundrobin or drr, got %q", cfg.Policy))
	}

	// valid when: every count is greater than 0
	if cfg.Workers <= 0 {
		errs = append(errs, fmt.Errorf("WORKERS: must be greater than 0, got %d", cfg.Workers))
	}
	if cfg.MaxAttempts <= 0 {
		errs = append(errs, fmt.Errorf("MAX_ATTEMPTS: must be greater than 0, got %d", cfg.MaxAttempts))
	}
	if cfg.BreakerFailures <= 0 {
		errs = append(errs, fmt.Errorf("BREAKER_FAILURES: must be greater than 0, got %d", cfg.BreakerFailures))
	}

	// valid when: every duration is greater than 0
	if cfg.LeaseTTL <= 0 {
		errs = append(errs, fmt.Errorf("LEASE_TTL: must be greater than 0, got %v", cfg.LeaseTTL))
	}
	if cfg.ConnectTimeout <= 0 {
		errs = append(errs, fmt.Errorf("CONNECT_TIMEOUT: must be greater than 0, got %v", cfg.ConnectTimeout))
	}
	if cfg.RequestTimeout <= 0 {
		errs = append(errs, fmt.Errorf("REQUEST_TIMEOUT: must be greater than 0 (0 means wait forever), got %v", cfg.RequestTimeout))
	}
	if cfg.BackoffBase <= 0 {
		errs = append(errs, fmt.Errorf("BACKOFF_BASE: must be greater than 0, got %v", cfg.BackoffBase))
	}
	if cfg.BackoffMax <= 0 {
		errs = append(errs, fmt.Errorf("BACKOFF_MAX: must be greater than 0, got %v", cfg.BackoffMax))
	}
	if cfg.BreakerCooldown <= 0 {
		errs = append(errs, fmt.Errorf("BREAKER_COOLDOWN: must be greater than 0, got %v", cfg.BreakerCooldown))
	}

	// valid when: the lease outlives the HTTP request (stale-worker rule)
	if cfg.LeaseTTL <= cfg.RequestTimeout {
		errs = append(errs, fmt.Errorf("LEASE_TTL: must be greater than REQUEST_TIMEOUT, got LEASE_TTL=%v REQUEST_TIMEOUT=%v", cfg.LeaseTTL, cfg.RequestTimeout))
	}
	// valid when: the first backoff wait is not bigger than the longest allowed wait
	if cfg.BackoffBase > cfg.BackoffMax {
		errs = append(errs, fmt.Errorf("BACKOFF_BASE: must not be greater than BACKOFF_MAX, got BACKOFF_BASE=%v BACKOFF_MAX=%v", cfg.BackoffBase, cfg.BackoffMax))
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation: %w", errors.Join(errs...))
	}
	return nil
}
