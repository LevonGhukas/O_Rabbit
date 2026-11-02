package main

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/ratelimit"
)

// Default limits are per client address. They are generous so that the
// admin UI and a burst of workers enrolling at once stay well inside them,
// even when several users share one address behind a proxy.
var (
	defaultHTTPRateLimit          = ratelimit.Policy{Rate: 50, Burst: 200}
	defaultHTTPSensitiveRateLimit = ratelimit.Policy{Rate: 1, Burst: 30}
	defaultAuthFailureRateLimit   = ratelimit.Policy{Rate: 0.2, Burst: 20}
	defaultEnrollRateLimit        = ratelimit.Policy{Rate: 2, Burst: 100}
)

const (
	envHTTPRateLimit          = "ORABBIT_HTTP_RATE_LIMIT"
	envHTTPSensitiveRateLimit = "ORABBIT_HTTP_SENSITIVE_RATE_LIMIT"
	envAuthFailureRateLimit   = "ORABBIT_AUTH_FAILURE_RATE_LIMIT"
	envEnrollRateLimit        = "ORABBIT_ENROLL_RATE_LIMIT"
	envHTTPTrustedProxies     = "ORABBIT_HTTP_TRUSTED_PROXIES"
)

// rateLimitConfig holds the master's per-client request limits.
type rateLimitConfig struct {
	HTTP           ratelimit.Policy
	HTTPSensitive  ratelimit.Policy
	AuthFailures   ratelimit.Policy
	Enroll         ratelimit.Policy
	TrustedProxies string

	// envErr records variables that could not be parsed, so startup fails
	// instead of silently falling back to a default.
	envErr error
}

func loadRateLimitConfigFromEnv() rateLimitConfig {
	var errs []error
	policy := func(prefix string, def ratelimit.Policy) ratelimit.Policy {
		p, err := envPolicyDefault(prefix, def)
		if err != nil {
			errs = append(errs, err)
		}
		return p
	}
	cfg := rateLimitConfig{
		HTTP:           policy(envHTTPRateLimit, defaultHTTPRateLimit),
		HTTPSensitive:  policy(envHTTPSensitiveRateLimit, defaultHTTPSensitiveRateLimit),
		AuthFailures:   policy(envAuthFailureRateLimit, defaultAuthFailureRateLimit),
		Enroll:         policy(envEnrollRateLimit, defaultEnrollRateLimit),
		TrustedProxies: strings.TrimSpace(os.Getenv(envHTTPTrustedProxies)),
	}
	cfg.envErr = errors.Join(errs...)
	return cfg
}

// envPolicyDefault reads <prefix>_RPS (events per second; 0 disables the
// limit) and <prefix>_BURST, using def for whichever is unset.
func envPolicyDefault(prefix string, def ratelimit.Policy) (ratelimit.Policy, error) {
	p := def
	if raw := strings.TrimSpace(os.Getenv(prefix + "_RPS")); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return def, fmt.Errorf("%s_RPS: %q is not a number", prefix, raw)
		}
		p.Rate = v
	}
	if raw := strings.TrimSpace(os.Getenv(prefix + "_BURST")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			return def, fmt.Errorf("%s_BURST: %q is not an integer", prefix, raw)
		}
		p.Burst = v
	}
	return p, nil
}

func (c rateLimitConfig) validate() error {
	if c.envErr != nil {
		return c.envErr
	}
	for prefix, p := range map[string]ratelimit.Policy{
		envHTTPRateLimit:          c.HTTP,
		envHTTPSensitiveRateLimit: c.HTTPSensitive,
		envAuthFailureRateLimit:   c.AuthFailures,
		envEnrollRateLimit:        c.Enroll,
	} {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("%s: %w", prefix, err)
		}
	}
	_, err := c.trustedProxies()
	return err
}

func (c rateLimitConfig) trustedProxies() ([]netip.Prefix, error) {
	proxies, err := ratelimit.ParseTrustedProxies(c.TrustedProxies)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", envHTTPTrustedProxies, err)
	}
	return proxies, nil
}
