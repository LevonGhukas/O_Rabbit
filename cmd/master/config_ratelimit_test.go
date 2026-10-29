package main

import (
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/ratelimit"
)

func TestRateLimitConfigDefaults(t *testing.T) {
	cfg := loadRateLimitConfigFromEnv()

	if err := cfg.validate(); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
	for name, p := range map[string]ratelimit.Policy{
		"http": cfg.HTTP, "sensitive": cfg.HTTPSensitive, "auth failures": cfg.AuthFailures, "enroll": cfg.Enroll,
	} {
		if !p.Enabled() {
			t.Errorf("%s limit disabled by default", name)
		}
	}
	if cfg.Enroll.Burst < 100 {
		t.Errorf("enroll burst=%d, want room for a burst of at least 100 workers", cfg.Enroll.Burst)
	}
	proxies, err := cfg.trustedProxies()
	if err != nil || len(proxies) != 0 {
		t.Fatalf("trusted proxies = %v, %v; want none by default", proxies, err)
	}
}

func TestRateLimitConfigFromEnv(t *testing.T) {
	t.Setenv("ORABBIT_HTTP_RATE_LIMIT_RPS", "10.5")
	t.Setenv("ORABBIT_HTTP_RATE_LIMIT_BURST", "40")
	t.Setenv("ORABBIT_HTTP_SENSITIVE_RATE_LIMIT_RPS", "0")
	t.Setenv("ORABBIT_AUTH_FAILURE_RATE_LIMIT_BURST", "5")
	t.Setenv("ORABBIT_HTTP_TRUSTED_PROXIES", "10.0.0.0/8, 192.168.0.1")

	cfg := loadRateLimitConfigFromEnv()

	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP != (ratelimit.Policy{Rate: 10.5, Burst: 40}) {
		t.Fatalf("http policy = %+v", cfg.HTTP)
	}
	if cfg.HTTPSensitive.Enabled() {
		t.Fatal("rps 0 did not disable the sensitive limit")
	}
	if cfg.AuthFailures.Burst != 5 || cfg.AuthFailures.Rate != defaultAuthFailureRateLimit.Rate {
		t.Fatalf("auth failure policy = %+v", cfg.AuthFailures)
	}
	proxies, err := cfg.trustedProxies()
	if err != nil || len(proxies) != 2 {
		t.Fatalf("trusted proxies = %v, %v", proxies, err)
	}
}

func TestRateLimitConfigRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		key, value, wantInErr string
	}{
		{"ORABBIT_HTTP_RATE_LIMIT_RPS", "fast", "ORABBIT_HTTP_RATE_LIMIT_RPS"},
		{"ORABBIT_HTTP_RATE_LIMIT_RPS", "-1", "ORABBIT_HTTP_RATE_LIMIT"},
		{"ORABBIT_ENROLL_RATE_LIMIT_BURST", "0", "ORABBIT_ENROLL_RATE_LIMIT"},
		{"ORABBIT_ENROLL_RATE_LIMIT_BURST", "many", "ORABBIT_ENROLL_RATE_LIMIT_BURST"},
		{"ORABBIT_HTTP_TRUSTED_PROXIES", "10.0.0.0/8,proxy.local", "ORABBIT_HTTP_TRUSTED_PROXIES"},
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			err := loadRateLimitConfigFromEnv().validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantInErr) {
				t.Fatalf("validate() = %v, want an error naming %s", err, tc.wantInErr)
			}
		})
	}
}
