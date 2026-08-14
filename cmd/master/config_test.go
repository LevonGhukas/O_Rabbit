package main

import (
	"os"
	"testing"
	"time"
)

func TestEnvBoolDefault(t *testing.T) {
	const key = "ORABBIT_TEST_BOOL_ENV"
	prev, hadPrev := os.LookupEnv(key)
	defer func() {
		if hadPrev {
			_ = os.Setenv(key, prev)
		} else {
			_ = os.Unsetenv(key)
		}
	}()

	_ = os.Unsetenv(key)
	if got := envBoolDefault(key, true); !got {
		t.Fatalf("empty env should return default true")
	}
	if got := envBoolDefault(key, false); got {
		t.Fatalf("empty env should return default false")
	}

	_ = os.Setenv(key, "on")
	if got := envBoolDefault(key, false); !got {
		t.Fatalf("on should parse as true")
	}

	_ = os.Setenv(key, "off")
	if got := envBoolDefault(key, true); got {
		t.Fatalf("off should parse as false")
	}

	_ = os.Setenv(key, "unknown")
	if got := envBoolDefault(key, true); !got {
		t.Fatalf("unknown should return default true")
	}
	if got := envBoolDefault(key, false); got {
		t.Fatalf("unknown should return default false")
	}
}

func TestAdmissionConfigurationFromEnvironment(t *testing.T) {
	t.Setenv("ORABBIT_MAX_ACTIVE_RUNS", "3")
	t.Setenv("ORABBIT_UPLOAD_CAPACITY_LIMIT", "5")
	t.Setenv("ORABBIT_UPLOAD_CAPACITY_LEASE_TTL", "45s")
	cfg := loadMasterConfigFromEnv()
	if cfg.MaxActiveRuns != 3 || cfg.UploadCapacityLimit != 5 || cfg.UploadCapacityLeaseTTL != 45*time.Second {
		t.Fatalf("admission config=%+v", cfg)
	}
	if err := cfg.validateLeasePolicy(); err != nil {
		t.Fatalf("valid admission config rejected: %v", err)
	}
}

func TestWorkerAuthTokenLoadsFromEnvironment(t *testing.T) {
	t.Setenv("ORABBIT_WORKER_AUTH_TOKEN", "  worker-secret  ")
	cfg := loadMasterConfigFromEnv()
	if cfg.WorkerAuthToken != "worker-secret" {
		t.Fatalf("WorkerAuthToken=%q", cfg.WorkerAuthToken)
	}
}
