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

func TestCatalogAttemptTimeoutsFromEnvironment(t *testing.T) {
	t.Setenv("ORABBIT_REGISTRATION_TIMEOUT", "4m")
	t.Setenv("ORABBIT_RECONCILIATION_TIMEOUT", "90s")
	cfg := loadMasterConfigFromEnv()
	if cfg.RegistrationTimeout != 4*time.Minute || cfg.ReconciliationTimeout != 90*time.Second {
		t.Fatalf("registration=%s reconciliation=%s", cfg.RegistrationTimeout, cfg.ReconciliationTimeout)
	}
}

func TestReconciliationTimeoutDefaultsWhenUnset(t *testing.T) {
	t.Setenv("ORABBIT_RECONCILIATION_TIMEOUT", "")
	if got := loadMasterConfigFromEnv().ReconciliationTimeout; got != defaultReconciliationTimeout {
		t.Fatalf("reconciliation timeout=%s want %s", got, defaultReconciliationTimeout)
	}
}

func TestLeasePolicyRejectsNonPositiveCatalogTimeouts(t *testing.T) {
	cfg := loadMasterConfigFromEnv()
	cfg.ReconciliationTimeout = 0
	if err := cfg.validateLeasePolicy(); err == nil {
		t.Fatal("zero reconciliation timeout accepted")
	}
	cfg = loadMasterConfigFromEnv()
	cfg.RegistrationTimeout = 0
	if err := cfg.validateLeasePolicy(); err == nil {
		t.Fatal("zero registration timeout accepted")
	}
}
