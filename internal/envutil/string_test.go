package envutil

import (
	"os"
	"testing"
)

func TestEnvOrDefault(t *testing.T) {
	const key = "ORABBIT_TEST_ENV_OR_DEFAULT"
	prev, hadPrev := os.LookupEnv(key)
	defer func() {
		if hadPrev {
			_ = os.Setenv(key, prev)
		} else {
			_ = os.Unsetenv(key)
		}
	}()

	_ = os.Unsetenv(key)
	if got := EnvOrDefault(key, "fallback"); got != "fallback" {
		t.Fatalf("missing env = %q, want fallback", got)
	}

	_ = os.Setenv(key, "   ")
	if got := EnvOrDefault(key, "fallback"); got != "fallback" {
		t.Fatalf("blank env = %q, want fallback", got)
	}

	_ = os.Setenv(key, "  value  ")
	if got := EnvOrDefault(key, "fallback"); got != "value" {
		t.Fatalf("trimmed env = %q, want value", got)
	}
}
