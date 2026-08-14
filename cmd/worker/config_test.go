package main

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadWorkerConfigFromEnv(t *testing.T) {
	t.Setenv("ORABBIT_LOG_LEVEL", "debug")
	t.Setenv("ORABBIT_LOG_FORMAT", "text")
	t.Setenv("ORABBIT_WORKER_TEMP_ROOT", filepath.Join(t.TempDir(), "managed"))
	t.Setenv("ORABBIT_TEMP_SCAN_INTERVAL", "17s")
	t.Setenv("ORABBIT_TEMP_DRY_RUN", "true")
	t.Setenv("ORABBIT_WORKER_AUTH_TOKEN", "  worker-secret  ")

	cfg := loadWorkerConfigFromEnv()
	if got := cfg.LogLevel; got != "debug" {
		t.Fatalf("LogLevel=%q want %q", got, "debug")
	}
	if got := cfg.LogFormat; got != "text" {
		t.Fatalf("LogFormat=%q want %q", got, "text")
	}
	if cfg.TempRoot == "" || cfg.TempScanInterval.String() != "17s" || !cfg.TempDryRun {
		t.Fatalf("temp config=%+v", cfg)
	}
	if cfg.WorkerAuthToken != "worker-secret" {
		t.Fatalf("WorkerAuthToken=%q", cfg.WorkerAuthToken)
	}
}

func TestWorkerAuthTokenFlagOverridesEnvironment(t *testing.T) {
	t.Setenv("ORABBIT_WORKER_AUTH_TOKEN", "env-secret")
	cfg := loadWorkerConfigFromEnv()
	fs := newWorkerFlagSet(&cfg)
	if err := fs.Parse([]string{"-worker-auth-token", "flag-secret"}); err != nil {
		t.Fatal(err)
	}
	if cfg.WorkerAuthToken != "flag-secret" {
		t.Fatalf("WorkerAuthToken=%q", cfg.WorkerAuthToken)
	}
}

func TestNewWorkerLoggerRejectsInvalidSettings(t *testing.T) {
	if _, _, _, err := newWorkerLogger("trace", "json", &bytes.Buffer{}); err == nil {
		t.Fatal("expected invalid log level to be rejected")
	}
	if _, _, _, err := newWorkerLogger("info", "console", &bytes.Buffer{}); err == nil {
		t.Fatal("expected invalid log format to be rejected")
	}
}

func TestNewWorkerLoggerSupportsTextFormat(t *testing.T) {
	var buf bytes.Buffer
	log, level, format, err := newWorkerLogger("info", "text", &buf)
	if err != nil {
		t.Fatalf("newWorkerLogger: %v", err)
	}
	if level != "INFO" {
		t.Fatalf("level=%q want %q", level, "INFO")
	}
	if format != "text" {
		t.Fatalf("format=%q want %q", format, "text")
	}

	log.Info("worker log test", slog.String("component", "worker"))
	got := buf.String()
	if !strings.Contains(got, "level=INFO") && !strings.Contains(got, "msg=\"worker log test\"") {
		t.Fatalf("expected text log output, got %q", got)
	}
	if strings.HasPrefix(strings.TrimSpace(got), "{") {
		t.Fatalf("expected text handler output, got %q", got)
	}
}
