package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/envutil"
	"github.com/LevonGhukas/O_Rabbit/internal/workerworkspace"
)

type workerConfig struct {
	MasterAddr           string
	WorkerID             string
	WorkerAddr           string
	InsecureGRPC         bool
	TLSCAFile            string
	TLSServerName        string
	WorkerAuthToken      string
	Poll                 time.Duration
	LogLevel             string
	LogFormat            string
	TempRoot             string
	TempScanInterval     time.Duration
	TempUnlockedGrace    time.Duration
	TempOfflineRetention time.Duration
	TempMaxEntries       int
	TempMaxBytesPerScan  int64
	TempMinFreeBytes     uint64
	TempMaxManagedBytes  int64
	TempDryRun           bool
}

func loadWorkerConfigFromEnv() workerConfig {
	return workerConfig{
		MasterAddr:           "localhost:9102",
		WorkerID:             "",
		WorkerAddr:           "",
		InsecureGRPC:         true,
		TLSCAFile:            "",
		TLSServerName:        "",
		WorkerAuthToken:      strings.TrimSpace(os.Getenv("ORABBIT_WORKER_AUTH_TOKEN")),
		Poll:                 2 * time.Second,
		LogLevel:             envutil.EnvOrDefault("ORABBIT_LOG_LEVEL", "INFO"),
		LogFormat:            envutil.EnvOrDefault("ORABBIT_LOG_FORMAT", "json"),
		TempRoot:             envutil.EnvOrDefault("ORABBIT_WORKER_TEMP_ROOT", workerworkspace.DefaultRoot()),
		TempScanInterval:     workerEnvDuration("ORABBIT_TEMP_SCAN_INTERVAL", 5*time.Minute),
		TempUnlockedGrace:    workerEnvDuration("ORABBIT_TEMP_UNLOCKED_GRACE", 30*time.Minute),
		TempOfflineRetention: workerEnvDuration("ORABBIT_TEMP_OFFLINE_RETENTION", 7*24*time.Hour),
		TempMaxEntries:       workerEnvInt("ORABBIT_TEMP_MAX_ENTRIES", 100),
		TempMaxBytesPerScan:  int64(workerEnvUint("ORABBIT_TEMP_MAX_BYTES_PER_SCAN", 10<<30)),
		TempMinFreeBytes:     workerEnvUint("ORABBIT_TEMP_MIN_FREE_BYTES", 1<<30),
		TempMaxManagedBytes:  int64(workerEnvUint("ORABBIT_TEMP_MAX_MANAGED_BYTES", 100<<30)),
		TempDryRun:           workerEnvBool("ORABBIT_TEMP_DRY_RUN", false),
	}
}

func newWorkerFlagSet(cfg *workerConfig) *flag.FlagSet {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	fs.StringVar(&cfg.MasterAddr, "master", cfg.MasterAddr, "Master gRPC address")
	fs.StringVar(&cfg.WorkerID, "worker-id", cfg.WorkerID, "Worker ID (optional; master can assign)")
	fs.StringVar(&cfg.WorkerAddr, "worker-addr", cfg.WorkerAddr, "Address advertised to master (for observability)")
	fs.BoolVar(&cfg.InsecureGRPC, "insecure", cfg.InsecureGRPC, "Disable gRPC TLS (dev)")
	fs.StringVar(&cfg.TLSCAFile, "tls-ca", cfg.TLSCAFile, "CA certificate file for master gRPC TLS")
	fs.StringVar(&cfg.TLSServerName, "tls-server-name", cfg.TLSServerName, "Expected TLS server name (optional)")
	fs.StringVar(&cfg.WorkerAuthToken, "worker-auth-token", cfg.WorkerAuthToken, "Bearer token for worker gRPC calls (or ORABBIT_WORKER_AUTH_TOKEN)")
	fs.DurationVar(&cfg.Poll, "poll", cfg.Poll, "Poll interval when no tasks")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: DEBUG, INFO, WARN, ERROR (or ORABBIT_LOG_LEVEL)")
	fs.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "Log format: json or text (or ORABBIT_LOG_FORMAT)")
	fs.StringVar(&cfg.TempRoot, "temp-root", cfg.TempRoot, "Managed worker temporary root")
	fs.DurationVar(&cfg.TempScanInterval, "temp-scan-interval", cfg.TempScanInterval, "Managed workspace scavenger interval")
	fs.DurationVar(&cfg.TempUnlockedGrace, "temp-unlocked-grace", cfg.TempUnlockedGrace, "Grace period for unlocked workspaces")
	fs.DurationVar(&cfg.TempOfflineRetention, "temp-offline-retention", cfg.TempOfflineRetention, "Conservative offline retention before cleanup")
	fs.IntVar(&cfg.TempMaxEntries, "temp-max-entries", cfg.TempMaxEntries, "Maximum workspace entries inspected per scan")
	fs.Int64Var(&cfg.TempMaxBytesPerScan, "temp-max-bytes-per-scan", cfg.TempMaxBytesPerScan, "Maximum managed bytes reclaimed per scan")
	fs.Uint64Var(&cfg.TempMinFreeBytes, "temp-min-free-bytes", cfg.TempMinFreeBytes, "Minimum disk bytes required to start local-file work")
	fs.Int64Var(&cfg.TempMaxManagedBytes, "temp-max-managed-bytes", cfg.TempMaxManagedBytes, "Maximum managed-root bytes before new work pauses")
	fs.BoolVar(&cfg.TempDryRun, "temp-dry-run", cfg.TempDryRun, "Classify stale workspaces without deleting them")
	return fs
}

func workerEnvDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func workerEnvUint(key string, fallback uint64) uint64 {
	value, err := strconv.ParseUint(strings.TrimSpace(os.Getenv(key)), 10, 64)
	if err != nil || value == 0 {
		return fallback
	}
	return value
}

func workerEnvInt(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func workerEnvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func newWorkerLogger(levelRaw, formatRaw string, w io.Writer) (*slog.Logger, string, string, error) {
	levelName, level, err := parseWorkerLogLevel(levelRaw)
	if err != nil {
		return nil, "", "", err
	}
	formatName, err := parseWorkerLogFormat(formatRaw)
	if err != nil {
		return nil, "", "", err
	}

	opts := &slog.HandlerOptions{Level: level}
	if formatName == "text" {
		return slog.New(slog.NewTextHandler(w, opts)), levelName, formatName, nil
	}
	return slog.New(slog.NewJSONHandler(w, opts)), levelName, formatName, nil
}

func parseWorkerLogLevel(raw string) (string, slog.Level, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", "INFO":
		return "INFO", slog.LevelInfo, nil
	case "DEBUG":
		return "DEBUG", slog.LevelDebug, nil
	case "WARN", "WARNING":
		return "WARN", slog.LevelWarn, nil
	case "ERROR":
		return "ERROR", slog.LevelError, nil
	default:
		return "", 0, fmt.Errorf("invalid worker log level %q: use DEBUG, INFO, WARN, or ERROR", strings.TrimSpace(raw))
	}
}

func parseWorkerLogFormat(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "json":
		return "json", nil
	case "text":
		return "text", nil
	default:
		return "", fmt.Errorf("invalid worker log format %q: use json or text", strings.TrimSpace(raw))
	}
}
