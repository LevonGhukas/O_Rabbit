package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/envutil"
)

func (c masterConfig) validateLeasePolicy() error {
	if c.TaskLeaseDuration < 3*time.Second {
		return fmt.Errorf("task lease duration must be at least 3s")
	}
	if c.TaskLeaseScanInterval <= 0 || c.TaskLeaseScanInterval >= c.TaskLeaseDuration {
		return fmt.Errorf("task lease scan interval must be positive and shorter than lease duration")
	}
	if c.TaskMaxAttempts <= 0 {
		return fmt.Errorf("task max attempts must be positive")
	}
	if c.MaxActiveRuns <= 0 || c.MaxActiveTasks <= 0 || c.CatalogWorkLimit <= 0 || c.UploadCapacityLimit <= 0 {
		return fmt.Errorf("global active-run, active-task, catalog-work, and upload-capacity limits must be positive")
	}
	if c.UploadCapacityLeaseTTL < 3*time.Second {
		return fmt.Errorf("upload capacity lease TTL must be at least 3s")
	}
	if c.TaskRetryBackoff <= 0 || c.TaskRetryBackoffMax < c.TaskRetryBackoff {
		return fmt.Errorf("task retry backoff must be positive and no greater than its maximum")
	}
	if c.LeadershipLeaseDuration < 3*time.Second {
		return fmt.Errorf("leadership lease duration must be at least 3s")
	}
	if c.LeadershipRenewInterval <= 0 || c.LeadershipRenewInterval >= c.LeadershipLeaseDuration {
		return fmt.Errorf("leadership renewal interval must be positive and shorter than leadership lease duration")
	}
	if c.MultipartCleanupScanInterval <= 0 || c.MultipartAbandonmentGrace <= 0 || c.MultipartCleanupMaxAttempts <= 0 {
		return fmt.Errorf("multipart cleanup scan, grace, and max attempts must be positive")
	}
	if c.CanceledObjectCleanupScanInterval <= 0 || c.CanceledObjectRetention <= 0 || c.CanceledObjectCleanupMaxAttempts <= 0 {
		return fmt.Errorf("canceled-object cleanup scan, retention, and max attempts must be positive")
	}
	return nil
}

func (c masterConfig) validateAuthentication() error {
	if !isLoopbackListenAddress(c.GRPCAddr) && strings.TrimSpace(c.WorkerAuthToken) == "" {
		return fmt.Errorf("remote gRPC listen address %q requires ORABBIT_WORKER_AUTH_TOKEN", c.GRPCAddr)
	}
	if !isLoopbackListenAddress(c.HTTPAddr) && strings.TrimSpace(c.HTTPAuthToken) == "" {
		return fmt.Errorf("remote HTTP listen address %q requires ORABBIT_HTTP_AUTH_TOKEN", c.HTTPAddr)
	}
	return nil
}

func isLoopbackListenAddress(addr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type masterConfig struct {
	DBPath          string
	GRPCAddr        string
	HTTPAddr        string
	HTTPAuthToken   string
	WorkerAuthToken string
	IceBin          string

	Insecure bool
	TLSCert  string
	TLSKey   string

	LogLevel                          string
	LogFormat                         string
	TaskLeaseDuration                 time.Duration
	TaskLeaseScanInterval             time.Duration
	TaskMaxAttempts                   int
	MaxActiveRuns                     int
	MaxActiveTasks                    int
	CatalogWorkLimit                  int
	UploadCapacityLimit               int
	UploadCapacityLeaseTTL            time.Duration
	TaskRetryBackoff                  time.Duration
	TaskRetryBackoffMax               time.Duration
	LeadershipLeaseDuration           time.Duration
	LeadershipRenewInterval           time.Duration
	MultipartCleanupScanInterval      time.Duration
	MultipartAbandonmentGrace         time.Duration
	MultipartCleanupMaxAttempts       int
	CanceledObjectCleanupScanInterval time.Duration
	CanceledObjectRetention           time.Duration
	CanceledObjectCleanupMaxAttempts  int
	CanceledObjectCleanupDryRun       bool
}

func loadMasterConfigFromEnv() masterConfig {
	return masterConfig{
		DBPath:                            envutil.EnvOrDefault("ORABBIT_DB_PATH", "./master.sqlite"),
		GRPCAddr:                          envutil.EnvOrDefault("ORABBIT_GRPC_ADDR", "127.0.0.1:9102"),
		HTTPAddr:                          envutil.EnvOrDefault("ORABBIT_HTTP_ADDR", "127.0.0.1:9100"),
		HTTPAuthToken:                     strings.TrimSpace(os.Getenv("ORABBIT_HTTP_AUTH_TOKEN")),
		WorkerAuthToken:                   strings.TrimSpace(os.Getenv("ORABBIT_WORKER_AUTH_TOKEN")),
		IceBin:                            envutil.EnvOrDefault("ORABBIT_ICE_BIN", "ice"),
		Insecure:                          envBoolDefault("ORABBIT_GRPC_INSECURE", true),
		TLSCert:                           strings.TrimSpace(os.Getenv("ORABBIT_TLS_CERT_FILE")),
		TLSKey:                            strings.TrimSpace(os.Getenv("ORABBIT_TLS_KEY_FILE")),
		LogLevel:                          envutil.EnvOrDefault("ORABBIT_LOG_LEVEL", "INFO"),
		LogFormat:                         envutil.EnvOrDefault("ORABBIT_LOG_FORMAT", "json"),
		TaskLeaseDuration:                 envDurationDefault("ORABBIT_TASK_LEASE_DURATION", 30*time.Second),
		TaskLeaseScanInterval:             envDurationDefault("ORABBIT_TASK_LEASE_SCAN_INTERVAL", 5*time.Second),
		TaskMaxAttempts:                   envPositiveIntDefault("ORABBIT_TASK_MAX_ATTEMPTS", 3),
		MaxActiveRuns:                     envPositiveIntDefault("ORABBIT_MAX_ACTIVE_RUNS", 16),
		MaxActiveTasks:                    envPositiveIntDefault("ORABBIT_MAX_ACTIVE_TASKS", 64),
		CatalogWorkLimit:                  envPositiveIntDefault("ORABBIT_CATALOG_WORK_LIMIT", 2),
		UploadCapacityLimit:               envPositiveIntDefault("ORABBIT_UPLOAD_CAPACITY_LIMIT", 8),
		UploadCapacityLeaseTTL:            envDurationDefault("ORABBIT_UPLOAD_CAPACITY_LEASE_TTL", 2*time.Minute),
		TaskRetryBackoff:                  envDurationDefault("ORABBIT_TASK_RETRY_BACKOFF", time.Second),
		TaskRetryBackoffMax:               envDurationDefault("ORABBIT_TASK_RETRY_BACKOFF_MAX", 30*time.Second),
		LeadershipLeaseDuration:           envDurationDefault("ORABBIT_LEADERSHIP_LEASE_DURATION", 15*time.Second),
		LeadershipRenewInterval:           envDurationDefault("ORABBIT_LEADERSHIP_RENEW_INTERVAL", 5*time.Second),
		MultipartCleanupScanInterval:      envDurationDefault("ORABBIT_MULTIPART_CLEANUP_SCAN_INTERVAL", time.Minute),
		MultipartAbandonmentGrace:         envDurationDefault("ORABBIT_MULTIPART_ABANDONMENT_GRACE", 15*time.Minute),
		MultipartCleanupMaxAttempts:       envPositiveIntDefault("ORABBIT_MULTIPART_CLEANUP_MAX_ATTEMPTS", 5),
		CanceledObjectCleanupScanInterval: envDurationDefault("ORABBIT_CANCELED_OBJECT_CLEANUP_SCAN_INTERVAL", 5*time.Minute),
		CanceledObjectRetention:           envDurationDefault("ORABBIT_CANCELED_OBJECT_RETENTION", 7*24*time.Hour),
		CanceledObjectCleanupMaxAttempts:  envPositiveIntDefault("ORABBIT_CANCELED_OBJECT_CLEANUP_MAX_ATTEMPTS", 5),
		CanceledObjectCleanupDryRun:       envBoolDefault("ORABBIT_CANCELED_OBJECT_CLEANUP_DRY_RUN", true),
	}
}

func bindMasterFlags(cfg *masterConfig) {
	flag.StringVar(&cfg.DBPath, "db", cfg.DBPath, "SQLite DB path")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	flag.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "HTTP listen address")
	flag.StringVar(&cfg.HTTPAuthToken, "http-auth-token", cfg.HTTPAuthToken, "HTTP bearer token for API/SSE (required for non-loopback HTTP)")
	flag.StringVar(&cfg.WorkerAuthToken, "worker-auth-token", cfg.WorkerAuthToken, "Bearer token required on worker gRPC calls (required for non-loopback gRPC)")
	flag.StringVar(&cfg.IceBin, "ice-bin", cfg.IceBin, "Ice CLI binary path for master-owned engine=ice registration")
	flag.BoolVar(&cfg.Insecure, "insecure", cfg.Insecure, "Disable gRPC TLS (dev)")
	flag.StringVar(&cfg.TLSCert, "tls-cert", cfg.TLSCert, "gRPC TLS cert file (or ORABBIT_TLS_CERT_FILE)")
	flag.StringVar(&cfg.TLSKey, "tls-key", cfg.TLSKey, "gRPC TLS key file (or ORABBIT_TLS_KEY_FILE)")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: DEBUG, INFO, WARN, ERROR")
	flag.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "Log format: json or text")
	flag.DurationVar(&cfg.TaskLeaseDuration, "task-lease-duration", cfg.TaskLeaseDuration, "Task attempt lease duration")
	flag.DurationVar(&cfg.TaskLeaseScanInterval, "task-lease-scan-interval", cfg.TaskLeaseScanInterval, "Expired task lease scan interval")
	flag.IntVar(&cfg.TaskMaxAttempts, "task-max-attempts", cfg.TaskMaxAttempts, "Maximum attempts per logical task")
	flag.IntVar(&cfg.MaxActiveRuns, "max-active-runs", cfg.MaxActiveRuns, "Global maximum active runs")
	flag.IntVar(&cfg.MaxActiveTasks, "max-active-tasks", cfg.MaxActiveTasks, "Global maximum active task attempts")
	flag.IntVar(&cfg.CatalogWorkLimit, "catalog-work-limit", cfg.CatalogWorkLimit, "Global concurrent registration/reconciliation limit")
	flag.IntVar(&cfg.UploadCapacityLimit, "upload-capacity-limit", cfg.UploadCapacityLimit, "Global concurrent object-upload task limit")
	flag.DurationVar(&cfg.UploadCapacityLeaseTTL, "upload-capacity-lease-ttl", cfg.UploadCapacityLeaseTTL, "Object-upload capacity lease duration")
	flag.DurationVar(&cfg.TaskRetryBackoff, "task-retry-backoff", cfg.TaskRetryBackoff, "Initial task retry backoff")
	flag.DurationVar(&cfg.TaskRetryBackoffMax, "task-retry-backoff-max", cfg.TaskRetryBackoffMax, "Maximum task retry backoff")
	flag.DurationVar(&cfg.LeadershipLeaseDuration, "leadership-lease-duration", cfg.LeadershipLeaseDuration, "Durable master leadership lease duration")
	flag.DurationVar(&cfg.LeadershipRenewInterval, "leadership-renew-interval", cfg.LeadershipRenewInterval, "Master leadership renewal interval")
	flag.DurationVar(&cfg.MultipartCleanupScanInterval, "multipart-cleanup-scan-interval", cfg.MultipartCleanupScanInterval, "Abandoned multipart cleanup scan interval")
	flag.DurationVar(&cfg.MultipartAbandonmentGrace, "multipart-abandonment-grace", cfg.MultipartAbandonmentGrace, "Grace period after multipart ownership loss")
	flag.IntVar(&cfg.MultipartCleanupMaxAttempts, "multipart-cleanup-max-attempts", cfg.MultipartCleanupMaxAttempts, "Maximum automatic multipart cleanup attempts")
	flag.DurationVar(&cfg.CanceledObjectCleanupScanInterval, "canceled-object-cleanup-scan-interval", cfg.CanceledObjectCleanupScanInterval, "Canceled-object cleanup scan interval")
	flag.DurationVar(&cfg.CanceledObjectRetention, "canceled-object-retention", cfg.CanceledObjectRetention, "Quarantine period before canceled-object cleanup")
	flag.IntVar(&cfg.CanceledObjectCleanupMaxAttempts, "canceled-object-cleanup-max-attempts", cfg.CanceledObjectCleanupMaxAttempts, "Maximum automatic canceled-object cleanup attempts")
	flag.BoolVar(&cfg.CanceledObjectCleanupDryRun, "canceled-object-cleanup-dry-run", cfg.CanceledObjectCleanupDryRun, "Inspect and report canceled objects without deleting them")
}

func envDurationDefault(key string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(strings.TrimSpace(os.Getenv(key)))
	if err != nil || v <= 0 {
		return def
	}
	return v
}

func envPositiveIntDefault(key string, def int) int {
	if v, ok := envutil.ParsePositiveInt(os.Getenv(key)); ok {
		return v
	}
	return def
}

func newMasterLogger(levelRaw, formatRaw string) *slog.Logger {
	level := parseLogLevel(levelRaw)
	opts := &slog.HandlerOptions{Level: level}

	format := strings.ToLower(strings.TrimSpace(formatRaw))
	if format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func parseLogLevel(raw string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func envBoolDefault(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if parsed, ok := envutil.ParseBoolEnv(v); ok {
		return parsed
	}
	return def
}
