package orabbitcli

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/envutil"
	"github.com/LevonGhukas/O_Rabbit/internal/sysinfo"
)

const envAutoMaxInFlight = "ORABBIT_AUTO_MAX_IN_FLIGHT"

type ranConfig struct {
	HTTPBase string
	GRPCAddr string

	StartStack        bool
	StartLocalWorkers bool
	Workers           int

	DBPath    string
	GOCache   string
	MasterBin string
	WorkerBin string

	WorkerLogLevel  string
	WorkerLogFormat string

	// Optional: automatically register uploaded Parquet parts into an Iceberg catalog.
	AutoIceberg   bool
	IcebergEngine string // rest-go (default) or ice
	IceBin        string // deprecated: master uses its in-container `ice` binary
	IceConfig     string // e.g. '.ice.yaml' (persisted as a per-run registration snapshot)
	IceTable      string // e.g. 'mssql.Orders'

	// Source
	SourceName   string
	SourceEngine string
	SourceDSN    string
	SourceSQL    string

	// Target
	TargetName        string
	S3Endpoint        string
	S3Region          string
	S3Bucket          string
	S3Prefix          string
	S3ForcePathStyle  bool
	S3AccessKeyID     string
	S3SecretAccessKey string

	// Job
	JobName         string
	TargetNamespace string
	TargetTable     string
	WriteMode       string
	Incremental     bool
	Table           string
	IDColumn        string
	PlannedTasks    int
	ChunkSize       int64
	FetchLimit      int

	// Planning
	AutoTune          bool
	MaxInFlightTasks  int
	TargetRowsPerTask int64
}

// defaultJobName handles default job name behavior.
// It exists to keep this logic isolated and reusable.
func defaultJobName(cfg ranConfig) string {
	// Stable job identity used for upsert in master SQLite.
	// Keep it short-ish but unique per (engine, table).
	t := strings.TrimSpace(cfg.Table)
	if t == "" {
		return "export"
	}
	// Normalize common SQL identifier forms.
	t = strings.ReplaceAll(t, "[", "")
	t = strings.ReplaceAll(t, "]", "")
	t = strings.ReplaceAll(t, ".", "_")
	t = strings.ReplaceAll(t, "/", "_")
	if len(t) > 80 {
		t = t[:80]
	}
	engine := normalizeSourceEngine(cfg.SourceEngine)
	return engine + "_" + t
}

// normalizeSourceEngine handles normalize source engine behavior.
// It exists to keep source-engine validation centralized.
func normalizeSourceEngine(raw string) string {
	n := connectors.NormalizeSourceEngine(raw)
	if strings.TrimSpace(n) == "" {
		return "mssql"
	}
	return n
}

func sourceEnginePromptLabel() string {
	return "Source database type"
}

func sourceEnginePromptNote() string {
	engines := connectors.KnownSourceEngines()
	if len(engines) == 0 {
		return ""
	}
	return fmt.Sprintf("Choices: %s.", strings.Join(engines, ", "))
}

func defaultSourceDSN(engine, current string) string {
	cur := strings.TrimSpace(current)
	switch normalizeSourceEngine(engine) {
	case "clickhouse":
		if cur == "" || strings.HasPrefix(strings.ToLower(cur), "sqlserver://") {
			return "clickhouse://myuser:mypassword@localhost:19000/default?dial_timeout=10s&compress=lz4"
		}
	case "oracle":
		if cur == "" || strings.HasPrefix(strings.ToLower(cur), "sqlserver://") {
			return "oracle://user:password@localhost:1521/ORCLCDB"
		}
	case "postgres":
		if cur == "" || strings.HasPrefix(strings.ToLower(cur), "sqlserver://") {
			return "postgresql://myuser:mypassword@localhost:5432/mydb?sslmode=disable"
		}
	case "mssql":
		if cur == "" || !strings.HasPrefix(strings.ToLower(cur), "sqlserver://") {
			return "sqlserver://sa:YourStrong(!)Password@localhost:1433?database=master&encrypt=disable&trustServerCertificate=true"
		}
	case "flightsql":
		if cur == "" || strings.HasPrefix(strings.ToLower(cur), "sqlserver://") {
			return "grpc+tcp://localhost:32010"
		}
	default:
		if strings.HasPrefix(strings.ToLower(cur), "sqlserver://") {
			return ""
		}
	}
	return cur
}

func sourceDSNPromptLabel(engine string) string {
	switch normalizeSourceEngine(engine) {
	case "clickhouse":
		return "ClickHouse connection URL"
	case "oracle":
		return "Oracle connection URL"
	case "postgres":
		return "PostgreSQL connection URL"
	case "mssql":
		return "SQL Server connection string"
	case "flightsql":
		return "FlightSQL connection URL"
	default:
		return fmt.Sprintf("%s connection URL", strings.ToUpper(normalizeSourceEngine(engine)))
	}
}

func sourceDSNPromptNote(engine string) string {
	switch normalizeSourceEngine(engine) {
	case "oracle":
		return "Must be reachable from all workers. For SID-based connections, use a driver-supported parameter such as oracle://user:password@host:1521/?SID=ORCL."
	case "flightsql":
		return "Must be reachable from all workers."
	default:
		return "Must be reachable from all workers."
	}
}

// autoDefaultMaxInFlight handles auto default max in flight behavior.
// It exists to keep this logic isolated and reusable.
func autoDefaultMaxInFlight() int {
	// Optional operator override.
	if n := envPositiveInt(envAutoMaxInFlight); n > 0 {
		return n
	}

	// Keep in sync with planner auto-tune defaults.
	cpuCap := runtime.NumCPU()
	if cpuCap < 2 {
		cpuCap = 2
	}
	ramCap := 32
	if memBytes, ok := sysinfo.TotalMemoryBytes(); ok {
		const perTask = 1536 * 1024 * 1024 // ~1.5GiB
		ramCap = int(memBytes / perTask)
		if ramCap < 2 {
			ramCap = 2
		}
	}
	n := cpuCap
	if ramCap < n {
		n = ramCap
	}
	if n > 32 {
		n = 32
	}
	return n
}

// desiredWorkers handles desired workers behavior.
// It exists to keep this logic isolated and reusable.
func desiredWorkers(taskCount int, maxInFlightTasks int) int {
	if taskCount <= 0 {
		taskCount = 1
	}
	d := maxInFlightTasks
	if d <= 0 {
		d = autoDefaultMaxInFlight()
	}
	if taskCount < d {
		d = taskCount
	}
	if d < 1 {
		d = 1
	}
	return d
}

// parseLocalWorkerNum parses local worker num.
// It exists to keep conversion and validation in one place.
func parseLocalWorkerNum(id string) (int, bool) {
	id = strings.TrimSpace(id)
	if !strings.HasPrefix(id, "local-") {
		return 0, false
	}
	ns := strings.TrimPrefix(id, "local-")
	n, err := strconv.Atoi(ns)
	if err != nil {
		return 0, false
	}
	return n, true
}

func envPositiveInt(key string) int {
	if n, ok := envutil.ParsePositiveInt(os.Getenv(key)); ok {
		return n
	}
	return 0
}
