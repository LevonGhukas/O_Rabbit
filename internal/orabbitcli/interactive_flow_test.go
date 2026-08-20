package orabbitcli

import (
	"context"
	"strings"
	"testing"
)

func TestIsRemoteMasterURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		base string
		want bool
	}{
		{base: "http://127.0.0.1:9100", want: false},
		{base: "http://localhost:9100", want: false},
		{base: "http://[::1]:9100", want: false},
		{base: "http://master.example.internal:9100", want: true},
		{base: "master.example.internal:9100", want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.base, func(t *testing.T) {
			t.Parallel()
			if got := isRemoteMasterURL(tt.base); got != tt.want {
				t.Fatalf("isRemoteMasterURL(%q)=%v want=%v", tt.base, got, tt.want)
			}
		})
	}
}

func TestDefaultStartLocalWorkers(t *testing.T) {
	t.Parallel()

	if got := defaultStartLocalWorkers(defaultInteractiveTestConfig()); !got {
		t.Fatalf("defaultStartLocalWorkers(local stack)=%v want=true", got)
	}

	remote := defaultInteractiveTestConfig()
	remote.StartStack = false
	remote.HTTPBase = "http://master.example.internal:9100"
	if got := defaultStartLocalWorkers(remote); got {
		t.Fatalf("defaultStartLocalWorkers(remote master)=%v want=false", got)
	}
}

func TestPromptRunConfigNormalModeHidesAdvancedPrompts(t *testing.T) {
	t.Parallel()

	rw := newScriptedReadWriter(strings.Repeat("\n", 20))
	prompts := newPromptSessionFromReadWriter(rw)
	cfg := defaultInteractiveTestConfig()
	cfg.HTTPBase = "http://127.0.0.1:1"

	if err := promptRunConfig(context.Background(), prompts, &cfg, 0, false, false); err != nil {
		t.Fatalf("promptRunConfig returned error: %v", err)
	}

	if !cfg.AutoTune {
		t.Fatalf("AutoTune=%v want=true", cfg.AutoTune)
	}
	if cfg.MaxInFlightTasks != 0 || cfg.PlannedTasks != 0 || cfg.FetchLimit != 0 {
		t.Fatalf("auto-tune defaults changed unexpectedly: max_in_flight=%d planned_tasks=%d fetch_limit=%d", cfg.MaxInFlightTasks, cfg.PlannedTasks, cfg.FetchLimit)
	}
	if cfg.S3Region != "us-east-1" || cfg.S3Prefix != "" || !cfg.S3ForcePathStyle {
		t.Fatalf("hidden storage defaults changed unexpectedly: region=%q prefix=%q force_path_style=%v", cfg.S3Region, cfg.S3Prefix, cfg.S3ForcePathStyle)
	}
	if !cfg.StartLocalWorkers {
		t.Fatalf("StartLocalWorkers=%v want=true for local normal mode", cfg.StartLocalWorkers)
	}

	output := rw.Output()
	for _, want := range []string{
		"Source database type",
		"SQL Server connection string",
		"Source table",
		"Cursor / ordering column",
		"Export only new rows after previous run?",
		"S3 / MinIO endpoint URL",
		"Target bucket",
		"Access key ID",
		"Secret access key",
		"Register output as an Iceberg table?",
		"Iceberg REST catalog URI",
		"Iceberg destination table",
		"Automatic performance tuning: enabled",
		"Submit run?",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("normal-mode prompt output missing %q:\n%s", want, output)
		}
	}
	for _, hidden := range []string{
		"S3 region",
		"S3 prefix override",
		"S3 force path style",
		"Start local worker processes on this machine?",
		"Use automatic performance tuning?",
		"max_in_flight_tasks",
		"planned_tasks",
		"fetch_limit_rows",
		"Iceberg engine",
		"config file",
	} {
		if strings.Contains(output, hidden) {
			t.Fatalf("normal-mode prompt output unexpectedly contained %q:\n%s", hidden, output)
		}
	}
}

func TestPromptRunConfigAdvancedModeShowsAdvancedPrompts(t *testing.T) {
	t.Parallel()

	rw := newScriptedReadWriter(strings.Repeat("\n", 30))
	prompts := newPromptSessionFromReadWriter(rw)
	cfg := defaultInteractiveTestConfig()
	cfg.HTTPBase = "http://127.0.0.1:1"

	if err := promptRunConfig(context.Background(), prompts, &cfg, 0, false, true); err != nil {
		t.Fatalf("promptRunConfig returned error: %v", err)
	}

	output := rw.Output()
	for _, want := range []string{
		"S3 region",
		"S3 prefix override",
		"S3 force path style",
		"Start local worker processes on this machine?",
		"Use automatic performance tuning?",
		"Iceberg engine",
		"Iceberg defaults file (optional)",
		"Iceberg REST catalog URI",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("advanced prompt output missing %q:\n%s", want, output)
		}
	}
}

func TestPromptRunConfigAdvancedManualTuningPreserved(t *testing.T) {
	t.Parallel()

	input := strings.Join([]string{
		"",  // source engine
		"",  // source dsn
		"",  // source table
		"",  // cursor column
		"",  // incremental
		"",  // endpoint
		"",  // bucket
		"",  // access key
		"",  // secret access key
		"",  // region
		"",  // prefix override
		"",  // force path style
		"",  // start local workers
		"n", // automatic performance tuning
		"3", // local worker processes
		"8", // max_in_flight_tasks
		"16",
		"500000",
		"n", // iceberg
		"",  // submit
	}, "\n") + "\n"

	rw := newScriptedReadWriter(input)
	prompts := newPromptSessionFromReadWriter(rw)
	cfg := defaultInteractiveTestConfig()
	cfg.HTTPBase = "http://127.0.0.1:1"

	if err := promptRunConfig(context.Background(), prompts, &cfg, 0, false, true); err != nil {
		t.Fatalf("promptRunConfig returned error: %v", err)
	}

	if cfg.AutoTune {
		t.Fatalf("AutoTune=%v want=false", cfg.AutoTune)
	}
	if cfg.Workers != 3 || cfg.MaxInFlightTasks != 8 || cfg.PlannedTasks != 16 || cfg.FetchLimit != 500000 {
		t.Fatalf("manual tuning not preserved: workers=%d max_in_flight=%d planned_tasks=%d fetch_limit=%d", cfg.Workers, cfg.MaxInFlightTasks, cfg.PlannedTasks, cfg.FetchLimit)
	}
	if cfg.AutoIceberg {
		t.Fatalf("AutoIceberg=%v want=false", cfg.AutoIceberg)
	}
}

func TestPromptRunConfigNormalModeBuildsExpectedAutoTunePayloadDefaults(t *testing.T) {
	t.Parallel()

	rw := newScriptedReadWriter(strings.Repeat("\n", 20))
	prompts := newPromptSessionFromReadWriter(rw)
	cfg := defaultInteractiveTestConfig()
	cfg.HTTPBase = "http://127.0.0.1:1"

	if err := promptRunConfig(context.Background(), prompts, &cfg, 0, false, false); err != nil {
		t.Fatalf("promptRunConfig returned error: %v", err)
	}

	payload, err := buildJobPayloadFromConfig(cfg, "src-1", "tgt-1")
	if err != nil {
		t.Fatalf("buildJobPayloadFromConfig returned error: %v", err)
	}

	if got := payload.OptionsJSON["auto_tune"]; got != true {
		t.Fatalf("auto_tune=%v want=true", got)
	}
	if got := int(payload.OptionsJSON["max_in_flight_tasks"].(int)); got != 0 {
		t.Fatalf("max_in_flight_tasks=%d want=0", got)
	}
	if got := int(payload.OptionsJSON["planned_tasks"].(int)); got != 0 {
		t.Fatalf("planned_tasks=%d want=0", got)
	}
	if got := int(payload.OptionsJSON["fetch_limit_rows"].(int)); got != 0 {
		t.Fatalf("fetch_limit_rows=%d want=0", got)
	}
	if payload.HWMColumn != cfg.IDColumn {
		t.Fatalf("HWMColumn=%q want=%q", payload.HWMColumn, cfg.IDColumn)
	}
}

func defaultInteractiveTestConfig() ranConfig {
	return ranConfig{
		HTTPBase:          "http://127.0.0.1:9100",
		GRPCAddr:          "127.0.0.1:9102",
		StartStack:        true,
		StartLocalWorkers: true,
		Workers:           10,

		AutoIceberg:   true,
		IcebergEngine: "rest-go",
		IceConfig:     ".ice.yaml",
		IceTable:      "",

		SourceName:   "mssql",
		SourceEngine: "mssql",
		SourceDSN:    "sqlserver://sa:YourStrong(!)Password@localhost:1433?database=master&encrypt=disable&trustServerCertificate=true",
		SourceSQL:    "",

		TargetName:        "s3",
		S3Endpoint:        "http://localhost:9000",
		S3Region:          "us-east-1",
		S3Bucket:          "bucket1",
		S3Prefix:          "",
		S3ForcePathStyle:  true,
		S3AccessKeyID:     "minioadmin",
		S3SecretAccessKey: "minioadmin",

		JobName:         "export",
		TargetNamespace: "orders",
		TargetTable:     "Orders",
		WriteMode:       "append",
		Incremental:     false,
		Table:           "SalesDB.dbo.BigTable4",
		IDColumn:        "RowId",
		PlannedTasks:    2,
		ChunkSize:       105000,
		FetchLimit:      50000,

		AutoTune:          true,
		MaxInFlightTasks:  0,
		TargetRowsPerTask: 200000,
	}
}
