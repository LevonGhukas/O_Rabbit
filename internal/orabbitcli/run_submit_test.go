package orabbitcli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
)

func TestCmdRunSubmitFromFile(t *testing.T) {
	type capturedConnection struct {
		Name     string         `json:"name"`
		Kind     string         `json:"kind"`
		Engine   string         `json:"engine"`
		Metadata map[string]any `json:"metadata"`
		Secret   map[string]any `json:"secret"`
	}
	type capturedJob struct {
		Name               string         `json:"name"`
		SourceConnectionID string         `json:"source_connection_id"`
		TargetConnectionID string         `json:"target_connection_id"`
		SourceSQL          string         `json:"source_sql"`
		TargetNamespace    string         `json:"target_namespace"`
		TargetTable        string         `json:"target_table"`
		WriteMode          string         `json:"write_mode"`
		Incremental        bool           `json:"incremental"`
		HWMColumn          string         `json:"hwm_column"`
		OptionsJSON        map[string]any `json:"options_json"`
	}

	var (
		mu          sync.Mutex
		connections []capturedConnection
		jobReq      capturedJob
		runReq      runStartPayload
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/connections":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `[]`)
		case r.Method == http.MethodPost && r.URL.Path == "/connections":
			var req capturedConnection
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode connection request: %v", err)
			}
			mu.Lock()
			connections = append(connections, req)
			idx := len(connections)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"conn-%d"}`, idx)
		case r.Method == http.MethodGet && r.URL.Path == "/jobs":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `[]`)
		case r.Method == http.MethodPost && r.URL.Path == "/jobs":
			if err := json.NewDecoder(r.Body).Decode(&jobReq); err != nil {
				t.Fatalf("decode job request: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"id":"job-1"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/jobs/job-1/runs":
			if err := json.NewDecoder(r.Body).Decode(&runReq); err != nil {
				t.Fatalf("decode run request: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"run":{"id":"run-1"},"tasks":[{},{}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	specPath := writeTempRunSubmitFile(t, "run-submit-*.yaml", fmt.Sprintf(`
master:
  http: %s
source:
  name: mssql-prod
  engine: mssql
  dsn: sqlserver://sa:pass@example:1433?database=SalesDB
target:
  name: minio-lake
  endpoint: http://localhost:9000
  region: us-east-1
  bucket: bucket1
  prefix: exports/orders
  force_path_style: true
  access_key_id: minioadmin
  secret_access_key: minioadmin
job:
  name: orders-export
  target_namespace: analytics
  target_table: orders
  write_mode: append
  incremental: false
  table: SalesDB.dbo.Orders
  id_column: RowId
  auto_tune: true
  max_in_flight_tasks: 8
  target_rows_per_task: 200000
iceberg:
  enabled: true
  engine: rest-go
  table: analytics.orders
  options:
    uri: http://catalog:8181
    partition_spec:
      - source: CreatedAt
        transform: day
    schema_evolution: additive
    target_file_size: 134217728
    distribution_mode: hash
    metrics_mode: full
    metadata_retention:
      min_snapshots_to_keep: 3
    upsert:
      enabled: true
      keys: [RowId]
      mode: merge-on-read
    credential_vending:
      enabled: true
      required: true
`, server.URL))

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdRun(context.Background(), []string{"submit", "--file", specPath})
	})

	if code != exitSuccess {
		t.Fatalf("exit code=%d want=%d stderr=%q stdout=%q", code, exitSuccess, stderr, stdout)
	}
	if !strings.Contains(stdout, "submitted run run-1") {
		t.Fatalf("expected stdout to contain submitted run line, got %q", stdout)
	}
	if !strings.Contains(stdout, "watch with: "+CLIName+" run watch run-1") {
		t.Fatalf("expected stdout watch guidance, got %q", stdout)
	}
	if !strings.Contains(stderr, "[client] submitting run via "+server.URL) {
		t.Fatalf("expected stderr to contain submit progress line, got %q", stderr)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(connections) != 2 {
		t.Fatalf("connection upserts=%d want=2", len(connections))
	}
	if connections[0].Name != "mssql-prod" || connections[0].Kind != "source" || connections[0].Engine != "mssql" {
		t.Fatalf("unexpected source connection payload: %+v", connections[0])
	}
	if connections[1].Name != "minio-lake" || connections[1].Kind != "target" || connections[1].Engine != "s3" {
		t.Fatalf("unexpected target connection payload: %+v", connections[1])
	}
	if got := strings.TrimSpace(jobReq.Name); got != "orders-export" {
		t.Fatalf("job name=%q want=%q", got, "orders-export")
	}
	if jobReq.TargetNamespace != "analytics" || jobReq.TargetTable != "orders" || jobReq.WriteMode != "append" {
		t.Fatalf("unexpected target/write payload: %+v", jobReq)
	}
	if jobReq.SourceConnectionID != "conn-1" || jobReq.TargetConnectionID != "conn-2" {
		t.Fatalf("unexpected connection ids in job payload: %+v", jobReq)
	}
	if jobReq.SourceSQL != "" {
		t.Fatalf("source_sql=%q want empty for mssql submit", jobReq.SourceSQL)
	}
	if jobReq.Incremental {
		t.Fatalf("job incremental=%v want false payload=%+v", jobReq.Incremental, jobReq)
	}
	if jobReq.HWMColumn != "RowId" {
		t.Fatalf("hwm_column=%q want %q", jobReq.HWMColumn, "RowId")
	}
	if got := jobReq.OptionsJSON["partition_strategy"]; got != "ordered_cursor" {
		t.Fatalf("partition_strategy=%v want ordered_cursor", got)
	}
	if got := jobReq.OptionsJSON["cursor_column"]; got != "RowId" {
		t.Fatalf("cursor_column=%v want RowId", got)
	}
	if got := int(jobReq.OptionsJSON["max_in_flight_tasks"].(float64)); got != 8 {
		t.Fatalf("max_in_flight_tasks=%d want=8", got)
	}
	if got := int64(jobReq.OptionsJSON["target_file_bytes"].(float64)); got != 134217728 {
		t.Fatalf("target_file_bytes=%d want=134217728", got)
	}
	registration, err := icebergreg.ParseRunConfig(runReq.RegistrationConfig)
	if err != nil {
		t.Fatalf("ParseRunConfig: %v", err)
	}
	if registration.URI != "http://catalog:8181" || registration.Table != "analytics.orders" || registration.SchemaEvolution != "additive" {
		t.Fatalf("registration=%+v", registration)
	}
	if len(registration.PartitionSpec) != 1 || !registration.Upsert.Enabled || !registration.CredentialVending.Required {
		t.Fatalf("partition_spec=%+v upsert=%+v vending=%+v", registration.PartitionSpec, registration.Upsert, registration.CredentialVending)
	}
}

func TestCmdRunSubmitRejectsMissingFile(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdRun(context.Background(), []string{"submit", "--file", filepath.Join(t.TempDir(), "missing.yaml")})
	})

	if code != exitUsage {
		t.Fatalf("exit code=%d want=%d stderr=%q stdout=%q", code, exitUsage, stderr, stdout)
	}
	if !strings.Contains(stderr, "invalid run submit config: read") {
		t.Fatalf("expected missing-file error in stderr, got %q", stderr)
	}
}

func TestBuildJobPayloadFromConfigIncludesIcebergRegistrationOptions(t *testing.T) {
	payload, err := buildJobPayloadFromConfig(ranConfig{
		SourceEngine:      "mssql",
		HTTPBase:          "http://127.0.0.1:9100",
		SourceName:        "src",
		SourceDSN:         "sqlserver://sa:pass@example:1433?database=SalesDB",
		TargetName:        "tgt",
		S3Endpoint:        "http://minio:9000",
		S3Region:          "us-east-1",
		S3Bucket:          "bucket1",
		S3AccessKeyID:     "minioadmin",
		S3SecretAccessKey: "minioadmin",
		JobName:           "orders-export",
		TargetNamespace:   "analytics",
		TargetTable:       "orders",
		WriteMode:         "append",
		Table:             "SalesDB.dbo.Orders",
		IDColumn:          "RowId",
		AutoTune:          true,
		MaxInFlightTasks:  8,
		TargetRowsPerTask: 200000,
		AutoIceberg:       true,
		IcebergEngine:     "rest-go",
		IceTable:          "",
	}, "src-1", "tgt-1")
	if err != nil {
		t.Fatalf("buildJobPayloadFromConfig: %v", err)
	}

	raw, ok := payload.OptionsJSON["iceberg_registration"].(map[string]any)
	if !ok {
		t.Fatalf("missing iceberg_registration options: %#v", payload.OptionsJSON)
	}
	if raw["enabled"] != true {
		t.Fatalf("enabled=%v want true", raw["enabled"])
	}
	if raw["engine"] != "rest-go" {
		t.Fatalf("engine=%v want rest-go", raw["engine"])
	}
	if raw["table"] != "mssql.SalesDB__dbo__Orders" {
		t.Fatalf("table=%v want mssql.SalesDB__dbo__Orders", raw["table"])
	}
}

func TestBuildJobPayloadFromConfigAcceptsOracle(t *testing.T) {
	payload, err := buildJobPayloadFromConfig(ranConfig{
		SourceEngine:      "oracle",
		HTTPBase:          "http://127.0.0.1:9100",
		SourceName:        "oracle-src",
		SourceDSN:         "oracle://user:password@localhost:1521/ORCLCDB",
		TargetName:        "tgt",
		S3Endpoint:        "http://minio:9000",
		S3Region:          "us-east-1",
		S3Bucket:          "bucket1",
		S3AccessKeyID:     "minioadmin",
		S3SecretAccessKey: "minioadmin",
		JobName:           "orders-export",
		TargetNamespace:   "analytics",
		TargetTable:       "orders",
		WriteMode:         "append",
		Table:             "SALES.ORDER_LINES",
		IDColumn:          "ORDER_LINE_ID",
		AutoTune:          true,
	}, "src-1", "tgt-1")
	if err != nil {
		t.Fatalf("buildJobPayloadFromConfig: %v", err)
	}
	if got := payload.OptionsJSON["partition_strategy"]; got != "ordered_cursor" {
		t.Fatalf("partition_strategy=%v want ordered_cursor", got)
	}
	if got := payload.OptionsJSON["cursor_column"]; got != "ORDER_LINE_ID" {
		t.Fatalf("cursor_column=%v want ORDER_LINE_ID", got)
	}
}

func TestCmdRunSubmitRejectsIncompleteSpec(t *testing.T) {
	specPath := writeTempRunSubmitFile(t, "run-submit-invalid-*.yaml", `
source:
  name: mssql-prod
  engine: mssql
  dsn: sqlserver://sa:pass@example:1433?database=SalesDB
target:
  name: minio-lake
  endpoint: http://localhost:9000
  region: us-east-1
  bucket: bucket1
  force_path_style: true
  access_key_id: minioadmin
  secret_access_key: minioadmin
job:
  name: orders-export
  target_table: orders
  write_mode: append
  incremental: false
  table: SalesDB.dbo.Orders
  id_column: RowId
  auto_tune: true
`)

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdRun(context.Background(), []string{"submit", "--file", specPath})
	})

	if code != exitUsage {
		t.Fatalf("exit code=%d want=%d stderr=%q stdout=%q", code, exitUsage, stderr, stdout)
	}
	if !strings.Contains(stderr, "job.target_namespace is required") {
		t.Fatalf("expected validation error in stderr, got %q", stderr)
	}
}

func writeTempRunSubmitFile(t *testing.T, pattern, body string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), pattern)
	if err != nil {
		t.Fatalf("create temp run submit file: %v", err)
	}
	if _, err := f.WriteString(strings.TrimSpace(body)); err != nil {
		t.Fatalf("write temp run submit file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp run submit file: %v", err)
	}
	return f.Name()
}
