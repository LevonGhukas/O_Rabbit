package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

func TestAPIRunSubmitPlannerFailureIncludesDetails(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)
	details := `cursor column "AGE" is nullable; nullable cursor columns can skip rows in incremental mode. Choose a NOT NULL ordered key or run full load`
	srv.runPlanner = func(context.Context, *db.Store, crypto.Key, db.Job, json.RawMessage, *db.AuditRecord) (db.Run, []db.TaskInsert, error) {
		return db.Run{}, nil, errors.New(details)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "oracle",
			"dsn": "oracle://user:pass@db:1521/ORCLCDB",
			"table": "APP.PEOPLE",
			"cursor_column": "AGE",
			"incremental": true
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if resp.Error.Code != httperr.CodeInternalError {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeInternalError)
	}
	if resp.Error.Message != "run planning failed" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "run planning failed")
	}
	if resp.Error.Details != details {
		t.Fatalf("error details=%q want=%q", resp.Error.Details, details)
	}
}

func TestTargetConnectionsReuseOnlyMatchingDestinationIdentity(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", nil)
	makeRequest := func(prefix, accessKey string) connectionCreateRequest {
		spec := validatedRunSubmitSpec{TargetEndpoint: "HTTP://MINIO:9000/", TargetRegion: "us-east-1", TargetBucket: "bucket1", TargetPrefixOverride: prefix, TargetForcePathStyle: true}
		applyFrontendDestinationIdentity(&spec)
		return connectionCreateRequest{
			Name:     spec.TargetConnectionName,
			Kind:     "target",
			Engine:   "s3",
			Metadata: json.RawMessage(`{"endpoint":"http://minio:9000","region":"us-east-1","bucket":"bucket1","prefix":"` + prefix + `","force_path_style":true}`),
			Secret:   json.RawMessage(`{"access_key_id":"` + accessKey + `","secret_access_key":"secret"}`),
		}
	}
	first, err := srv.upsertConnectionByName(req, makeRequest("prefix-a/", "old-key"))
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := srv.upsertConnectionByName(req, makeRequest("/prefix-a", "new-key"))
	if err != nil {
		t.Fatal(err)
	}
	if rotated.ID != first.ID {
		t.Fatalf("credential rotation created a new destination: first=%s rotated=%s", first.ID, rotated.ID)
	}
	second, err := srv.upsertConnectionByName(req, makeRequest("prefix-b", "other-key"))
	if err != nil {
		t.Fatal(err)
	}
	if second.ID == first.ID || second.Name == first.Name {
		t.Fatalf("different prefixes collided: first=%+v second=%+v", first, second)
	}
}

func TestAPIRunSubmitPostgresBuildsExpectedJobOptions(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user:pass@db:5432/app?sslmode=disable",
			"table": "public.orders",
			"cursor_column": "id",
			"incremental": false
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		},
		"iceberg": {
			"enabled": false
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var resp struct {
		RunID              string `json:"run_id"`
		JobID              string `json:"job_id"`
		SourceConnectionID string `json:"source_connection_id"`
		TargetConnectionID string `json:"target_connection_id"`
		Status             string `json:"status"`
		EventsURL          string `json:"events_url"`
		RunURL             string `json:"run_url"`
	}
	decodeJSONBody(t, rec, &resp)
	if resp.RunID != "run-submit-test" {
		t.Fatalf("run_id=%q want run-submit-test", resp.RunID)
	}
	if resp.Status != "RUNNING" {
		t.Fatalf("status=%q want RUNNING", resp.Status)
	}
	if resp.EventsURL != "/api/runs/run-submit-test/events" {
		t.Fatalf("events_url=%q", resp.EventsURL)
	}
	if resp.RunURL != "/api/runs/run-submit-test" {
		t.Fatalf("run_url=%q", resp.RunURL)
	}
	if strings.Contains(rec.Body.String(), "postgresql://user:pass@db:5432/app?sslmode=disable") {
		t.Fatalf("response leaked source dsn: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "miniosecret") {
		t.Fatalf("response leaked target secret: %s", rec.Body.String())
	}

	job, err := st.GetJob(context.Background(), resp.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if !strings.HasPrefix(job.Name, "postgres_public_orders-") || len(strings.TrimPrefix(job.Name, "postgres_public_orders-")) != 16 {
		t.Fatalf("job name=%q want destination-scoped postgres_public_orders-<hash>", job.Name)
	}
	if job.TargetNamespace != defaultFrontendTargetNamespace || job.TargetTable != defaultFrontendTargetTable || job.WriteMode != "overwrite" {
		t.Fatalf("unexpected target defaults: namespace=%q table=%q write_mode=%q", job.TargetNamespace, job.TargetTable, job.WriteMode)
	}
	if job.HWMColumn == nil || *job.HWMColumn != "id" {
		t.Fatalf("hwm_column=%v want id", job.HWMColumn)
	}

	opts, err := jobopts.Parse(job.OptionsJSON)
	if err != nil {
		t.Fatalf("parse job options: %v", err)
	}
	if opts.PartitionStrategy != "ordered_cursor" {
		t.Fatalf("partition_strategy=%q want ordered_cursor", opts.PartitionStrategy)
	}
	if opts.Table != "public.orders" {
		t.Fatalf("table=%q want public.orders", opts.Table)
	}
	if opts.EffectiveCursorColumn() != "id" {
		t.Fatalf("cursor_column=%q want id", opts.EffectiveCursorColumn())
	}
	if !opts.AutoTune {
		t.Fatalf("auto_tune=%v want true", opts.AutoTune)
	}
	if opts.MaxInFlightTasks != 0 || opts.PlannedTasks != 0 {
		t.Fatalf("expected zero planner hints for default auto-tune: max_in_flight=%d planned_tasks=%d", opts.MaxInFlightTasks, opts.PlannedTasks)
	}

	var raw map[string]any
	if err := json.Unmarshal(job.OptionsJSON, &raw); err != nil {
		t.Fatalf("decode raw job options: %v", err)
	}
	if _, ok := raw[icebergreg.JobOptionsKey]; ok {
		t.Fatalf("did not expect iceberg registration job options when disabled: %#v", raw)
	}
	if got := srv.lastRegistrationConfig(); len(got) != 0 {
		t.Fatalf("expected empty registration config when iceberg disabled, got %s", string(got))
	}
}

func TestAPIRunSubmitQueryModePersistsSourceMetadata(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user:pass@db:5432/app?sslmode=disable",
			"mode": "query",
			"query": "SELECT id, name FROM public.orders WHERE status = 'active'",
			"cursor_column": "id",
			"incremental": true
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		},
		"iceberg": {
			"enabled": false
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var resp struct {
		JobID string `json:"job_id"`
	}
	decodeJSONBody(t, rec, &resp)
	job, err := st.GetJob(context.Background(), resp.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	opts, err := jobopts.Parse(job.OptionsJSON)
	if err != nil {
		t.Fatalf("parse job options: %v", err)
	}
	if opts.NormalizedSourceMode() != "query" {
		t.Fatalf("source_mode=%q want query", opts.NormalizedSourceMode())
	}
	if opts.Query == "" || job.SourceSQL != opts.Query {
		t.Fatalf("query not persisted consistently: source_sql=%q options_query=%q", job.SourceSQL, opts.Query)
	}
	if opts.QueryHash == "" || !strings.Contains(opts.SourceName, opts.QueryHash) {
		t.Fatalf("query hash/source name not persisted: hash=%q source_name=%q", opts.QueryHash, opts.SourceName)
	}
	if opts.Table != "" {
		t.Fatalf("query mode should not persist physical table, got %q", opts.Table)
	}
}

func TestAPIRunSubmitOracleBuildsExpectedJobOptions(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "oracle",
			"dsn": "oracle://user:pass@db:1521/ORCLCDB",
			"table": "SALES.ORDER_LINES",
			"cursor_column": "ORDER_LINE_ID"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_region": "us-east-1",
			"s3_bucket": "bucket1",
			"s3_force_path_style": true,
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		},
		"performance": {
			"auto_tune": true,
			"max_in_flight_tasks": 5,
			"planned_tasks": 11,
			"fetch_limit_rows": 22222,
			"target_rows_per_task": 333333,
			"target_file_bytes": 123456789,
			"max_rows_per_file": 7777
		},
		"iceberg": {
			"enabled": false
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var resp struct {
		JobID string `json:"job_id"`
	}
	decodeJSONBody(t, rec, &resp)
	job, err := st.GetJob(context.Background(), resp.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	opts, err := jobopts.Parse(job.OptionsJSON)
	if err != nil {
		t.Fatalf("parse job options: %v", err)
	}
	if opts.Table != "SALES.ORDER_LINES" {
		t.Fatalf("table=%q want SALES.ORDER_LINES", opts.Table)
	}
	if opts.EffectiveCursorColumn() != "ORDER_LINE_ID" {
		t.Fatalf("cursor_column=%q want ORDER_LINE_ID", opts.EffectiveCursorColumn())
	}
	if !opts.AutoTune {
		t.Fatalf("auto_tune=%v want true", opts.AutoTune)
	}
}

func TestAPIRunSubmitMissingSourceTableReturnsValidationError(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user:pass@db:5432/app?sslmode=disable",
			"cursor_column": "id"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if resp.Error.Code != httperr.CodeInvalidInput {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeInvalidInput)
	}
	details := detailsMap(t, resp.Error.Details)
	if details["field"] != "source" {
		t.Fatalf("field=%v want source", details["field"])
	}
}

func TestAPIRunValidateRejectsBothTableAndQuery(t *testing.T) {
	req := validRunSubmitRequestForValidation()
	req.Source.Mode = "query"
	req.Source.Table = "public.orders"
	req.Source.Query = "SELECT id FROM public.orders"

	_, err := validateRunSubmitRequest(req)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	var validationErr *requestValidationError
	if !AsValidationError(err, &validationErr) {
		t.Fatalf("expected requestValidationError, got %T: %v", err, err)
	}
	if validationErr.details["field"] != "source" {
		t.Fatalf("field=%v want source", validationErr.details["field"])
	}
}

func TestAPIRunValidateRejectsNeitherTableNorQuery(t *testing.T) {
	req := validRunSubmitRequestForValidation()
	req.Source.Table = ""

	_, err := validateRunSubmitRequest(req)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	var validationErr *requestValidationError
	if !AsValidationError(err, &validationErr) {
		t.Fatalf("expected requestValidationError, got %T: %v", err, err)
	}
	if validationErr.details["field"] != "source" {
		t.Fatalf("field=%v want source", validationErr.details["field"])
	}
}

func TestAPIRunValidateAcceptsSimpleSelectQueryMode(t *testing.T) {
	req := validRunSubmitRequestForValidation()
	req.Source.Mode = "query"
	req.Source.Table = ""
	req.Source.Query = "SELECT id, name FROM public.orders WHERE status = 'active'"

	spec, err := validateRunSubmitRequest(req)
	if err != nil {
		t.Fatalf("validate query mode: %v", err)
	}
	if spec.SourceMode != "query" {
		t.Fatalf("source_mode=%q want query", spec.SourceMode)
	}
	if spec.SourceQuery != req.Source.Query {
		t.Fatalf("source_query=%q want %q", spec.SourceQuery, req.Source.Query)
	}
	if spec.QueryHash == "" {
		t.Fatalf("query_hash should be populated")
	}
	if !spec.QuerySupported {
		t.Fatalf("query_supported=false want true")
	}
}

func TestAPIRunValidateRejectsUnsupportedQueryEngine(t *testing.T) {
	req := validRunSubmitRequestForValidation()
	req.Source.Engine = "mongodb"
	req.Source.Mode = "query"
	req.Source.Table = ""
	req.Source.Query = "SELECT id FROM orders"

	_, err := validateRunSubmitRequest(req)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	var validationErr *requestValidationError
	if !AsValidationError(err, &validationErr) {
		t.Fatalf("expected requestValidationError, got %T: %v", err, err)
	}
	if validationErr.details["field"] != "source.engine" {
		t.Fatalf("field=%v want source.engine", validationErr.details["field"])
	}
	if !strings.Contains(validationErr.message, "query mode is not supported for mongodb") {
		t.Fatalf("message=%q", validationErr.message)
	}
}

func TestAPIRunSubmitMissingCursorColumnReturnsValidationError(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "oracle",
			"dsn": "oracle://user:pass@db:1521/ORCLCDB",
			"table": "SALES.ORDER_LINES",
			"incremental": true
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	details := detailsMap(t, resp.Error.Details)
	if details["field"] != "source.cursor_column" {
		t.Fatalf("field=%v want source.cursor_column", details["field"])
	}
}

func TestAPIRunSubmitFullModeWithoutCursorColumnAccepted(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "oracle",
			"dsn": "oracle://user:pass@db:1521/ORCLCDB",
			"table": "SALES.ORDER_LINES",
			"incremental": false
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func TestAPIRunSubmitUnsupportedEngineReturnsValidationError(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "sqlite",
			"dsn": "file:test.db",
			"table": "orders",
			"cursor_column": "id"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if resp.Error.Code != httperr.CodeInvalidInput {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeInvalidInput)
	}
}

func TestAPIRunSubmitAutoTuneDefaultsTrue(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user:pass@db:5432/app?sslmode=disable",
			"table": "public.orders",
			"cursor_column": "id"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var resp struct {
		JobID string `json:"job_id"`
	}
	decodeJSONBody(t, rec, &resp)
	job, err := st.GetJob(context.Background(), resp.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	opts, err := jobopts.Parse(job.OptionsJSON)
	if err != nil {
		t.Fatalf("parse job options: %v", err)
	}
	if !opts.AutoTune {
		t.Fatalf("auto_tune=%v want true", opts.AutoTune)
	}
}

func TestAPIRunSubmitAdvancedPerformanceValuesArePreserved(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user:pass@db:5432/app?sslmode=disable",
			"table": "public.orders",
			"cursor_column": "id"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		},
		"performance": {
			"auto_tune": true,
			"max_in_flight_tasks": 4,
			"planned_tasks": 9,
			"fetch_limit_rows": 45678,
			"target_rows_per_task": 250000,
			"target_file_bytes": 222222222,
			"max_rows_per_file": 99999
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var resp struct {
		JobID string `json:"job_id"`
	}
	decodeJSONBody(t, rec, &resp)
	job, err := st.GetJob(context.Background(), resp.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	opts, err := jobopts.Parse(job.OptionsJSON)
	if err != nil {
		t.Fatalf("parse job options: %v", err)
	}
	if opts.MaxInFlightTasks != 4 || opts.PlannedTasks != 9 || opts.TargetRowsPerTask != 250000 || opts.TargetFileBytes != 222222222 {
		t.Fatalf("planner hints not persisted correctly: %+v", opts)
	}
}

func TestAPIRunSubmitUsesIcebergTargetFileSizeForWorker(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user:pass@db:5432/app?sslmode=disable",
			"table": "public.orders",
			"cursor_column": "id"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		},
		"iceberg": {
			"enabled": true,
			"engine": "rest-go",
			"table": "analytics.orders",
			"config_yaml": "uri: http://default-catalog:8181\ntarget_file_size: 111111111\n",
			"options": {
				"uri": "http://run-catalog:8181",
				"target_file_size": 123456789
			}
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var resp struct {
		JobID string `json:"job_id"`
	}
	decodeJSONBody(t, rec, &resp)
	job, err := st.GetJob(context.Background(), resp.JobID)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := jobopts.Parse(job.OptionsJSON)
	if err != nil {
		t.Fatal(err)
	}
	if opts.TargetFileBytes != 123456789 {
		t.Fatalf("target_file_bytes=%d want 123456789", opts.TargetFileBytes)
	}
	registration, err := icebergreg.ParseRunConfig(srv.lastRegistrationConfig())
	if err != nil {
		t.Fatal(err)
	}
	if registration.URI != "http://run-catalog:8181" || registration.TargetFileSize != 123456789 {
		t.Fatalf("resolved registration=%+v", registration)
	}
}

func TestAPIRunSubmitResponseDoesNotIncludeSecrets(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	body := `{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user:verysecret@db:5432/app?sslmode=disable",
			"table": "public.orders",
			"cursor_column": "id"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "MINIO_KEY",
			"s3_secret_access_key": "MINIO_SECRET"
		}
	}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(body))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "verysecret") || strings.Contains(rec.Body.String(), "MINIO_KEY") || strings.Contains(rec.Body.String(), "MINIO_SECRET") {
		t.Fatalf("response leaked secret material: %s", rec.Body.String())
	}
}

func TestAPIRunSubmitIcebergDisabledRequestWorks(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "oracle",
			"dsn": "oracle://user:pass@db:1521/ORCLCDB",
			"table": "SALES.ORDER_LINES",
			"cursor_column": "ORDER_LINE_ID"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		},
		"iceberg": {
			"enabled": false
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if got := srv.lastRegistrationConfig(); len(got) != 0 {
		t.Fatalf("expected empty registration config, got %s", string(got))
	}
}

func TestAPIRunSubmitIcebergEnabledValidatesAndSnapshotsConfig(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user:pass@db:5432/app?sslmode=disable",
			"table": "public.orders",
			"cursor_column": "id"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		},
		"iceberg": {
			"enabled": true,
			"engine": "rest-go",
			"config_yaml": "uri: http://catalog:8181\nbearerToken: token\n"
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var resp struct {
		JobID string `json:"job_id"`
	}
	decodeJSONBody(t, rec, &resp)
	job, err := st.GetJob(context.Background(), resp.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(job.OptionsJSON, &raw); err != nil {
		t.Fatalf("decode raw job options: %v", err)
	}
	iceRaw, ok := raw[icebergreg.JobOptionsKey].(map[string]any)
	if !ok {
		t.Fatalf("missing iceberg job options: %#v", raw)
	}
	if iceRaw["enabled"] != true {
		t.Fatalf("enabled=%v want true", iceRaw["enabled"])
	}
	if iceRaw["engine"] != "rest-go" {
		t.Fatalf("engine=%v want rest-go", iceRaw["engine"])
	}
	if iceRaw["table"] != "postgres.public__orders" {
		t.Fatalf("table=%v want postgres.public__orders", iceRaw["table"])
	}

	regCfg, err := icebergreg.ParseRunConfig(srv.lastRegistrationConfig())
	if err != nil {
		t.Fatalf("parse registration config: %v", err)
	}
	if !regCfg.Enabled {
		t.Fatalf("registration enabled=%v want true", regCfg.Enabled)
	}
	if regCfg.Engine != "rest-go" {
		t.Fatalf("registration engine=%q want rest-go", regCfg.Engine)
	}
	if regCfg.Table != "postgres.public__orders" {
		t.Fatalf("registration table=%q want postgres.public__orders", regCfg.Table)
	}
	if regCfg.URI != "http://catalog:8181" {
		t.Fatalf("registration uri=%q want http://catalog:8181", regCfg.URI)
	}
}

func TestAPIRunValidateReturnsDerivedValues(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)
	if err := st.TouchWorkerHeartbeat(context.Background(), "", "worker-1"); err != nil {
		t.Fatalf("touch worker heartbeat: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/validate", strings.NewReader(`{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user:pass@db:5432/app?sslmode=disable",
			"table": "public.orders",
			"cursor_column": "id"
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		},
		"iceberg": {
			"enabled": false
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp struct {
		OK               bool `json:"ok"`
		AvailableWorkers int  `json:"available_workers"`
		Derived          struct {
			JobName      string `json:"job_name"`
			TargetPrefix string `json:"target_prefix"`
			IcebergTable string `json:"iceberg_table"`
			AutoTune     bool   `json:"auto_tune"`
			S3Region     string `json:"s3_region"`
			ForcePath    bool   `json:"s3_force_path_style"`
		} `json:"derived"`
	}
	decodeJSONBody(t, rec, &resp)
	if !resp.OK {
		t.Fatalf("ok=%v want true", resp.OK)
	}
	if resp.AvailableWorkers != 1 {
		t.Fatalf("available_workers=%d want=1", resp.AvailableWorkers)
	}
	if !strings.HasPrefix(resp.Derived.JobName, "postgres_public_orders-") || len(strings.TrimPrefix(resp.Derived.JobName, "postgres_public_orders-")) != 16 {
		t.Fatalf("job_name=%q want destination-scoped postgres_public_orders-<hash>", resp.Derived.JobName)
	}
	if resp.Derived.TargetPrefix != "postgres/public__orders" {
		t.Fatalf("target_prefix=%q want postgres/public__orders", resp.Derived.TargetPrefix)
	}
	if resp.Derived.IcebergTable != "postgres.public__orders" {
		t.Fatalf("iceberg_table=%q want postgres.public__orders", resp.Derived.IcebergTable)
	}
	if !resp.Derived.AutoTune {
		t.Fatalf("auto_tune=%v want true", resp.Derived.AutoTune)
	}
	if resp.Derived.S3Region != "us-east-1" {
		t.Fatalf("s3_region=%q want us-east-1", resp.Derived.S3Region)
	}
	if !resp.Derived.ForcePath {
		t.Fatalf("s3_force_path_style=%v want true", resp.Derived.ForcePath)
	}
}

func TestAPISourceEnginesListsCapabilities(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/source-engines", nil)

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var items []struct {
		Engine                        string                     `json:"engine"`
		OrderedCursorSupported        bool                       `json:"ordered_cursor_supported"`
		QuerySupported                bool                       `json:"query_supported"`
		QueryLanguages                []connectors.QueryLanguage `json:"query_languages"`
		QueryIncrementalSupported     bool                       `json:"query_incremental_supported"`
		QuerySchemaInferenceSupported bool                       `json:"query_schema_inference_supported"`
		FrontendSubmitSupported       bool                       `json:"frontend_submit_supported"`
	}
	decodeJSONBody(t, rec, &items)
	if len(items) == 0 {
		t.Fatal("expected source engine list")
	}
	foundOracle := false
	foundMongoDB := false
	foundCassandra := false
	foundFlightSQL := false
	for _, item := range items {
		if item.Engine == "oracle" {
			foundOracle = true
			if !item.OrderedCursorSupported || !item.QuerySupported || !item.QueryIncrementalSupported || !item.QuerySchemaInferenceSupported || !item.FrontendSubmitSupported {
				t.Fatalf("oracle capabilities=%+v want complete query and submit support", item)
			}
			if len(item.QueryLanguages) != 1 || item.QueryLanguages[0] != connectors.QueryLanguageOracleSQL {
				t.Fatalf("oracle query languages=%v want [%s]", item.QueryLanguages, connectors.QueryLanguageOracleSQL)
			}
		}
		if item.Engine == "mongodb" {
			foundMongoDB = true
			assertQueryCapabilitiesDisabled(t, item.Engine, item.QuerySupported, item.QueryLanguages, item.QueryIncrementalSupported, item.QuerySchemaInferenceSupported)
		}
		if item.Engine == "cassandra" {
			foundCassandra = true
			if !item.QuerySupported || !item.QueryIncrementalSupported || !item.QuerySchemaInferenceSupported {
				t.Fatalf("cassandra capabilities=%+v want complete query and submit support", item)
			}
			if len(item.QueryLanguages) != 1 || item.QueryLanguages[0] != connectors.QueryLanguageCQL {
				t.Fatalf("cassandra query languages=%v want [%s]", item.QueryLanguages, connectors.QueryLanguageCQL)
			}
		}
		if item.Engine == "flightsql" {
			foundFlightSQL = true
			assertQueryCapabilitiesDisabled(t, item.Engine, item.QuerySupported, item.QueryLanguages, item.QueryIncrementalSupported, item.QuerySchemaInferenceSupported)
		}
	}
	if !foundOracle {
		t.Fatal("expected oracle in source engine list")
	}
	if !foundMongoDB || !foundCassandra || !foundFlightSQL {
		t.Fatalf(
			"expected contained engines in source engine list: mongodb=%v cassandra=%v flightsql=%v",
			foundMongoDB,
			foundCassandra,
			foundFlightSQL,
		)
	}
}

func assertQueryCapabilitiesDisabled(t *testing.T, engine string, supported bool, languages []connectors.QueryLanguage, incremental, schemaInference bool) {
	t.Helper()
	if supported || incremental || schemaInference || len(languages) != 0 {
		t.Fatalf("%s query capabilities=(supported=%v languages=%v incremental=%v schema_inference=%v) want disabled", engine, supported, languages, incremental, schemaInference)
	}
}

func TestAuthMiddlewareProtectsAPISourceEngines(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "topsecret")

	unauthRec := httptest.NewRecorder()
	unauthReq := httptest.NewRequest(http.MethodGet, "/api/source-engines", nil)
	srv.Handler().ServeHTTP(unauthRec, unauthReq)
	if unauthRec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth status=%d want=%d body=%s", unauthRec.Code, http.StatusUnauthorized, unauthRec.Body.String())
	}

	authRec := httptest.NewRecorder()
	authReq := httptest.NewRequest(http.MethodGet, "/api/source-engines", nil)
	authReq.Header.Set("Authorization", "Bearer topsecret")
	srv.Handler().ServeHTTP(authRec, authReq)
	if authRec.Code != http.StatusOK {
		t.Fatalf("auth status=%d want=%d body=%s", authRec.Code, http.StatusOK, authRec.Body.String())
	}
}

func TestAPIJobRunsStartExistingIncrementalRunWithRegistration(t *testing.T) {
	st := openTestStore(t)
	seedExistingJobRunFixture(t, st, "job-api-job-run", true, true)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/job-api-job-run/runs", strings.NewReader(`{
		"mode": "incremental",
		"iceberg": {
			"enabled": true,
			"engine": "rest-go",
			"table": "postgres.public__orders",
			"config_yaml": "uri: http://catalog:8181\nbearerToken: token\n"
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var resp struct {
		RunID     string `json:"run_id"`
		JobID     string `json:"job_id"`
		Status    string `json:"status"`
		EventsURL string `json:"events_url"`
		RunURL    string `json:"run_url"`
	}
	decodeJSONBody(t, rec, &resp)
	if resp.RunID != "run-submit-test" {
		t.Fatalf("run_id=%q want run-submit-test", resp.RunID)
	}
	if resp.JobID != "job-api-job-run" {
		t.Fatalf("job_id=%q want job-api-job-run", resp.JobID)
	}
	if resp.Status != "RUNNING" {
		t.Fatalf("status=%q want RUNNING", resp.Status)
	}
	if resp.EventsURL != "/api/runs/run-submit-test/events" {
		t.Fatalf("events_url=%q", resp.EventsURL)
	}
	if resp.RunURL != "/api/runs/run-submit-test" {
		t.Fatalf("run_url=%q", resp.RunURL)
	}
	if strings.Contains(rec.Body.String(), "bearerToken") {
		t.Fatalf("response leaked registration config: %s", rec.Body.String())
	}

	regCfg, err := icebergreg.ParseRunConfig(srv.lastRegistrationConfig())
	if err != nil {
		t.Fatalf("parse registration config: %v", err)
	}
	if !regCfg.Enabled {
		t.Fatalf("registration enabled=%v want true", regCfg.Enabled)
	}
	if regCfg.Engine != "rest-go" {
		t.Fatalf("registration engine=%q want rest-go", regCfg.Engine)
	}
	if regCfg.Table != "postgres.public__orders" {
		t.Fatalf("registration table=%q want postgres.public__orders", regCfg.Table)
	}
	if regCfg.URI != "http://catalog:8181" {
		t.Fatalf("registration uri=%q want http://catalog:8181", regCfg.URI)
	}
}

func TestAPIJobRunsFullModeAcceptedForFullJob(t *testing.T) {
	st := openTestStore(t)
	seedExistingJobRunFixture(t, st, "job-api-job-run-full", false, true)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/job-api-job-run-full/runs", strings.NewReader(`{
		"mode": "full",
		"iceberg": {
			"enabled": true,
			"config_yaml": "uri: http://catalog:8181\nbearerToken: token\n"
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	regCfg, err := icebergreg.ParseRunConfig(srv.lastRegistrationConfig())
	if err != nil {
		t.Fatalf("parse registration config: %v", err)
	}
	if regCfg.Table != "postgres.public__orders" {
		t.Fatalf("registration table=%q want postgres.public__orders", regCfg.Table)
	}
}

func TestAPIJobRunsIcebergDisabledDoesNotRegister(t *testing.T) {
	st := openTestStore(t)
	seedExistingJobRunFixture(t, st, "job-api-job-run-disabled", true, true)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/job-api-job-run-disabled/runs", strings.NewReader(`{
		"mode": "incremental",
		"iceberg": {
			"enabled": false
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	got := srv.lastRegistrationConfig()
	if len(got) == 0 {
		t.Fatal("expected explicit disabled registration snapshot")
	}
	regCfg, err := icebergreg.ParseRunConfig(got)
	if err != nil {
		t.Fatalf("parse registration config: %v", err)
	}
	if regCfg.Enabled {
		t.Fatalf("registration enabled=%v want false", regCfg.Enabled)
	}
}

func TestAPIJobRunsMissingJobReturnsNotFound(t *testing.T) {
	st := openTestStore(t)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/missing/runs", strings.NewReader(`{
		"mode": "incremental",
		"iceberg": {
			"enabled": false
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if resp.Error.Code != httperr.CodeNotFound {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeNotFound)
	}
}

func TestAPIJobRunsInvalidModeReturnsValidationError(t *testing.T) {
	st := openTestStore(t)
	seedExistingJobRunFixture(t, st, "job-api-job-run-invalid-mode", true, true)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/job-api-job-run-invalid-mode/runs", strings.NewReader(`{
		"mode": "delta",
		"iceberg": {
			"enabled": false
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	details := detailsMap(t, resp.Error.Details)
	if details["field"] != "mode" {
		t.Fatalf("field=%v want mode", details["field"])
	}
}

func TestAPIJobRunsAcceptsRunOptionsWithoutConfigYAML(t *testing.T) {
	st := openTestStore(t)
	seedExistingJobRunFixture(t, st, "job-api-job-run-missing-yaml", true, true)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/job-api-job-run-missing-yaml/runs", strings.NewReader(`{
		"mode": "incremental",
		"iceberg": {
			"enabled": true,
			"engine": "rest-go",
			"table": "postgres.public__orders",
			"options": {
				"uri": "http://catalog:8181",
				"schema_evolution": "additive",
				"target_file_size": 134217728,
				"partition_spec": [{"source": "created_at", "transform": "day"}],
				"upsert": {"enabled": true, "keys": ["id"], "mode": "merge-on-read"}
			}
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	cfg, err := icebergreg.ParseRunConfig(srv.lastRegistrationConfig())
	if err != nil {
		t.Fatalf("ParseRunConfig: %v", err)
	}
	if cfg.URI != "http://catalog:8181" || cfg.SchemaEvolution != "additive" || cfg.TargetFileSize != 134217728 {
		t.Fatalf("resolved run config=%+v", cfg)
	}
	if len(cfg.PartitionSpec) != 1 || cfg.PartitionSpec[0].Transform != "day" {
		t.Fatalf("partition_spec=%+v", cfg.PartitionSpec)
	}
	if !cfg.Upsert.Enabled || len(cfg.Upsert.Keys) != 1 || cfg.Upsert.Keys[0] != "id" {
		t.Fatalf("upsert=%+v", cfg.Upsert)
	}
	plannedOptions, err := jobopts.Parse(srv.lastPlannedJob().OptionsJSON)
	if err != nil {
		t.Fatalf("parse planned job options: %v", err)
	}
	if plannedOptions.TargetFileBytes != 134217728 {
		t.Fatalf("planned target_file_bytes=%d want 134217728", plannedOptions.TargetFileBytes)
	}
	persistedJob, err := st.GetJob(context.Background(), "job-api-job-run-missing-yaml")
	if err != nil {
		t.Fatalf("get persisted job: %v", err)
	}
	persistedOptions, err := jobopts.Parse(persistedJob.OptionsJSON)
	if err != nil {
		t.Fatalf("parse persisted job options: %v", err)
	}
	if persistedOptions.TargetFileBytes == 134217728 {
		t.Fatal("run target_file_size must not rewrite the persisted job")
	}
}

func TestLegacyJobRunsEndpointRemainsUnchanged(t *testing.T) {
	st := openTestStore(t)
	seedExistingJobRunFixture(t, st, "job-legacy-job-run", true, true)
	srv := newSubmitTestServer(st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs/job-legacy-job-run/runs", strings.NewReader(`{
		"mode": "incremental"
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var resp struct {
		Run   map[string]any   `json:"run"`
		Tasks []map[string]any `json:"tasks"`
	}
	decodeJSONBody(t, rec, &resp)
	if resp.Run["id"] != "run-submit-test" {
		t.Fatalf("run.id=%v want run-submit-test", resp.Run["id"])
	}
	if len(resp.Tasks) != 1 {
		t.Fatalf("tasks=%d want 1", len(resp.Tasks))
	}
	if got := srv.lastRegistrationConfig(); len(got) != 0 {
		t.Fatalf("expected legacy endpoint to keep empty registration config, got %s", string(got))
	}
}

func TestAPIRunAliasesExposeExistingRunEndpoints(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	run := db.Run{
		ID:            "run-api-alias",
		JobID:         "job-api-alias",
		Status:        "RUNNING",
		CorrelationID: "corr-api-alias",
		StartedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []db.TaskInsert{{
		ID:            "task-api-alias",
		RunID:         run.ID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        "PENDING",
	}}); err != nil {
		t.Fatalf("insert tasks: %v", err)
	}
	if err := st.InsertEvent(ctx, db.Event{
		ID:      "ev-api-alias",
		RunID:   run.ID,
		TS:      time.Now().UTC().Format(time.RFC3339Nano),
		Level:   "INFO",
		Message: "run queued",
	}); err != nil {
		t.Fatalf("insert event: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	cases := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{name: "runs list", method: http.MethodGet, path: "/api/runs", want: http.StatusOK},
		{name: "run detail", method: http.MethodGet, path: "/api/runs/" + run.ID, want: http.StatusOK},
		{name: "run events", method: http.MethodGet, path: "/api/runs/" + run.ID + "/events", want: http.StatusOK},
		{name: "run progress", method: http.MethodGet, path: "/api/runs/" + run.ID + "/progress", want: http.StatusOK},
		{name: "run artifacts", method: http.MethodGet, path: "/api/runs/" + run.ID + "/artifacts", want: http.StatusOK},
		{name: "run cancel", method: http.MethodPost, path: "/api/runs/" + run.ID + "/cancel", body: `{}`, want: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func validRunSubmitRequestForValidation() runSubmitRequest {
	return runSubmitRequest{
		Source: runSubmitSourceRequest{
			Engine:       "postgres",
			DSN:          "postgresql://user:pass@db:5432/app?sslmode=disable",
			Table:        "public.orders",
			CursorColumn: "id",
			Incremental:  true,
		},
		Target: runSubmitTargetRequest{
			S3Endpoint:        "http://minio:9000",
			S3Bucket:          "bucket1",
			S3AccessKeyID:     "minioadmin",
			S3SecretAccessKey: "miniosecret",
		},
		Iceberg: runSubmitIcebergRequest{
			Enabled: false,
		},
	}
}

type submitTestServer struct {
	*Server
	registrationConfig json.RawMessage
	plannedJob         db.Job
}

func newSubmitTestServer(st *db.Store) *submitTestServer {
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	ts := &submitTestServer{Server: srv}
	srv.runPlanner = func(ctx context.Context, st *db.Store, k crypto.Key, job db.Job, registrationConfig json.RawMessage, audit *db.AuditRecord) (db.Run, []db.TaskInsert, error) {
		ts.registrationConfig = append(json.RawMessage(nil), registrationConfig...)
		ts.plannedJob = job
		return db.Run{
				ID:            "run-submit-test",
				JobID:         job.ID,
				Status:        "RUNNING",
				CorrelationID: "corr-submit-test",
				StartedAt:     "2026-01-01T00:00:00Z",
			}, []db.TaskInsert{{
				ID:            "task-submit-test",
				RunID:         "run-submit-test",
				TaskIndex:     1,
				PartitionSpec: []byte(`{"type":"sql_cursor_single"}`),
				Status:        "PENDING",
			}}, nil
	}
	return ts
}

func (s *submitTestServer) lastRegistrationConfig() json.RawMessage {
	if s == nil {
		return nil
	}
	return append(json.RawMessage(nil), s.registrationConfig...)
}

func (s *submitTestServer) lastPlannedJob() db.Job {
	if s == nil {
		return db.Job{}
	}
	return s.plannedJob
}

func seedExistingJobRunFixture(t *testing.T, st *db.Store, jobID string, incremental bool, withJobRegistration bool) {
	t.Helper()

	srcSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"dsn":"postgresql://user:pass@db:5432/app?sslmode=disable"}`), []byte("src-"+jobID))
	if err != nil {
		t.Fatalf("encrypt source secret: %v", err)
	}
	tgtSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"access_key_id":"minioadmin","secret_access_key":"miniosecret"}`), []byte("tgt-"+jobID))
	if err != nil {
		t.Fatalf("encrypt target secret: %v", err)
	}

	createTestConnection(t, st, db.Connection{
		ID:            "src-" + jobID,
		Name:          "src-" + jobID,
		Kind:          "source",
		Engine:        "postgres",
		MetadataJSON:  []byte(`{}`),
		SecretEncBlob: srcSecret,
	})
	createTestConnection(t, st, db.Connection{
		ID:     "tgt-" + jobID,
		Name:   "tgt-" + jobID,
		Kind:   "target",
		Engine: "s3",
		MetadataJSON: []byte(`{
			"endpoint":"http://minio:9000",
			"region":"us-east-1",
			"bucket":"bucket1",
			"prefix":"exports",
			"force_path_style":true
		}`),
		SecretEncBlob: tgtSecret,
	})

	options := map[string]any{
		"table":              "public.orders",
		"partition_strategy": "ordered_cursor",
		"cursor_column":      "id",
		"id_column":          "id",
	}
	if withJobRegistration {
		options[icebergreg.JobOptionsKey] = map[string]any{
			"enabled": true,
			"engine":  "rest-go",
			"table":   "postgres.public__orders",
		}
	}
	optionsJSON, err := json.Marshal(options)
	if err != nil {
		t.Fatalf("marshal options: %v", err)
	}

	hwmColumn := "id"
	if err := st.CreateJob(context.Background(), db.Job{
		ID:                 jobID,
		Name:               jobID,
		SourceConnectionID: "src-" + jobID,
		TargetConnectionID: "tgt-" + jobID,
		TargetNamespace:    "orders",
		TargetTable:        "Orders",
		WriteMode:          "append",
		Incremental:        incremental,
		HWMColumn:          &hwmColumn,
		OptionsJSON:        optionsJSON,
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
}
