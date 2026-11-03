package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

func identityFields(columns ...string) []icebergreg.PartitionFieldConfig {
	out := make([]icebergreg.PartitionFieldConfig, len(columns))
	for i, c := range columns {
		out[i] = icebergreg.PartitionFieldConfig{Source: c, Transform: "identity"}
	}
	return out
}

func submitWithPartitionKeys(t *testing.T, srv *submitTestServer, path, iceberg string) *httptest.ResponseRecorder {
	t.Helper()
	body := `{
		"source": {"engine": "postgres", "dsn": "postgres://u:p@db/app", "table": "public.orders", "cursor_column": "id"},
		"target": {"s3_endpoint": "http://minio:9000", "s3_bucket": "b", "s3_access_key_id": "k", "s3_secret_access_key": "s"},
		"iceberg": ` + iceberg + `
	}`
	if strings.Contains(path, "/jobs/") {
		body = `{"mode": "incremental", "iceberg": ` + iceberg + `}`
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	return rec
}

func storedRunConfig(t *testing.T, srv *submitTestServer) icebergreg.RunConfig {
	t.Helper()
	cfg, err := icebergreg.ParseRunConfig(srv.lastRegistrationConfig())
	if err != nil {
		t.Fatalf("ParseRunConfig: %v", err)
	}
	return cfg
}

// partition_keys reaches the run's registration snapshot as identity fields,
// so the catalog partitions the table by them and, for rest-go, workers split
// their files by them.
func TestAPIRunSubmitPartitionKeysBecomeIdentityPartitionSpec(t *testing.T) {
	for _, engine := range []string{"rest-go", "ice"} {
		t.Run(engine, func(t *testing.T) {
			srv := newSubmitTestServer(openTestStore(t))

			rec := submitWithPartitionKeys(t, srv, "/api/v1/runs/submit", `{
				"enabled": true, "engine": "`+engine+`", "table": "ns.orders",
				"options": {"uri": "http://catalog:8181"},
				"partition_keys": [" tenant_id ", "region"]
			}`)

			if rec.Code != http.StatusCreated {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			cfg := storedRunConfig(t, srv)
			if want := identityFields("tenant_id", "region"); !reflect.DeepEqual(cfg.PartitionSpec, want) {
				t.Fatalf("partition_spec=%+v want %+v", cfg.PartitionSpec, want)
			}
			workerSpec, err := cfg.WorkerPartitionSpecJSON()
			if err != nil {
				t.Fatal(err)
			}
			if engine == "rest-go" && !strings.Contains(workerSpec, `"source":"tenant_id"`) {
				t.Fatalf("worker partition spec=%q, want workers to split by tenant_id", workerSpec)
			}
		})
	}
}

func TestAPIJobRunsPartitionKeysBecomeIdentityPartitionSpec(t *testing.T) {
	st := openTestStore(t)
	seedExistingJobRunFixture(t, st, "job-partition-keys", true, true)
	srv := newSubmitTestServer(st)

	rec := submitWithPartitionKeys(t, srv, "/api/jobs/job-partition-keys/runs", `{
		"enabled": true, "engine": "rest-go", "table": "postgres.public__orders",
		"options": {"uri": "http://catalog:8181"},
		"partition_keys": ["tenant_id"]
	}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got, want := storedRunConfig(t, srv).PartitionSpec, identityFields("tenant_id"); !reflect.DeepEqual(got, want) {
		t.Fatalf("partition_spec=%+v want %+v", got, want)
	}
}

func TestAPIRunSubmitAcceptsPartitionKeysMatchingPartitionSpec(t *testing.T) {
	srv := newSubmitTestServer(openTestStore(t))

	rec := submitWithPartitionKeys(t, srv, "/api/v1/runs/submit", `{
		"enabled": true, "engine": "rest-go", "table": "ns.orders",
		"options": {"uri": "http://catalog:8181", "partition_spec": [{"source": "tenant_id", "name": "tenant"}]},
		"partition_keys": ["tenant_id"]
	}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if spec := storedRunConfig(t, srv).PartitionSpec; len(spec) != 1 || spec[0].Name != "tenant" {
		t.Fatalf("partition_spec=%+v, want the explicit spec kept", spec)
	}
}

func TestAPIRunValidateRejectsUnusablePartitionKeys(t *testing.T) {
	tests := []struct {
		name, field, contains string
		edit                  func(*runSubmitRequest)
	}{
		{"iceberg disabled", "iceberg.partition_keys", "iceberg.enabled", func(r *runSubmitRequest) {
			r.Iceberg.Enabled = false
		}},
		{"empty key", "iceberg.partition_keys[1]", "column names", func(r *runSubmitRequest) {
			r.Iceberg.PartitionKeys = []string{"tenant_id", " "}
		}},
		{"duplicate key", "iceberg.partition_keys[1]", "more than once", func(r *runSubmitRequest) {
			r.Iceberg.PartitionKeys = []string{"tenant_id", "TENANT_ID"}
		}},
		{"not selected", "iceberg.partition_keys[0]", "select_columns", func(r *runSubmitRequest) {
			r.Source.SelectColumns = []string{"id", "amount"}
		}},
		{"different partition_spec", "iceberg.partition_keys", "different partitions", func(r *runSubmitRequest) {
			spec := []icebergreg.PartitionFieldConfig{{Source: "tenant_id", Transform: "bucket[16]"}}
			r.Iceberg.Options.PartitionSpec = &spec
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validRunSubmitRequestForValidation()
			req.Iceberg = runSubmitIcebergRequest{Enabled: true, Engine: "rest-go", Table: "ns.orders", PartitionKeys: []string{"tenant_id"}}
			tt.edit(&req)

			_, err := validateRunSubmitRequest(req)

			var validationErr *requestValidationError
			if !AsValidationError(err, &validationErr) {
				t.Fatalf("expected requestValidationError, got %T: %v", err, err)
			}
			if validationErr.details["field"] != tt.field || !strings.Contains(validationErr.message, tt.contains) {
				t.Fatalf("field=%v message=%q, want %s containing %q", validationErr.details["field"], validationErr.message, tt.field, tt.contains)
			}
		})
	}
}

func TestAPIJobRunsRejectsPartitionKeysWithoutIceberg(t *testing.T) {
	st := openTestStore(t)
	seedExistingJobRunFixture(t, st, "job-partition-keys-disabled", true, true)
	srv := newSubmitTestServer(st)

	rec := submitWithPartitionKeys(t, srv, "/api/jobs/job-partition-keys-disabled/runs", `{"enabled": false, "partition_keys": ["tenant_id"]}`)

	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "iceberg.partition_keys") {
		t.Fatalf("status=%d body=%s, want 400 naming iceberg.partition_keys", rec.Code, rec.Body.String())
	}
	if got := srv.lastRegistrationConfig(); len(got) != 0 {
		t.Fatalf("a run was planned despite the rejected option: %s", got)
	}
}

// Partitioning belongs to the run's registration snapshot; one-shot jobs no
// longer copy the keys into options_json, where nothing reads them.
func TestOneShotSubmitKeepsPartitionKeysOutOfJobOptions(t *testing.T) {
	req := validRunSubmitRequestForValidation()
	req.Iceberg = runSubmitIcebergRequest{Enabled: true, Engine: "rest-go", Table: "ns.orders", PartitionKeys: []string{"tenant_id"}}
	uri := "http://catalog:8181"
	req.Iceberg.Options.URI = &uri

	spec, err := validateRunSubmitRequest(req)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	job, err := buildOneShotJobRequest(spec, "src", "tgt")
	if err != nil {
		t.Fatalf("build job: %v", err)
	}

	if strings.Contains(string(job.OptionsJSON), "partition_keys") {
		t.Fatalf("one-shot job options carry partition_keys: %s", job.OptionsJSON)
	}
	if got, want := spec.IcebergRunConfig.PartitionSpec, identityFields("tenant_id"); !reflect.DeepEqual(got, want) {
		t.Fatalf("run partition_spec=%+v want %+v", got, want)
	}
}

// Older versions copied iceberg.partition_keys into the reused one-shot job,
// which job validation now refuses. A later submit must still run and drop the
// stale key.
func TestOneShotSubmitReplacesStoredPartitionKeys(t *testing.T) {
	srv := newSubmitTestServer(openTestStore(t))
	submit := func() string {
		t.Helper()
		rec := submitWithPartitionKeys(t, srv, "/api/v1/runs/submit", `{"enabled": false}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		var resp struct {
			JobID string `json:"job_id"`
		}
		decodeJSONBody(t, rec, &resp)
		return resp.JobID
	}
	ctx := context.Background()
	jobID := submit()
	job, err := srv.st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	var options map[string]any
	if err := json.Unmarshal(job.OptionsJSON, &options); err != nil {
		t.Fatal(err)
	}
	options["partition_keys"] = []string{"id"}
	if job.OptionsJSON, err = json.Marshal(options); err != nil {
		t.Fatal(err)
	}
	if err := srv.st.UpdateJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	if again := submit(); again != jobID {
		t.Fatalf("job %s, want the same job %s reused", again, jobID)
	}

	stored, err := srv.st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(stored.OptionsJSON), "partition_keys") {
		t.Fatalf("stale partition_keys kept: %s", stored.OptionsJSON)
	}
}

func TestJobOptionPartitionKeysMessageNamesTheRunSetting(t *testing.T) {
	if !strings.Contains(jobopts.ErrPartitionKeysIneffective, "iceberg.partition_keys") ||
		!strings.Contains(jobopts.ErrPartitionKeysIneffective, "iceberg.options.partition_spec") {
		t.Fatalf("job option message %q does not name the run settings", jobopts.ErrPartitionKeysIneffective)
	}
}
