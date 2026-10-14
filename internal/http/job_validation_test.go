package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
)

func jobValidationServer(t *testing.T) (*Server, *db.Store) {
	t.Helper()
	st := openTestStore(t)
	createTestConnection(t, st, db.Connection{ID: "src", Name: "src", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: []byte(`{}`)})
	createTestConnection(t, st, db.Connection{ID: "tgt", Name: "tgt", Kind: "target", Engine: "s3", MetadataJSON: []byte(`{"bucket":"b","endpoint":"http://127.0.0.1:1"}`), SecretEncBlob: []byte(`{}`)})
	return NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, ""), st
}

func jobBody(options string) string {
	return `{"name":"job","source_connection_id":"src","target_connection_id":"tgt","target_table":"t","write_mode":"overwrite","options_json":` + options + `}`
}

func serveJSON(srv *Server, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

type jobErrorResponse struct {
	Error struct {
		Code         string `json:"code"`
		Message      string `json:"message"`
		FailureClass string `json:"failure_class"`
		Details      struct {
			Errors []struct {
				Field string `json:"field"`
				Error string `json:"error"`
			} `json:"errors"`
		} `json:"details"`
	} `json:"error"`
}

func decodeJobError(t *testing.T, rec *httptest.ResponseRecorder) jobErrorResponse {
	t.Helper()
	var resp jobErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
	return resp
}

func (r jobErrorResponse) hasField(field string) bool {
	for _, e := range r.Error.Details.Errors {
		if e.Field == field {
			return true
		}
	}
	return false
}

// The cases from the bug report: each must be rejected on create and on
// update with 400, and never stored.
var invalidJobOptions = map[string]string{
	"options_json":                    `"not an object"`,
	"options_json.planned_tasks":      `{"planned_tasks":"ten"}`,
	"options_json.worker_pool":        `{"worker_pool":"Bad Pool!"}`,
	"options_json.partition_strategy": `{"partition_strategy":"bogus"}`,
	"options_json.where_clause":       `{"where_clause":"1=1; DROP TABLE x"}`,
	"options_json.column_types.a":     `{"column_types":{"a":"notatype<"}}`,
	"options_json.planed_tasks":       `{"planed_tasks":4}`,
}

func TestJobCreateRejectsInvalidOptionsJSON(t *testing.T) {
	for field, options := range invalidJobOptions {
		srv, st := jobValidationServer(t)
		rec := serveJSON(srv, http.MethodPost, "/api/v1/jobs", jobBody(options))
		resp := decodeJobError(t, rec)
		if rec.Code != http.StatusBadRequest || resp.Error.Code != string(httperr.CodeInvalidInput) || !resp.hasField(field) {
			t.Errorf("%s: status=%d body=%s, want 400 invalid_input naming %s", options, rec.Code, rec.Body.String(), field)
		}
		if jobs, _ := st.ListJobs(context.Background()); len(jobs) != 0 {
			t.Errorf("%s: %d job(s) stored, want none", options, len(jobs))
		}
	}
}

func TestJobUpdateRejectsInvalidOptionsJSONAndKeepsStoredJob(t *testing.T) {
	for field, options := range invalidJobOptions {
		srv, st := jobValidationServer(t)
		created := serveJSON(srv, http.MethodPost, "/api/v1/jobs", jobBody(`{"partition_strategy":"single"}`))
		if created.Code != http.StatusCreated {
			t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
		}
		var job db.Job
		decodeJSONBody(t, created, &job)

		rec := serveJSON(srv, http.MethodPut, "/api/v1/jobs/"+job.ID, jobBody(options))
		if resp := decodeJobError(t, rec); rec.Code != http.StatusBadRequest || !resp.hasField(field) {
			t.Errorf("%s: status=%d body=%s, want 400 naming %s", options, rec.Code, rec.Body.String(), field)
		}
		stored, err := st.GetJob(context.Background(), job.ID)
		if err != nil || string(stored.OptionsJSON) != `{"partition_strategy":"single"}` {
			t.Errorf("%s: stored options=%s err=%v, want the previous options unchanged", options, stored.OptionsJSON, err)
		}
	}
}

func TestJobValidationReportsEveryProblemAtOnce(t *testing.T) {
	srv, _ := jobValidationServer(t)
	body := `{"name":" ","source_connection_id":"missing","target_connection_id":"src","options_json":{"planned_tasks":"ten","planed_tasks":4}}`
	rec := serveJSON(srv, http.MethodPost, "/api/v1/jobs", body)
	resp := decodeJobError(t, rec)
	if rec.Code != http.StatusBadRequest || resp.Error.Message != "invalid job" {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, field := range []string{"name", "source_connection_id", "target_connection_id", "options_json.planned_tasks", "options_json.planed_tasks"} {
		if !resp.hasField(field) {
			t.Errorf("missing error for %s in %s", field, rec.Body.String())
		}
	}
	if !strings.Contains(rec.Body.String(), "is a source connection, not a target connection") {
		t.Errorf("want the wrong-kind explanation, got %s", rec.Body.String())
	}
}

func TestStoredJobRoundTripsThroughGetAndPut(t *testing.T) {
	// Options as older one-shot submits and the CLI stored them, including
	// keys that are no longer written; fetching and saving back must work.
	srv, st := jobValidationServer(t)
	options := `{"partition_strategy":"single","table":"public.t","iceberg_registration":{"enabled":true,"engine":"rest-go","table":"ns.t"},"iceberg_enabled":true,"iceberg_engine":"rest-go","iceberg_table":"ns.t","iceberg_partition_keys":null,"fetch_limit_rows":5000,"auto_tune":true,"target_file_bytes":0}`
	if err := st.CreateJob(context.Background(), db.Job{ID: "old", Name: "old", SourceConnectionID: "src", TargetConnectionID: "tgt", TargetTable: "t", WriteMode: "overwrite", OptionsJSON: json.RawMessage(options)}); err != nil {
		t.Fatal(err)
	}
	got := serveJSON(srv, http.MethodGet, "/api/v1/jobs/old", "")
	if got.Code != http.StatusOK {
		t.Fatalf("get status=%d", got.Code)
	}
	rec := serveJSON(srv, http.MethodPut, "/api/v1/jobs/old", got.Body.String())
	if rec.Code != http.StatusOK {
		t.Fatalf("put back status=%d body=%s, want 200", rec.Code, rec.Body.String())
	}
}

func TestRunStartForStoredInvalidOptionsReturns422(t *testing.T) {
	// Jobs saved before validation existed: run start must explain the
	// problem as a configuration error (422), not fail with 500.
	for field, options := range map[string]string{
		"options_json":                    `"not an object"`,
		"options_json.planned_tasks":      `{"planned_tasks":"ten"}`,
		"options_json.worker_pool":        `{"worker_pool":"Bad Pool!"}`,
		"options_json.partition_strategy": `{"partition_strategy":"bogus"}`,
		"options_json.where_clause":       `{"partition_strategy":"single","where_clause":"1=1; DROP TABLE x"}`,
		"options_json.column_types.a":     `{"partition_strategy":"single","column_types":{"a":"notatype<"}}`,
	} {
		srv, st := jobValidationServer(t)
		if err := st.CreateJob(context.Background(), db.Job{ID: "bad", Name: "bad", SourceConnectionID: "src", TargetConnectionID: "tgt", TargetTable: "t", WriteMode: "overwrite", OptionsJSON: json.RawMessage(options)}); err != nil {
			t.Fatal(err)
		}
		rec := serveJSON(srv, http.MethodPost, "/api/v1/jobs/bad/runs", "")
		resp := decodeJobError(t, rec)
		if rec.Code != http.StatusUnprocessableEntity || resp.Error.FailureClass != string(failure.FailureConfigurationInvalid) || !resp.hasField(field) {
			t.Errorf("%s: status=%d body=%s, want 422 CONFIGURATION_INVALID naming %s", options, rec.Code, rec.Body.String(), field)
		}
		if runs, _ := st.ListRuns(context.Background()); len(runs) != 0 {
			t.Errorf("%s: %d run(s) created, want none", options, len(runs))
		}
	}
}

func TestJobWriteModeIsValidatedAndDefaulted(t *testing.T) {
	body := func(mode string, incremental bool) string {
		return fmt.Sprintf(`{"name":"job","source_connection_id":"src","target_connection_id":"tgt","target_table":"t","write_mode":%q,"incremental":%t,"hwm_column":"id","options_json":{"partition_strategy":"single"}}`, mode, incremental)
	}
	rejected := []struct {
		mode        string
		incremental bool
		contains    string
	}{
		{"overwrite", true, "cannot be combined with incremental"},
		{"append", false, "not supported for full"},
		{"replace", false, `got \"replace\"`},
	}
	for _, tc := range rejected {
		srv, st := jobValidationServer(t)
		rec := serveJSON(srv, http.MethodPost, "/api/v1/jobs", body(tc.mode, tc.incremental))
		if resp := decodeJobError(t, rec); rec.Code != http.StatusBadRequest || !resp.hasField("write_mode") || !strings.Contains(rec.Body.String(), tc.contains) {
			t.Errorf("%s incremental=%t: status=%d body=%s, want 400 write_mode containing %q", tc.mode, tc.incremental, rec.Code, rec.Body.String(), tc.contains)
		}
		if jobs, _ := st.ListJobs(context.Background()); len(jobs) != 0 {
			t.Errorf("%s incremental=%t: job stored, want none", tc.mode, tc.incremental)
		}
	}

	// Empty or differently spelled modes are stored in canonical form.
	stored := []struct {
		mode        string
		incremental bool
		want        string
	}{
		{"", false, "overwrite"},
		{"", true, "append"},
		{" Overwrite ", false, "overwrite"},
		{"APPEND", true, "append"},
	}
	for _, tc := range stored {
		srv, _ := jobValidationServer(t)
		rec := serveJSON(srv, http.MethodPost, "/api/v1/jobs", body(tc.mode, tc.incremental))
		var job db.Job
		if rec.Code != http.StatusCreated {
			t.Fatalf("%q incremental=%t: status=%d body=%s", tc.mode, tc.incremental, rec.Code, rec.Body.String())
		}
		decodeJSONBody(t, rec, &job)
		if job.WriteMode != tc.want {
			t.Errorf("%q incremental=%t: stored write_mode=%q, want %q", tc.mode, tc.incremental, job.WriteMode, tc.want)
		}
	}
}

func TestRunStartRejectsIncrementalOverwriteJob(t *testing.T) {
	// Stored before validation: replacing the table with only the new rows
	// (or with nothing, for a run with no new rows) would lose data.
	srv, st := jobValidationServer(t)
	hwm := "id"
	if err := st.CreateJob(context.Background(), db.Job{ID: "inc", Name: "inc", SourceConnectionID: "src", TargetConnectionID: "tgt", TargetTable: "t", WriteMode: "overwrite", Incremental: true, HWMColumn: &hwm, OptionsJSON: json.RawMessage(`{"partition_strategy":"single"}`)}); err != nil {
		t.Fatal(err)
	}
	rec := serveJSON(srv, http.MethodPost, "/api/v1/jobs/inc/runs", "")
	resp := decodeJobError(t, rec)
	if rec.Code != http.StatusUnprocessableEntity || resp.Error.FailureClass != string(failure.FailureConfigurationInvalid) || !resp.hasField("write_mode") {
		t.Fatalf("status=%d body=%s, want 422 CONFIGURATION_INVALID naming write_mode", rec.Code, rec.Body.String())
	}
	if runs, _ := st.ListRuns(context.Background()); len(runs) != 0 {
		t.Fatalf("%d run(s) created, want none", len(runs))
	}
}
