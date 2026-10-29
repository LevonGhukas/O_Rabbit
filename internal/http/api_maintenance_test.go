package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func TestHandleMaintenanceSubmitMethodNotAllowed(t *testing.T) {
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/maintenance", nil)

	srv.handleMaintenanceSubmit(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleMaintenanceSubmitInvalidJSON(t *testing.T) {
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/maintenance", strings.NewReader("{"))

	srv.handleMaintenanceSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleMaintenanceSubmitInvalidOperation(t *testing.T) {
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "")

	body := `{"operation":"drop"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/maintenance", strings.NewReader(body))

	srv.handleMaintenanceSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusBadRequest)
	}
}

func seedMaintenanceJob(t *testing.T) (*Server, *db.Store) {
	t.Helper()
	st := openTestStore(t)
	if err := st.CreateJob(context.Background(), db.Job{ID: "maint-job", Name: "maint-job", SourceConnectionID: "source", TargetConnectionID: "target", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "table", WriteMode: "overwrite", OptionsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	return NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, ""), st
}

func postMaintenance(srv *Server, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/maintenance/submit", strings.NewReader(body))
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func TestMaintenanceSubmitDefaultsToDryRunAndIsInspectable(t *testing.T) {
	srv, _ := seedMaintenanceJob(t)
	rec := postMaintenance(srv, `{"job_id":"maint-job","operation":"vacuum","orphan_cleanup":{"min_age_hours":48}}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Operation db.MaintenanceOperation `json:"operation"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	op := created.Operation
	var params struct {
		MinAgeHours float64 `json:"min_age_hours"`
	}
	if err := json.Unmarshal(op.Params, &params); err != nil {
		t.Fatal(err)
	}
	if op.Kind != db.MaintenanceOrphanCleanup || !op.DryRun || op.Status != db.MaintenancePending || params.MinAgeHours != 48 {
		t.Fatalf("operation=%+v params=%s", op, op.Params)
	}

	get := serveRoute(srv, http.MethodGet, "/api/v1/maintenance/"+op.ID, "")
	var shown struct {
		Operation  db.MaintenanceOperation   `json:"operation"`
		Candidates []db.MaintenanceCandidate `json:"candidates"`
	}
	if get.Code != http.StatusOK || json.Unmarshal(get.Body.Bytes(), &shown) != nil || shown.Operation.ID != op.ID || shown.Candidates == nil {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}
	list := serveRoute(srv, http.MethodGet, "/api/v1/maintenance?job_id=maint-job", "")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), op.ID) {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	if missing := serveRoute(srv, http.MethodGet, "/api/v1/maintenance/nope", ""); missing.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d", missing.Code)
	}
}

func TestMaintenanceSubmitAllowsOneActiveOperationPerJob(t *testing.T) {
	srv, _ := seedMaintenanceJob(t)
	if rec := postMaintenance(srv, `{"job_id":"maint-job","operation":"compaction","dry_run":false}`); rec.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := postMaintenance(srv, `{"job_id":"maint-job","operation":"orphan_cleanup"}`); rec.Code != http.StatusConflict {
		t.Fatalf("second status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestMaintenanceSubmitValidatesRequests(t *testing.T) {
	srv, _ := seedMaintenanceJob(t)
	for body, want := range map[string]int{
		`{"operation":"orphan_cleanup"}`:                                                                              http.StatusBadRequest,
		`{"job_id":"missing","operation":"orphan_cleanup"}`:                                                           http.StatusNotFound,
		`{"job_id":"maint-job","operation":"orphan_cleanup","orphan_cleanup":{"min_age_hours":0.5}}`:                  http.StatusBadRequest,
		`{"job_id":"maint-job","operation":"orphan_cleanup","compaction":{}}`:                                         http.StatusBadRequest,
		`{"job_id":"maint-job","operation":"compaction","compaction":{"target_file_bytes":10,"small_file_bytes":20}}`: http.StatusBadRequest,
		`{"job_id":"maint-job","operation":"compaction","dryrun":false}`:                                              http.StatusBadRequest,
	} {
		if rec := postMaintenance(srv, body); rec.Code != want {
			t.Fatalf("%s: status=%d want %d body=%s", body, rec.Code, want, rec.Body.String())
		}
	}
}
