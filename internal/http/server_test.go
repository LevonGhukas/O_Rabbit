package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
)

func TestHandlerAuthMiddlewareReturnsJSONUnauthorized(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{PID: 1, HTTPAddr: ":9100", GRPCAddr: ":9102", DBPath: "test.sqlite"}, "topsecret")
	h := srv.Handler()

	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthRec := httptest.NewRecorder()
	h.ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("healthz status=%d want=%d", healthRec.Code, http.StatusOK)
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/status", nil)
	statusRec := httptest.NewRecorder()
	h.ServeHTTP(statusRec, statusReq)
	resp := decodeErrorResponse(t, statusRec)
	if statusRec.Code != http.StatusUnauthorized {
		t.Fatalf("status without auth=%d want=%d", statusRec.Code, http.StatusUnauthorized)
	}
	if got := statusRec.Header().Get("WWW-Authenticate"); got == "" {
		t.Fatalf("missing WWW-Authenticate header")
	}
	if resp.Error.Code != httperr.CodeUnauthorized {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeUnauthorized)
	}
	if resp.Error.Message != "unauthorized" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "unauthorized")
	}
	errObj := decodeErrorObject(t, statusRec)
	if _, ok := errObj["details"]; ok {
		t.Fatalf("details field should be omitted for unauthorized response")
	}

	authReq := httptest.NewRequest(http.MethodGet, "/status", nil)
	authReq.Header.Set("Authorization", "Bearer topsecret")
	authRec := httptest.NewRecorder()
	h.ServeHTTP(authRec, authReq)
	if authRec.Code != http.StatusOK {
		t.Fatalf("status with auth=%d want=%d", authRec.Code, http.StatusOK)
	}
}

func TestBearerAuthenticationCasesDoNotLeakToken(t *testing.T) {
	const secret = "admin-token-0123456789"
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, secret)
	h := srv.Handler()
	tests := []struct {
		name   string
		header string
		want   int
	}{
		{name: "valid", header: "Bearer " + secret, want: http.StatusOK},
		{name: "invalid", header: "Bearer wrong-token", want: http.StatusUnauthorized},
		{name: "missing", want: http.StatusUnauthorized},
		{name: "empty", header: "Bearer ", want: http.StatusUnauthorized},
		{name: "near-match", header: "Bearer admin-token-0123456788", want: http.StatusUnauthorized},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/status", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.want, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), secret) || strings.Contains(rec.Header().Get("WWW-Authenticate"), secret) {
				t.Fatal("authentication response leaked configured token")
			}
		})
	}
}

func TestHealthzRemainsPlainTextLiveness(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, "topsecret")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusOK)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != "ok" {
		t.Fatalf("body=%q want=%q", body, "ok")
	}
}

func TestReadyReturnsJSONOKWithoutAuth(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "topsecret")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusOK)
	}
	var resp readinessResponse
	decodeJSONBody(t, rec, &resp)
	if !resp.OK {
		t.Fatalf("ready response ok=%v want true", resp.OK)
	}
	if got := resp.Checks["db"]; got != "ok" {
		t.Fatalf("db check=%q want=%q", got, "ok")
	}
}

func TestReadyReturnsServiceUnavailableWhenStoreNotReady(t *testing.T) {
	st := openTestStore(t)
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusServiceUnavailable)
	}
	var resp readinessResponse
	decodeJSONBody(t, rec, &resp)
	if resp.OK {
		t.Fatalf("ready response ok=%v want false", resp.OK)
	}
	if got := resp.Checks["db"]; got != "error" {
		t.Fatalf("db check=%q want=%q", got, "error")
	}
}

func TestStatusMethodNotAllowedReturnsJSON(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{PID: 1, HTTPAddr: ":9100", GRPCAddr: ":9102", DBPath: "test.sqlite"}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/status", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusMethodNotAllowed)
	}
	if resp.Error.Code != httperr.CodeMethodNotAllowed {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("allow header=%q want=%q", got, http.MethodGet)
	}
	details := detailsMap(t, resp.Error.Details)
	if details["method"] != http.MethodPost {
		t.Fatalf("method detail=%v want=%q", details["method"], http.MethodPost)
	}
}

func TestInvalidJSONBodyReturnsStructuredError(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/connections", strings.NewReader("{"))

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusBadRequest)
	}
	if resp.Error.Code != httperr.CodeInvalidInput {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeInvalidInput)
	}
	if resp.Error.Message != "invalid JSON body" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "invalid JSON body")
	}
	details := detailsMap(t, resp.Error.Details)
	if strings.TrimSpace(stringValue(details["cause"])) == "" {
		t.Fatalf("expected non-empty cause detail")
	}
}

func TestUnknownRouteReturnsJSONNotFound(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotFound)
	}
	if resp.Error.Code != httperr.CodeNotFound {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeNotFound)
	}
	if resp.Error.Message != "route not found" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "route not found")
	}
	details := detailsMap(t, resp.Error.Details)
	if details["path"] != "/does-not-exist" {
		t.Fatalf("path detail=%v want=%q", details["path"], "/does-not-exist")
	}
}

func TestUnknownRouteWithAuthReturnsJSONNotFound(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "topsecret")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotFound)
	}
	if resp.Error.Code != httperr.CodeNotFound {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeNotFound)
	}
}

func TestConnectionNotFoundReturnsStructuredError(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/connections/missing", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotFound)
	}
	if resp.Error.Code != httperr.CodeNotFound {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeNotFound)
	}
	if resp.Error.Message != "connection not found" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "connection not found")
	}
}

func TestRunCancelEndpointCancelsPendingTasksAndPublishesEvent(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	run := db.Run{
		ID:            "run-cancel-http",
		JobID:         "job-cancel-http",
		Status:        "RUNNING",
		CorrelationID: "corr-cancel-http",
		StartedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []db.TaskInsert{
		{
			ID:            "task-cancel-running",
			RunID:         run.ID,
			TaskIndex:     1,
			PartitionSpec: []byte(`{"type":"single"}`),
			Status:        "RUNNING",
		},
		{
			ID:            "task-cancel-pending",
			RunID:         run.ID,
			TaskIndex:     2,
			PartitionSpec: []byte(`{"type":"single"}`),
			Status:        "PENDING",
		},
	}); err != nil {
		t.Fatalf("insert tasks: %v", err)
	}

	bc := NewBroadcaster(nil)
	ch, unsub := bc.Subscribe(run.ID)
	defer unsub()

	srv := NewServer(nil, st, bc, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/runs/"+run.ID+"/cancel", strings.NewReader(`{}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp struct {
		Run struct {
			Status string `json:"status"`
		} `json:"run"`
		Canceled             bool `json:"canceled"`
		PendingTasksCanceled int  `json:"pending_tasks_canceled"`
	}
	decodeJSONBody(t, rec, &resp)
	if !resp.Canceled {
		t.Fatalf("canceled=%v want true", resp.Canceled)
	}
	if resp.Run.Status != "CANCELED" {
		t.Fatalf("run status=%q want CANCELED", resp.Run.Status)
	}
	if resp.PendingTasksCanceled != 1 {
		t.Fatalf("pending_tasks_canceled=%d want=1", resp.PendingTasksCanceled)
	}

	gotRun, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if gotRun.Status != "CANCELED" {
		t.Fatalf("stored run status=%q want CANCELED", gotRun.Status)
	}
	tasks, err := st.ListTasksForRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if tasks[0].Status != "CANCELED" || tasks[1].Status != "CANCELED" {
		t.Fatalf("task statuses after cancel = %q/%q want CANCELED/CANCELED", tasks[0].Status, tasks[1].Status)
	}

	select {
	case ev := <-ch:
		if ev.Message != "run CANCELED" {
			t.Fatalf("event message=%q want %q", ev.Message, "run CANCELED")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cancel event")
	}
}

func TestRunCancelEndpointRejectsTerminalRuns(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	run := db.Run{
		ID:            "run-cancel-terminal",
		JobID:         "job-cancel-terminal",
		Status:        "SUCCEEDED",
		CorrelationID: "corr-cancel-terminal",
		StartedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/runs/"+run.ID+"/cancel", strings.NewReader(`{}`))

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if resp.Error.Message != "run is already succeeded" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "run is already succeeded")
	}
}

func TestDeleteMissingConnectionReturnsStructuredError(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/connections/missing", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotFound)
	}
	if resp.Error.Code != httperr.CodeNotFound {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeNotFound)
	}
	if resp.Error.Message != "connection not found" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "connection not found")
	}
}

func TestConnectionDeleteConflictReturnsStructuredError(t *testing.T) {
	st := openTestStore(t)
	conn := db.Connection{
		ID:            "conn-1",
		Name:          "source-1",
		Kind:          "source",
		Engine:        "postgres",
		MetadataJSON:  []byte(`{}`),
		SecretEncBlob: []byte("secret"),
	}
	if err := st.CreateConnection(context.Background(), conn); err != nil {
		t.Fatalf("create connection: %v", err)
	}
	job := db.Job{
		ID:                 "job-1",
		Name:               "job-1",
		SourceConnectionID: conn.ID,
		TargetConnectionID: conn.ID,
		SourceSQL:          "SELECT 1",
		TargetNamespace:    "ns",
		TargetTable:        "tbl",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{}`),
	}
	if err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/connections/"+conn.ID, nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusConflict)
	}
	if resp.Error.Code != httperr.CodeConflict {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeConflict)
	}
	details := detailsMap(t, resp.Error.Details)
	if details["job_count"] != float64(1) {
		t.Fatalf("job_count detail=%v want=1", details["job_count"])
	}
}

func TestInternalErrorReturnsGenericStructuredJSON(t *testing.T) {
	st := openTestStore(t)
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/connections", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusInternalServerError)
	}
	if resp.Error.Code != httperr.CodeInternalError {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeInternalError)
	}
	if resp.Error.Message != "failed to list connections" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "failed to list connections")
	}
}

func TestJobNotFoundReturnsStructuredError(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/jobs/missing", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotFound)
	}
	if resp.Error.Code != httperr.CodeNotFound {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeNotFound)
	}
	if resp.Error.Message != "job not found" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "job not found")
	}
}

func TestDeleteMissingJobReturnsStructuredError(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/jobs/missing", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotFound)
	}
	if resp.Error.Code != httperr.CodeNotFound {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeNotFound)
	}
	if resp.Error.Message != "job not found" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "job not found")
	}
}

func TestJobExtraSegmentsReturnJSONNotFound(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")
	tests := []string{
		"/jobs/job-1/extra",
		"/jobs/job-1/runs/extra",
	}
	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			srv.Handler().ServeHTTP(rec, req)
			resp := decodeErrorResponse(t, rec)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotFound)
			}
			if resp.Error.Code != httperr.CodeNotFound {
				t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeNotFound)
			}
			if resp.Error.Message != "route not found" {
				t.Fatalf("error message=%q want=%q", resp.Error.Message, "route not found")
			}
		})
	}
}

func TestParseJobRouteRejectsMalformedPaths(t *testing.T) {
	tests := []string{
		"/jobs//runs",
		"/jobs/",
		"/jobs/job-1//runs",
		"/jobs/job-1/extra",
	}
	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			_, _, ok := parseJobRoute(path)
			if ok {
				t.Fatalf("parseJobRoute(%q) unexpectedly succeeded", path)
			}
		})
	}
}

func TestJobRunsDatasetBusyReturnsStructuredConflict(t *testing.T) {
	st := openTestStore(t)
	createTestConnection(t, st, db.Connection{
		ID:            "src-1",
		Name:          "src-1",
		Kind:          "source",
		Engine:        "postgres",
		MetadataJSON:  []byte(`{}`),
		SecretEncBlob: []byte(`{"dsn":"postgres://example"}`),
	})
	createTestConnection(t, st, db.Connection{
		ID:     "tgt-1",
		Name:   "tgt-1",
		Kind:   "target",
		Engine: "s3",
		MetadataJSON: []byte(`{
			"endpoint":"http://localhost:9000",
			"bucket":"bucket1",
			"prefix":"exports"
		}`),
		SecretEncBlob: []byte(`{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}`),
	})
	job := db.Job{
		ID:                 "job-1",
		Name:               "job-1",
		SourceConnectionID: "src-1",
		TargetConnectionID: "tgt-1",
		TargetTable:        "big_table",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{}`),
	}
	if err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	basePrefix := dataset.Prefix("exports", "postgres", "big_table")
	datasetKey := dataset.StorageKey("http://localhost:9000", "bucket1", basePrefix)
	if err := st.CreateRun(context.Background(), db.Run{
		ID:            "run-busy",
		JobID:         "other-job",
		DatasetKey:    datasetKey,
		Status:        "RUNNING",
		CorrelationID: "corr-busy",
		StartedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create busy run: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs/job-1/runs", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusConflict)
	}
	if resp.Error.Code != httperr.CodeDatasetBusy {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeDatasetBusy)
	}
	if resp.Error.Message != "dataset is busy" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "dataset is busy")
	}
	errObj := decodeErrorObject(t, rec)
	if _, ok := errObj["details"]; ok {
		t.Fatalf("details field should be omitted for dataset_busy response")
	}
}

func TestJobRunsPersistRegistrationConfigSnapshot(t *testing.T) {
	st := openTestStore(t)
	createTestConnection(t, st, db.Connection{
		ID:            "src-reg",
		Name:          "src-reg",
		Kind:          "source",
		Engine:        "mssql",
		MetadataJSON:  []byte(`{}`),
		SecretEncBlob: []byte(`{"dsn":"sqlserver://sa:pass@example:1433?database=SalesDB"}`),
	})
	createTestConnection(t, st, db.Connection{
		ID:     "tgt-reg",
		Name:   "tgt-reg",
		Kind:   "target",
		Engine: "s3",
		MetadataJSON: []byte(`{
			"endpoint":"http://minio:9000",
			"region":"us-east-1",
			"bucket":"bucket1",
			"prefix":"exports",
			"force_path_style":true
		}`),
		SecretEncBlob: []byte(`{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}`),
	})
	job := db.Job{
		ID:                 "job-reg",
		Name:               "job-reg",
		SourceConnectionID: "src-reg",
		TargetConnectionID: "tgt-reg",
		TargetTable:        "Orders",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{"partition_strategy":"single","table":"SalesDB.dbo.Orders"}`),
	}
	if err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs/job-reg/runs", strings.NewReader(`{
		"registration_config": {
			"enabled": true,
			"engine": "rest-go",
			"table": "mssql.orders",
			"uri": "http://catalog:8181",
			"bearer_token": "token",
			"s3": {
				"endpoint": "http://minio:9000",
				"region": "us-east-1",
				"path_style_access": true,
				"access_key_id": "minioadmin",
				"secret_access_key": "minioadmin"
			}
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var resp struct {
		Run db.Run `json:"run"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
	gotRun, err := st.GetRun(context.Background(), resp.Run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(gotRun.RegistrationConfigJSON, &cfg); err != nil {
		t.Fatalf("decode registration config: %v", err)
	}
	if cfg["uri"] != "http://catalog:8181" {
		t.Fatalf("uri=%v want http://catalog:8181", cfg["uri"])
	}
}

func TestJobRunsPersistIceRegistrationConfigSnapshotWithRawConfig(t *testing.T) {
	st := openTestStore(t)
	createTestConnection(t, st, db.Connection{
		ID:            "src-ice",
		Name:          "src-ice",
		Kind:          "source",
		Engine:        "mssql",
		MetadataJSON:  []byte(`{}`),
		SecretEncBlob: []byte(`{"dsn":"sqlserver://sa:pass@example:1433?database=SalesDB"}`),
	})
	createTestConnection(t, st, db.Connection{
		ID:     "tgt-ice",
		Name:   "tgt-ice",
		Kind:   "target",
		Engine: "s3",
		MetadataJSON: []byte(`{
			"endpoint":"http://minio:9000",
			"region":"us-east-1",
			"bucket":"bucket1",
			"prefix":"exports",
			"force_path_style":true
		}`),
		SecretEncBlob: []byte(`{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}`),
	})
	job := db.Job{
		ID:                 "job-ice",
		Name:               "job-ice",
		SourceConnectionID: "src-ice",
		TargetConnectionID: "tgt-ice",
		TargetTable:        "Orders",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{"partition_strategy":"single","table":"SalesDB.dbo.Orders"}`),
	}
	if err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs/job-ice/runs", strings.NewReader(`{
		"registration_config": {
			"enabled": true,
			"engine": "ice",
			"table": "mssql.orders",
			"uri": "http://catalog:8181",
			"bearer_token": "token",
			"config_yaml": "uri: http://catalog:8181\nbearerToken: token\nhttpCacheDir: data/ice/http/cache\n",
			"s3": {
				"endpoint": "http://minio:9000",
				"region": "us-east-1",
				"path_style_access": true,
				"access_key_id": "minioadmin",
				"secret_access_key": "minioadmin"
			}
		}
	}`))

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var resp struct {
		Run db.Run `json:"run"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
	gotRun, err := st.GetRun(context.Background(), resp.Run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	var cfg icebergreg.RunConfig
	if err := json.Unmarshal(gotRun.RegistrationConfigJSON, &cfg); err != nil {
		t.Fatalf("decode registration config: %v", err)
	}
	if cfg.Engine != "ice" {
		t.Fatalf("engine=%q want ice", cfg.Engine)
	}
	if cfg.ConfigYAML != "uri: http://catalog:8181\nbearerToken: token\nhttpCacheDir: data/ice/http/cache\n" {
		t.Fatalf("config_yaml=%q", cfg.ConfigYAML)
	}
}

func TestJobRunsGenericPlannerFailureReturnsStructuredInternalError(t *testing.T) {
	st := openTestStore(t)
	job := db.Job{
		ID:                 "job-bad",
		Name:               "job-bad",
		SourceConnectionID: "missing-src",
		TargetConnectionID: "missing-tgt",
		TargetTable:        "big_table",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{`),
	}
	if err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs/job-bad/runs", nil)

	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusInternalServerError)
	}
	if resp.Error.Code != httperr.CodeInternalError {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeInternalError)
	}
	if resp.Error.Message != "run planning failed" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "run planning failed")
	}
}

func TestRecovererReturnsStructuredJSONInternalError(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, "")
	h := srv.withRecoverer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)

	h.ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusInternalServerError)
	}
	if resp.Error.Code != httperr.CodeInternalError {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeInternalError)
	}
	if resp.Error.Message != "internal server error" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "internal server error")
	}
}

func openTestStore(t *testing.T) *db.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.sqlite")
	st, err := db.Open(context.Background(), db.Config{Path: path}, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})
	return st
}

func createTestConnection(t *testing.T, st *db.Store, c db.Connection) {
	t.Helper()
	if err := st.CreateConnection(context.Background(), c); err != nil {
		t.Fatalf("create connection %s: %v", c.ID, err)
	}
}

func decodeErrorResponse(t *testing.T, rec *httptest.ResponseRecorder) httperr.Response {
	t.Helper()
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("content-type=%q want application/json", got)
	}
	var resp httperr.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error response: %v\nbody=%s", err, rec.Body.String())
	}
	return resp
}

func decodeErrorObject(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode raw error response: %v\nbody=%s", err, rec.Body.String())
	}
	errObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("error object type=%T want map[string]any", body["error"])
	}
	return errObj
}

func detailsMap(t *testing.T, details any) map[string]any {
	t.Helper()
	if details == nil {
		t.Fatalf("expected details")
	}
	m, ok := details.(map[string]any)
	if !ok {
		t.Fatalf("details type=%T want map[string]any", details)
	}
	return m
}

func stringValue(v any) string {
	s, _ := v.(string)
	return s
}

func decodeJSONBody(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("content-type=%q want application/json", got)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json body: %v\nbody=%s", err, rec.Body.String())
	}
}
