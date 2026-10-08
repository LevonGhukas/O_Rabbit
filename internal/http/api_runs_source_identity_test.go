package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
)

func submitOneShotForSource(t *testing.T, srv *submitTestServer, dsn, table string) (jobID, connID string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {"engine": "postgres", "dsn": "`+dsn+`", "table": "`+table+`", "cursor_column": "id", "incremental": false},
		"target": {"s3_endpoint": "http://minio:9000", "s3_bucket": "b", "s3_access_key_id": "k", "s3_secret_access_key": "s"},
		"iceberg": {"enabled": false}
	}`))
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		JobID              string `json:"job_id"`
		SourceConnectionID string `json:"source_connection_id"`
	}
	decodeJSONBody(t, rec, &resp)
	return resp.JobID, resp.SourceConnectionID
}

func sourceDSNForJob(t *testing.T, srv *submitTestServer, jobID string) string {
	t.Helper()
	ctx := context.Background()
	job, err := srv.st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	conn, err := srv.st.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		t.Fatalf("get connection: %v", err)
	}
	plain, err := crypto.Decrypt(testCryptoKey, conn.SecretEncBlob, []byte(conn.ID))
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	return string(plain)
}

// A one-shot submit must never repoint another submit's source connection:
// workers resolve credentials at task start, so a shared connection would let
// an in-flight run read a different database.
func TestOneShotSubmitsForDifferentSourcesDoNotShareConnections(t *testing.T) {
	srv := newSubmitTestServer(openTestStore(t))

	jobA, connA := submitOneShotForSource(t, srv, "postgresql://u:p@tenant-a-db:5432/sales", "public.orders")
	_, connB := submitOneShotForSource(t, srv, "postgresql://u:p@tenant-b-db:5432/hr", "public.employees")

	if connA == connB {
		t.Fatalf("different DSNs share source connection %s", connA)
	}
	if got := sourceDSNForJob(t, srv, jobA); !strings.Contains(got, "tenant-a-db") {
		t.Fatalf("job A resolves %s, want tenant A's DSN", got)
	}
}

func TestOneShotSubmitsForSameSourceReuseConnection(t *testing.T) {
	srv := newSubmitTestServer(openTestStore(t))
	dsn := "postgresql://u:p@db:5432/sales"

	_, first := submitOneShotForSource(t, srv, dsn, "public.orders")
	_, second := submitOneShotForSource(t, srv, dsn, "public.customers")

	if first != second {
		t.Fatalf("same DSN created two connections: %s, %s", first, second)
	}
	conn, err := srv.st.GetConnection(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(conn.Name, "postgres_source-") || strings.Contains(conn.Name, "db:5432") {
		t.Fatalf("connection name %q must be engine-scoped and must not expose the DSN", conn.Name)
	}
}

// Submit must enforce the same column_types validation as validate, so a
// client that skips /validate gets a 400 instead of a late worker failure.
func TestRunSubmitRejectsInvalidColumnTypeOverrides(t *testing.T) {
	for _, override := range []string{"BIGINT", "VARCHAR(20)"} {
		for _, path := range []string{"/api/runs/submit", "/api/runs/validate"} {
			srv := newSubmitTestServer(openTestStore(t))
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{
				"source": {"engine": "postgres", "dsn": "postgresql://u:p@db:5432/app", "table": "public.orders",
					"cursor_column": "id", "incremental": false, "column_types": {"amount": "`+override+`"}},
				"target": {"s3_endpoint": "http://minio:9000", "s3_bucket": "b", "s3_access_key_id": "k", "s3_secret_access_key": "s"},
				"iceberg": {"enabled": false}
			}`))
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s %q: status=%d want 400 body=%s", path, override, rec.Code, rec.Body.String())
			}
			var resp struct {
				Error struct {
					Message string         `json:"message"`
					Details map[string]any `json:"details"`
				} `json:"error"`
			}
			decodeJSONBody(t, rec, &resp)
			if resp.Error.Message != "invalid column_types" || resp.Error.Details["column"] != "amount" {
				t.Fatalf("%s %q: error must name the column: %s", path, override, rec.Body.String())
			}
			if path == "/api/runs/submit" {
				if conns, _ := srv.st.ListConnections(context.Background()); len(conns) != 0 {
					t.Fatalf("rejected submit persisted %d connections", len(conns))
				}
			}
		}
	}
}

// An unreachable source is not the caller's fault and may succeed later.
func TestRunPlanningFailureForUnreachableSourceIsRetryable503(t *testing.T) {
	srv := newSubmitTestServer(openTestStore(t))
	srv.runPlanner = func(context.Context, *db.Store, crypto.Key, db.Job, json.RawMessage, *db.AuditRecord) (db.Run, []db.TaskInsert, error) {
		return db.Run{}, nil, errors.New("open source reader: dial tcp 10.0.0.5:5432: connect: connection refused")
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(`{
		"source": {"engine": "postgres", "dsn": "postgresql://u:p@db:5432/app", "table": "public.orders", "cursor_column": "id", "incremental": false},
		"target": {"s3_endpoint": "http://minio:9000", "s3_bucket": "b", "s3_access_key_id": "k", "s3_secret_access_key": "s"},
		"iceberg": {"enabled": false}
	}`))
	srv.Handler().ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusServiceUnavailable || resp.Error.Code != httperr.CodeRunPlanningFailed {
		t.Fatalf("status=%d code=%q, want 503 run_planning_failed", rec.Code, resp.Error.Code)
	}
	if resp.Error.FailureClass != "NETWORK_CONNECTION_FAILED" || resp.Error.Retryable == nil || !*resp.Error.Retryable {
		t.Fatalf("failure_class=%q retryable=%v", resp.Error.FailureClass, resp.Error.Retryable)
	}
}

// Concurrent one-shot submits for the same source and table must reuse one
// job and one source connection: job names are not unique in the schema, so
// the find-or-create step has to be atomic.
func TestConcurrentOneShotSubmitsShareOneJob(t *testing.T) {
	srv := newSubmitTestServer(openTestStore(t))
	body := `{
		"source": {"engine": "postgres", "dsn": "postgresql://u:p@db:5432/sales", "table": "public.orders", "cursor_column": "id", "incremental": false},
		"target": {"s3_endpoint": "http://minio:9000", "s3_bucket": "b", "s3_access_key_id": "k", "s3_secret_access_key": "s"},
		"iceberg": {"enabled": false}
	}`
	const submits = 8
	var wg sync.WaitGroup
	for i := 0; i < submits; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/runs/submit", strings.NewReader(body)))
		}()
	}
	wg.Wait()

	ctx := context.Background()
	jobs, err := srv.st.ListJobs(ctx)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs=%d after %d concurrent submits, want 1", len(jobs), submits)
	}
	conns, err := srv.st.ListConnections(ctx)
	if err != nil {
		t.Fatalf("list connections: %v", err)
	}
	sources := 0
	for _, c := range conns {
		if c.Kind == "source" {
			sources++
		}
	}
	if sources != 1 {
		t.Fatalf("source connections=%d, want 1", sources)
	}
}
