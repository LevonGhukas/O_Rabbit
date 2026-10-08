package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

// persistingSubmitServer plans every submit as its own durable run so the
// test exercises real run/task persistence and admission, not a shared stub.
func persistingSubmitServer(t *testing.T) *Server {
	t.Helper()
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	srv.runPlanner = func(ctx context.Context, st *db.Store, _ crypto.Key, job db.Job, _ json.RawMessage, _ *db.AuditRecord) (db.Run, []db.TaskInsert, error) {
		run := db.Run{ID: newID(), JobID: job.ID, DatasetKey: "dataset-" + job.ID, Status: "PLANNING", CorrelationID: newID(), StartedAt: db.FormatTimestamp(time.Now())}
		if err := st.CreateRun(ctx, run); err != nil {
			return db.Run{}, nil, err
		}
		tasks := []db.TaskInsert{{ID: newID(), RunID: run.ID, TaskIndex: 1, PartitionSpec: []byte(`{"type":"sql_cursor_single"}`), Status: "PENDING"}}
		admitted, err := st.StartRunWithTasks(ctx, run, tasks)
		if err != nil {
			return db.Run{}, nil, err
		}
		if admitted {
			run.Status = "RUNNING"
		}
		return run, tasks, nil
	}
	return srv
}

func etlSubmitBody(i int) string {
	return fmt.Sprintf(`{
		"source": {
			"engine": "postgres",
			"dsn": "postgresql://user%d:pass@db%d:5432/app",
			"table": "public.orders_%d",
			"cursor_column": "id",
			"incremental": true
		},
		"target": {
			"s3_endpoint": "http://minio:9000",
			"s3_bucket": "bucket1",
			"s3_prefix": "pek/etl_%d",
			"s3_access_key_id": "minioadmin",
			"s3_secret_access_key": "miniosecret"
		}
	}`, i, i, i, i)
}

type submitResult struct {
	code  int
	body  string
	runID string
	jobID string
}

func submitETL(srv *Server, i int) submitResult {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runs/submit", strings.NewReader(etlSubmitBody(i)))
	srv.Handler().ServeHTTP(rec, req)
	out := submitResult{code: rec.Code, body: rec.Body.String()}
	var resp struct {
		RunID string `json:"run_id"`
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	out.runID, out.jobID = resp.RunID, resp.JobID
	return out
}

// assertIndependentSubmissions checks the TID-898 guarantees: every submit
// succeeds with its own run and job, and no submission rewrote the source
// credentials another run reads at task time.
func assertIndependentSubmissions(t *testing.T, srv *Server, results []submitResult) {
	t.Helper()
	ctx := context.Background()
	runs := map[string]bool{}
	for i, res := range results {
		if res.code != http.StatusCreated {
			t.Fatalf("submit %d: status=%d body=%s", i, res.code, res.body)
		}
		if res.runID == "" || runs[res.runID] {
			t.Fatalf("submit %d: run id %q missing or reused", i, res.runID)
		}
		runs[res.runID] = true

		run, err := srv.st.GetRun(ctx, res.runID)
		if err != nil {
			t.Fatalf("submit %d: run not persisted: %v", i, err)
		}
		if run.JobID != res.jobID {
			t.Fatalf("submit %d: run %s belongs to job %s, want %s", i, run.ID, run.JobID, res.jobID)
		}
		job, err := srv.st.GetJob(ctx, res.jobID)
		if err != nil {
			t.Fatal(err)
		}
		src, err := srv.st.GetConnection(ctx, job.SourceConnectionID)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := crypto.Decrypt(srv.k, src.SecretEncBlob, []byte(src.ID))
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("postgresql://user%d:pass@db%d:5432/app", i, i)
		if !strings.Contains(string(plain), want) {
			t.Fatalf("submit %d: source credentials were overwritten by another submission: %s", i, plain)
		}
	}
}

func TestSequentialETLSubmitsStayIndependent(t *testing.T) {
	srv := persistingSubmitServer(t)
	results := []submitResult{submitETL(srv, 0), submitETL(srv, 1)}
	assertIndependentSubmissions(t, srv, results)
}

func TestConcurrentETLSubmitsStayIndependent(t *testing.T) {
	for _, n := range []int{2, 5} {
		t.Run(fmt.Sprintf("%d_submits", n), func(t *testing.T) {
			srv := persistingSubmitServer(t)
			// Seed each job once so the concurrent round exercises the
			// update path that previously rewrote the shared source row.
			for i := 0; i < n; i++ {
				res := submitETL(srv, i)
				if res.code != http.StatusCreated {
					t.Fatalf("seed %d: status=%d body=%s", i, res.code, res.body)
				}
				if err := srv.st.UpdateRunStatus(context.Background(), res.runID, "FAILED", true, nil); err != nil {
					t.Fatal(err)
				}
			}
			results := make([]submitResult, n)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					results[i] = submitETL(srv, i)
				}(i)
			}
			close(start)
			wg.Wait()
			assertIndependentSubmissions(t, srv, results)
		})
	}
}
