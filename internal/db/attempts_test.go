package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
)

func createLeaseTestTask(t *testing.T, st *Store, suffix string) {
	t.Helper()
	ctx := context.Background()
	jobID, runID, taskID := "job-"+suffix, "run-"+suffix, "task-"+suffix
	if err := st.CreateJob(ctx, Job{ID: jobID, Name: jobID, SourceConnectionID: "src", TargetConnectionID: "dst", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, Run{ID: runID, JobID: jobID, DatasetKey: "dataset-" + suffix, Status: "RUNNING", CorrelationID: "corr-" + suffix, StartedAt: nowUTC()}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{{ID: taskID, RunID: runID, TaskIndex: 1, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"}}); err != nil {
		t.Fatal(err)
	}
}

func fixedGenerator(values ...string) func() (string, error) {
	i := 0
	return func() (string, error) {
		if i >= len(values) {
			return "", fmt.Errorf("generator exhausted")
		}
		v := values[i]
		i++
		return v, nil
	}
}

func TestLeaseAssignmentIsAtomicAndFenced(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "atomic")
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	policy := LeasePolicy{Duration: 30 * time.Second, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Minute}

	var wg sync.WaitGroup
	results := make(chan Task, 2)
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, ok, err := st.AssignNextPendingTaskWithLease(context.Background(), "", fmt.Sprintf("worker-%d", i), now, policy, fixedGenerator(fmt.Sprintf("attempt-%d", i)), fixedGenerator(fmt.Sprintf("token-%d", i)))
			if err != nil {
				t.Errorf("assign: %v", err)
				return
			}
			if ok {
				results <- task
			}
		}()
	}
	wg.Wait()
	close(results)
	var got []Task
	for x := range results {
		got = append(got, x)
	}
	if len(got) != 1 {
		t.Fatalf("valid assignments=%d want 1", len(got))
	}
	a := got[0]
	if a.AttemptNumber != 1 || a.AttemptID == "" || a.FencingToken == "" {
		t.Fatalf("incomplete assignment: %+v", a)
	}
	if _, err := st.RenewTaskLease(context.Background(), "", a.ID, a.AttemptID, "wrong", *a.WorkerID, now.Add(time.Second), policy.Duration); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("wrong token renewal err=%v", err)
	}
	if _, err := st.RenewTaskLease(context.Background(), "", a.ID, a.AttemptID, a.FencingToken, "wrong-worker", now.Add(time.Second), policy.Duration); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("wrong worker renewal err=%v", err)
	}
	deadline, err := st.RenewTaskLease(context.Background(), "", a.ID, a.AttemptID, a.FencingToken, *a.WorkerID, now.Add(time.Second), policy.Duration)
	if err != nil {
		t.Fatal(err)
	}
	if deadline != now.Add(31*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("deadline=%s", deadline)
	}
}

func TestGlobalActiveTaskAdmissionLeavesExcessPending(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "global-cap-a")
	createLeaseTestTask(t, st, "global-cap-b")
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	policy := LeasePolicy{Duration: time.Minute, MaxAttempts: 3, MaxActiveTasks: 1, BackoffBase: time.Second, BackoffMax: time.Minute}

	first, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-1", now, policy, fixedGenerator("global-attempt-1"), fixedGenerator("global-token-1"))
	if err != nil || !ok {
		t.Fatalf("first assignment ok=%v err=%v", ok, err)
	}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-2", now, policy, fixedGenerator("unused-attempt"), fixedGenerator("unused-token")); err != nil || ok {
		t.Fatalf("throttled assignment ok=%v err=%v", ok, err)
	}
	pendingRun := "run-global-cap-a"
	if first.RunID == pendingRun {
		pendingRun = "run-global-cap-b"
	}
	tasks, err := st.ListTasksForRun(ctx, pendingRun)
	if err != nil || len(tasks) != 1 || tasks[0].Status != "PENDING" || tasks[0].AttemptCount != 0 {
		t.Fatalf("excess task was mutated: tasks=%+v err=%v", tasks, err)
	}
	if _, _, _, err := st.CompleteTaskAttemptAt(ctx, "", first.ID, first.AttemptID, first.FencingToken, "worker-1", "SUCCEEDED", nil, []byte(`[]`), 0, 0, 0, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-2", now.Add(2*time.Second), policy, fixedGenerator("global-attempt-2"), fixedGenerator("global-token-2")); err != nil || !ok {
		t.Fatalf("pending task did not become eligible ok=%v err=%v", ok, err)
	}
}

func TestLeaseExpiryReassignmentAndStaleResultFencing(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "retry")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: 10 * time.Second, MaxAttempts: 3, BackoffBase: 2 * time.Second, BackoffMax: time.Minute}
	a1, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-old", t0, p, fixedGenerator("attempt-old"), fixedGenerator("token-old"))
	if err != nil || !ok {
		t.Fatalf("assign 1 ok=%v err=%v", ok, err)
	}
	if n, err := st.ExpireTaskAttempts(ctx, t0.Add(11*time.Second), p); err != nil || n != 1 {
		t.Fatalf("expire n=%d err=%v", n, err)
	}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "too-early", t0.Add(12*time.Second), p, fixedGenerator("unused"), fixedGenerator("unused-token")); err != nil || ok {
		t.Fatalf("backoff assignment ok=%v err=%v", ok, err)
	}
	a2, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-new", t0.Add(13*time.Second), p, fixedGenerator("attempt-new"), fixedGenerator("token-new"))
	if err != nil || !ok {
		t.Fatalf("assign 2 ok=%v err=%v", ok, err)
	}
	if a2.AttemptNumber != 2 || a2.AttemptID == a1.AttemptID || a2.FencingToken == a1.FencingToken {
		t.Fatalf("reassignment identities: old=%+v new=%+v", a1, a2)
	}
	events, err := st.ListEventsForRun(ctx, "run-retry", 100)
	if err != nil {
		t.Fatal(err)
	}
	foundSuperseded := false
	for _, event := range events {
		if strings.Contains(string(event.FieldsJSON), `"event_type":"ATTEMPT_SUPERSEDED"`) && strings.Contains(string(event.FieldsJSON), `"replacement_attempt_id":"attempt-new"`) {
			foundSuperseded = true
		}
	}
	if !foundSuperseded {
		t.Fatal("reassignment did not emit supersession context")
	}
	if err := st.UpdateTaskProgressFencedAt(ctx, "", a1.ID, a1.AttemptID, a1.FencingToken, "worker-old", 99, 99, 99, t0.Add(14*time.Second)); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("stale progress err=%v", err)
	}
	if err := st.UpdateTaskProgressFencedAt(ctx, "", a2.ID, a2.AttemptID, a2.FencingToken, "worker-new", 2, 2, 2, t0.Add(14*time.Second)); err != nil {
		t.Fatalf("current progress: %v", err)
	}
	if _, _, _, err := st.CompleteTaskAttemptAt(ctx, "", a1.ID, a1.AttemptID, a1.FencingToken, "worker-old", "SUCCEEDED", nil, []byte(`[{"key":"stale"}]`), 1, 1, 1, t0.Add(14*time.Second)); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("stale success err=%v", err)
	}
	accepted, _, final, err := st.CompleteTaskAttemptAt(ctx, "", a2.ID, a2.AttemptID, a2.FencingToken, "worker-new", "SUCCEEDED", nil, []byte(`[{"key":"accepted"}]`), 2, 2, 2, t0.Add(14*time.Second))
	if err != nil || !accepted || final != "SUCCEEDED" {
		t.Fatalf("complete accepted=%v final=%s err=%v", accepted, final, err)
	}
	accepted, msg, _, err := st.CompleteTaskAttemptAt(ctx, "", a2.ID, a2.AttemptID, a2.FencingToken, "worker-new", "SUCCEEDED", nil, []byte(`[{"key":"accepted"}]`), 2, 2, 2, t0.Add(time.Hour))
	if err != nil || !accepted || msg != "already accepted" {
		t.Fatalf("duplicate accepted=%v msg=%q err=%v", accepted, msg, err)
	}
	if _, _, _, err := st.CompleteTaskAttemptAt(ctx, "", a2.ID, a2.AttemptID, a2.FencingToken, "worker-new", "SUCCEEDED", nil, []byte(`[{"key":"different"}]`), 2, 2, 2, t0.Add(time.Hour)); err == nil {
		t.Fatal("different duplicate result accepted")
	}
	staleFailure := "late old worker failure"
	if _, _, _, err := st.CompleteTaskAttemptAt(ctx, "", a1.ID, a1.AttemptID, a1.FencingToken, "worker-old", "FAILED", &staleFailure, []byte(`[]`), 0, 0, 0, t0.Add(time.Hour)); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("stale failure after new success err=%v", err)
	}
	tasks, err := st.ListTasksForRun(ctx, "run-retry")
	if err != nil || !strings.Contains(string(tasks[0].ParquetObjects), "accepted") || strings.Contains(string(tasks[0].ParquetObjects), "stale") {
		t.Fatalf("logical task adopted stale objects: %+v err=%v", tasks, err)
	}
	attempts, err := st.ListTaskAttempts(ctx, a1.ID)
	if err != nil || len(attempts) != 2 || attempts[0].Status != AttemptExpired || attempts[1].Status != AttemptSucceeded {
		t.Fatalf("attempt history=%+v err=%v", attempts, err)
	}
}

func TestRenewalExpirationRaceHasOneWinner(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "race")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: 10 * time.Second, MaxAttempts: 2, BackoffBase: time.Second, BackoffMax: time.Second}
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt"), fixedGenerator("token"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	var renewErr, expireErr error
	var expired int
	go func() {
		defer wg.Done()
		_, renewErr = st.RenewTaskLease(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", t0.Add(10*time.Second), p.Duration)
	}()
	go func() { defer wg.Done(); expired, expireErr = st.ExpireTaskAttempts(ctx, t0.Add(10*time.Second), p) }()
	wg.Wait()
	if expireErr != nil {
		t.Fatal(expireErr)
	}
	if expired != 1 || !errors.Is(renewErr, ErrAttemptFenced) {
		t.Fatalf("expired=%d renewErr=%v", expired, renewErr)
	}
}

func TestExpiryDoesNotRevokeRenewedCurrentAttempt(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "renewed-not-expired")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: 10 * time.Second, MaxAttempts: 2, BackoffBase: time.Second, BackoffMax: time.Second}
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "boot", "worker", t0, p, fixedGenerator("attempt"), fixedGenerator("token"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	if _, err := st.RenewTaskLease(ctx, "boot", a.ID, a.AttemptID, a.FencingToken, "worker", t0.Add(9*time.Second), p.Duration); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ExpireTaskAttempts(ctx, t0.Add(10*time.Second), p); err != nil || n != 0 {
		t.Fatalf("expiry revoked renewed attempt n=%d err=%v", n, err)
	}
	if err := st.UpdateTaskProgressFencedAt(ctx, "boot", a.ID, a.AttemptID, a.FencingToken, "worker", 1, 1, 1, t0.Add(10*time.Second)); err != nil {
		t.Fatalf("current renewed attempt lost ownership: %v", err)
	}
}

func TestRestartPreservesValidLeaseThenRequeuesExpiredAttempt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.sqlite")
	ctx := context.Background()
	st, err := Open(ctx, Config{Path: path}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	createLeaseTestTask(t, st, "restart")
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: time.Minute, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Second}
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt-before-restart"), fixedGenerator("token-before-restart"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(ctx, Config{Path: path}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.RenewTaskLease(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", t0.Add(30*time.Second), p.Duration); err != nil {
		t.Fatalf("valid pre-restart owner lost: %v", err)
	}
	if n, err := st.ExpireTaskAttempts(ctx, t0.Add(91*time.Second), p); err != nil || n != 1 {
		t.Fatalf("restart expire n=%d err=%v", n, err)
	}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-2", t0.Add(92*time.Second), p, fixedGenerator("attempt-after-restart"), fixedGenerator("token-after-restart")); err != nil || !ok {
		t.Fatalf("restart reassign ok=%v err=%v", ok, err)
	}
	attempts, err := st.ListTaskAttempts(ctx, a.ID)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempt history=%+v err=%v", attempts, err)
	}
}

func TestLeaseRetryLimitQuarantinesAndFailsRun(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "poison")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: time.Second, MaxAttempts: 1, BackoffBase: time.Second, BackoffMax: time.Second}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt"), fixedGenerator("token")); err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	if n, err := st.ExpireTaskAttempts(ctx, t0.Add(2*time.Second), p); err != nil || n != 1 {
		t.Fatalf("expire n=%d err=%v", n, err)
	}
	tasks, _ := st.ListTasksForRun(ctx, "run-poison")
	if len(tasks) != 1 || tasks[0].Status != "QUARANTINED" {
		t.Fatalf("tasks=%+v", tasks)
	}
	if tasks[0].AttemptStatus != AttemptExpired || tasks[0].FailureClass != "LEASE_EXPIRED" || tasks[0].AttemptNumber != 1 {
		t.Fatalf("operator attempt status incomplete: %+v", tasks[0])
	}
	serialized, err := json.Marshal(tasks[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(serialized), "token") || !strings.Contains(string(serialized), `"attempt_status":"EXPIRED"`) {
		t.Fatalf("unsafe/incomplete task JSON: %s", serialized)
	}
	run, err := st.GetRun(ctx, "run-poison")
	if err != nil || run.Status != "FAILED" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-2", t0.Add(time.Hour), p, fixedGenerator("no"), fixedGenerator("no")); err != nil || ok {
		t.Fatalf("poison reassigned ok=%v err=%v", ok, err)
	}
}

func TestAssignmentAbandonmentUsesBackoffBelowRetryLimit(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "abandon-retry")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: time.Minute, MaxAttempts: 2, BackoffBase: 2 * time.Second, BackoffMax: time.Minute}
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt-abandon-1"), fixedGenerator("token-abandon-1"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	if err := st.AbandonTaskAttemptWithPolicy(ctx, a.ID, a.AttemptID, "worker", "assignment build failed", t0, p); err != nil {
		t.Fatal(err)
	}
	tasks, err := st.ListTasksForRun(ctx, a.RunID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	task := tasks[0]
	if task.Status != "PENDING" || task.CurrentAttemptID != nil || task.WorkerID != nil || task.NextEligibleAt == nil || *task.NextEligibleAt != t0.Add(2*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("requeued task=%+v", task)
	}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "early", t0.Add(time.Second), p, fixedGenerator("unused"), fixedGenerator("unused")); err != nil || ok {
		t.Fatalf("early assignment ok=%v err=%v", ok, err)
	}
	a2, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-2", t0.Add(2*time.Second), p, fixedGenerator("attempt-abandon-2"), fixedGenerator("token-abandon-2"))
	if err != nil || !ok || a2.AttemptNumber != 2 {
		t.Fatalf("retry assignment=%+v ok=%v err=%v", a2, ok, err)
	}
}

func TestAssignmentAbandonmentAtRetryLimitQuarantinesAtomically(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "abandon-limit")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: time.Minute, MaxAttempts: 1, BackoffBase: time.Second, BackoffMax: time.Second}
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt-abandon-final"), fixedGenerator("token-abandon-final"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	if err := st.AbandonTaskAttemptWithPolicy(ctx, a.ID, a.AttemptID, "worker", "assignment build failed", t0, p); err != nil {
		t.Fatal(err)
	}
	if err := st.AbandonTaskAttemptWithPolicy(ctx, a.ID, a.AttemptID, "worker", "assignment build failed", t0, p); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("repeated abandonment err=%v", err)
	}
	tasks, err := st.ListTasksForRun(ctx, a.RunID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	task := tasks[0]
	if task.Status != "QUARANTINED" || task.CurrentAttemptID != nil || task.WorkerID != nil || task.NextEligibleAt != nil || task.FinishedAt == nil {
		t.Fatalf("quarantined task=%+v", task)
	}
	if task.Status == "PENDING" && task.AttemptCount >= p.MaxAttempts {
		t.Fatalf("non-assignable pending task=%+v", task)
	}
	run, err := st.GetRun(ctx, a.RunID)
	if err != nil || run.Status != "FAILED" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	attempts, err := st.ListTaskAttempts(ctx, a.ID)
	if err != nil || len(attempts) != 1 || attempts[0].Status != AttemptFailed || attempts[0].FailureClass != "ASSIGNMENT_BUILD_FAILURE" {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	events, err := st.ListEventsForRun(ctx, a.RunID, 100)
	if err != nil {
		t.Fatal(err)
	}
	quarantined := 0
	for _, event := range events {
		if strings.Contains(string(event.FieldsJSON), `"event_type":"TASK_QUARANTINED"`) {
			quarantined++
		}
	}
	if quarantined != 1 {
		t.Fatalf("quarantine events=%d want 1", quarantined)
	}
}

func TestConcurrentAssignmentAbandonmentHasOneTerminalTransition(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "abandon-concurrent")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: time.Minute, MaxAttempts: 1, BackoffBase: time.Second, BackoffMax: time.Second}
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt-abandon-concurrent"), fixedGenerator("token-abandon-concurrent"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}

	const callers = 8
	results := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- st.AbandonTaskAttemptWithPolicy(ctx, a.ID, a.AttemptID, "worker", "assignment build failed", t0, p)
		}()
	}
	wg.Wait()
	close(results)
	accepted, fenced := 0, 0
	for err := range results {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrAttemptFenced):
			fenced++
		default:
			t.Fatalf("unexpected abandonment error: %v", err)
		}
	}
	if accepted != 1 || fenced != callers-1 {
		t.Fatalf("accepted=%d fenced=%d", accepted, fenced)
	}
	tasks, err := st.ListTasksForRun(ctx, a.RunID)
	if err != nil || len(tasks) != 1 || tasks[0].Status != "QUARANTINED" {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	events, err := st.ListEventsForRun(ctx, a.RunID, 100)
	if err != nil {
		t.Fatal(err)
	}
	quarantined := 0
	for _, event := range events {
		if strings.Contains(string(event.FieldsJSON), `"event_type":"TASK_QUARANTINED"`) {
			quarantined++
		}
	}
	if quarantined != 1 {
		t.Fatalf("quarantine events=%d want 1", quarantined)
	}
}

func TestLeaseExpirationComparesInstantsNotTimestampText(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "timestamp")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: 10 * time.Second, MaxAttempts: 2, BackoffBase: time.Second, BackoffMax: time.Second}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt"), fixedGenerator("token")); err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	// RFC3339Nano renders the deadline as ...10Z and now as ...10.1Z. A
	// lexical comparison gets this ordering wrong.
	if n, err := st.ExpireTaskAttempts(ctx, t0.Add(10*time.Second+100*time.Millisecond), p); err != nil || n != 1 {
		t.Fatalf("expire n=%d err=%v", n, err)
	}
}

func TestExpiredLeaseCannotReportBeforeScannerRuns(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "deadline-fence")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: 10 * time.Second, MaxAttempts: 2}
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt"), fixedGenerator("token"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	now := t0.Add(11 * time.Second)
	if err := st.UpdateTaskProgressFencedAt(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", 1, 1, 1, now); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("expired progress err=%v", err)
	}
	if _, _, _, err := st.CompleteTaskAttemptAt(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", "SUCCEEDED", nil, []byte(`[]`), 1, 1, 1, now); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("expired result err=%v", err)
	}
}

func TestCancellationFencesActiveAttemptAndDoesNotRequeue(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "cancel-lease")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: time.Second, MaxAttempts: 3}
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt"), fixedGenerator("token"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	if _, _, _, err := st.CancelRun(ctx, "run-cancel-lease", "stop"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RenewTaskLease(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", t0, p.Duration); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("renew after cancel err=%v", err)
	}
	if n, err := st.ExpireTaskAttempts(ctx, t0.Add(time.Hour), p); err != nil || n != 0 {
		t.Fatalf("expire after cancel n=%d err=%v", n, err)
	}
	attempts, _ := st.ListTaskAttempts(ctx, a.ID)
	if len(attempts) != 1 || attempts[0].Status != AttemptCanceled {
		t.Fatalf("attempts=%+v", attempts)
	}
}

func TestAttemptLifecycleEventsAreStructuredIdempotentAndTokenSafe(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "events")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: 10 * time.Second, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Second}
	a1, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-1", t0, p, fixedGenerator("attempt-event-1"), fixedGenerator("raw-secret-token"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	for i := 0; i < 25; i++ {
		if _, err := st.RenewTaskLease(ctx, "", a1.ID, a1.AttemptID, a1.FencingToken, "worker-1", t0.Add(time.Duration(i+1)*100*time.Millisecond), p.Duration); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := st.ExpireTaskAttempts(ctx, t0.Add(13*time.Second), p); err != nil || n != 1 {
		t.Fatalf("expire n=%d err=%v", n, err)
	}
	if n, err := st.ExpireTaskAttempts(ctx, t0.Add(13*time.Second), p); err != nil || n != 0 {
		t.Fatalf("repeat expire n=%d err=%v", n, err)
	}
	events, err := st.ListEventsForRun(ctx, "run-events", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("events after renew/expiry=%d want assignment+expiry+retry", len(events))
	}
	wantTypes := map[string]bool{"ATTEMPT_ASSIGNED": false, "ATTEMPT_EXPIRED": false, "TASK_RETRY_SCHEDULED": false}
	for _, event := range events {
		if strings.Contains(string(event.FieldsJSON), "raw-secret-token") {
			t.Fatal("event exposed fencing token")
		}
		var fields map[string]any
		if err := json.Unmarshal(event.FieldsJSON, &fields); err != nil {
			t.Fatal(err)
		}
		if eventType, ok := fields["event_type"].(string); ok {
			wantTypes[eventType] = true
		}
		if fields["attempt_id"] == "" || fields["worker_id"] == "" {
			t.Fatalf("incomplete event fields: %s", event.FieldsJSON)
		}
	}
	for eventType, seen := range wantTypes {
		if !seen {
			t.Fatalf("missing event type %s", eventType)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = st.InsertAttemptRejectionEvent(ctx, a1.ID, a1.AttemptID, "stale-worker", "STALE_RESULT_REJECTED", "OWNERSHIP_FENCED", t0.Add(time.Hour))
		}()
	}
	wg.Wait()
	events, err = st.ListEventsForRun(ctx, "run-events", 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if strings.Contains(string(event.FieldsJSON), `"event_type":"STALE_RESULT_REJECTED"`) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("bounded stale result events=%d want 1", count)
	}
}

func TestRetryExhaustionAndCancellationEventsAreSingleAndActionable(t *testing.T) {
	t.Run("quarantine", func(t *testing.T) {
		st := openTestStore(t)
		createLeaseTestTask(t, st, "event-quarantine")
		ctx := context.Background()
		t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
		p := LeasePolicy{Duration: time.Second, MaxAttempts: 1}
		if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, p, fixedGenerator("attempt-q"), fixedGenerator("token-q")); err != nil || !ok {
			t.Fatal(err)
		}
		if _, err := st.ExpireTaskAttempts(ctx, t0.Add(2*time.Second), p); err != nil {
			t.Fatal(err)
		}
		if _, err := st.ExpireTaskAttempts(ctx, t0.Add(3*time.Second), p); err != nil {
			t.Fatal(err)
		}
		events, _ := st.ListEventsForRun(ctx, "run-event-quarantine", 100)
		q := 0
		for _, event := range events {
			if strings.Contains(string(event.FieldsJSON), `"event_type":"TASK_QUARANTINED"`) {
				q++
			}
		}
		if q != 1 {
			t.Fatalf("quarantine events=%d", q)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		st := openTestStore(t)
		createLeaseTestTask(t, st, "event-cancel")
		ctx := context.Background()
		t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
		if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, LeasePolicy{Duration: time.Minute}, fixedGenerator("attempt-c"), fixedGenerator("token-c")); err != nil || !ok {
			t.Fatal(err)
		}
		if _, _, _, err := st.CancelRun(ctx, "run-event-cancel", "operator stop"); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := st.CancelRun(ctx, "run-event-cancel", "operator stop"); err != nil {
			t.Fatal(err)
		}
		events, _ := st.ListEventsForRun(ctx, "run-event-cancel", 100)
		c := 0
		for _, event := range events {
			if strings.Contains(string(event.FieldsJSON), `"event_type":"ATTEMPT_CANCELED"`) {
				c++
			}
		}
		if c != 1 {
			t.Fatalf("cancellation events=%d", c)
		}
	})
}

func TestFencedArtifactPersistenceIsAtomicAndIdempotent(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "artifact")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	p := LeasePolicy{Duration: time.Minute, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Second}
	a1, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "old", t0, p, fixedGenerator("attempt-art-old"), fixedGenerator("token-old"))
	if err != nil || !ok {
		t.Fatal(err)
	}
	if _, err := st.ExpireTaskAttempts(ctx, t0.Add(time.Minute), p); err != nil {
		t.Fatal(err)
	}
	a2, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "new", t0.Add(time.Minute+time.Second), p, fixedGenerator("attempt-art-new"), fixedGenerator("token-new"))
	if err != nil || !ok {
		t.Fatal(err)
	}
	record := artifact.Record{ObjectKey: "run/task/attempt/file.parquet", ByteSize: 10, SHA256: strings.Repeat("a", 64), RowCount: 2, SchemaFingerprint: strings.Repeat("b", 64), RunID: a2.RunID, TaskID: a2.ID, AttemptID: a2.AttemptID, AttemptNumber: a2.AttemptNumber, FileIndex: 0, FormatVersion: artifact.FormatVersion, VerificationMethod: artifact.VerificationPortable, VerificationStatus: artifact.VerificationVerified}
	stale := record
	stale.AttemptID = a1.AttemptID
	stale.AttemptNumber = a1.AttemptNumber
	stale.ObjectKey = "stale.parquet"
	if _, _, _, err := st.CompleteTaskAttemptWithArtifactsAt(ctx, "", a1.ID, a1.AttemptID, a1.FencingToken, "old", "SUCCEEDED", nil, []artifact.Record{stale}, 2, 0, 10, t0.Add(time.Minute+2*time.Second)); !errors.Is(err, ErrAttemptFenced) {
		t.Fatalf("stale artifact err=%v", err)
	}
	accepted, msg, _, err := st.CompleteTaskAttemptWithArtifactsAt(ctx, "", a2.ID, a2.AttemptID, a2.FencingToken, "new", "SUCCEEDED", nil, []artifact.Record{record}, 2, 0, 10, t0.Add(time.Minute+2*time.Second))
	if err != nil || !accepted || msg != "accepted" {
		t.Fatalf("accepted=%v msg=%s err=%v", accepted, msg, err)
	}
	accepted, msg, _, err = st.CompleteTaskAttemptWithArtifactsAt(ctx, "", a2.ID, a2.AttemptID, a2.FencingToken, "new", "SUCCEEDED", nil, []artifact.Record{record}, 2, 0, 10, t0.Add(time.Hour))
	if err != nil || !accepted || msg != "already accepted" {
		t.Fatalf("duplicate accepted=%v msg=%s err=%v", accepted, msg, err)
	}
	conflict := record
	conflict.SHA256 = strings.Repeat("c", 64)
	if _, _, _, err := st.CompleteTaskAttemptWithArtifactsAt(ctx, "", a2.ID, a2.AttemptID, a2.FencingToken, "new", "SUCCEEDED", nil, []artifact.Record{conflict}, 2, 0, 10, t0.Add(time.Hour)); err == nil {
		t.Fatal("conflicting artifact accepted")
	}
	artifacts, err := st.ListArtifactsForRun(ctx, a2.RunID)
	if err != nil || len(artifacts) != 1 || artifacts[0].SHA256 != record.SHA256 {
		t.Fatalf("artifacts=%+v err=%v", artifacts, err)
	}
	tasks, err := st.ListTasksForRun(ctx, a2.RunID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	if tasks[0].ArtifactCount != 1 || tasks[0].ArtifactBytes != 10 || tasks[0].ArtifactRows != 2 || tasks[0].VerificationStatus != artifact.VerificationVerified || tasks[0].VerificationMethod != artifact.VerificationPortable {
		t.Fatalf("artifact status=%+v", tasks[0])
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO task_artifacts(id,task_id,attempt_id,file_index,object_key,byte_size,sha256,row_count,schema_fingerprint,run_id,attempt_number,format_version,verification_method,verification_status,verified_at,max_hwm,created_at) VALUES('invalid-artifact',?,?,1,'invalid.parquet',1,?,0,?,?,?,1,'PORTABLE_FULL_SHA256','VERIFIED','x','','x')`, a2.ID, a2.AttemptID, strings.Repeat("A", 64), strings.Repeat("b", 64), a2.RunID, a2.AttemptNumber); err == nil {
		t.Fatal("database accepted non-canonical digest")
	}
}

func TestArtifactWorkerIdentityCannotOverrideDurableRunOrAttemptNumber(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "artifact-identity")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, LeasePolicy{Duration: time.Minute}, fixedGenerator("attempt-id"), fixedGenerator("token-id"))
	if err != nil || !ok {
		t.Fatal(err)
	}
	record := artifact.Record{ObjectKey: "wrong.parquet", ByteSize: 1, SHA256: strings.Repeat("a", 64), RowCount: 0, SchemaFingerprint: strings.Repeat("b", 64), RunID: "worker-invented-run", TaskID: a.ID, AttemptID: a.AttemptID, AttemptNumber: a.AttemptNumber + 1, FileIndex: 0, FormatVersion: artifact.FormatVersion, VerificationMethod: artifact.VerificationPortable, VerificationStatus: artifact.VerificationVerified}
	if _, _, _, err := st.CompleteTaskAttemptWithArtifactsAt(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", "SUCCEEDED", nil, []artifact.Record{record}, 0, 0, 1, t0.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "durable identity mismatch") {
		t.Fatalf("identity error=%v", err)
	}
	artifacts, err := st.ListArtifactsForRun(ctx, a.RunID)
	if err != nil || len(artifacts) != 0 {
		t.Fatalf("invented artifacts=%+v err=%v", artifacts, err)
	}
}

func TestExplicitFailureClassificationOverridesMessage(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "artifact-failure-class")
	ctx := context.Background()
	t0 := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, LeasePolicy{Duration: time.Minute}, fixedGenerator("attempt-class"), fixedGenerator("token-class"))
	if err != nil || !ok {
		t.Fatal(err)
	}
	message := string(artifact.FailureRemoteChecksumMismatch) + ": object bytes differ"
	if accepted, _, _, err := st.CompleteTaskAttemptWithArtifactsAndFailureClassAt(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", "FAILED", &message, string(failure.FailureConfigurationUnavailable), nil, 0, 0, 0, t0.Add(time.Second)); err != nil || !accepted {
		t.Fatalf("accepted=%v err=%v", accepted, err)
	}
	attempts, err := st.ListTaskAttempts(ctx, a.ID)
	if err != nil || len(attempts) != 1 || attempts[0].Status != "FAILED" || attempts[0].FailureClass != string(failure.FailureConfigurationUnavailable) {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
}
