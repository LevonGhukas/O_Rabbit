package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestGlobalActiveRunAdmissionIsAtomicAndReusesTerminalCapacity(t *testing.T) {
	st := openTestStore(t)
	st.SetMaxActiveRuns(2)
	ctx := context.Background()

	const runs = 8
	for i := 0; i < runs; i++ {
		jobID := fmt.Sprintf("admission-job-%02d", i)
		runID := fmt.Sprintf("admission-run-%02d", i)
		if err := st.CreateJob(ctx, Job{ID: jobID, Name: jobID, SourceConnectionID: "src", TargetConnectionID: "dst", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: []byte(`{"max_in_flight_tasks":1}`)}); err != nil {
			t.Fatalf("create job %d: %v", i, err)
		}
		if err := st.CreateRun(ctx, Run{ID: runID, JobID: jobID, DatasetKey: "dataset-" + runID, Status: "PLANNING", CorrelationID: "corr-" + runID, StartedAt: fmt.Sprintf("2026-07-29T12:00:%02dZ", i)}); err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, runs)
	for i := 0; i < runs; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			runID := fmt.Sprintf("admission-run-%02d", i)
			_, err := st.StartRunWithTasks(ctx, Run{ID: runID}, []TaskInsert{{ID: "task-" + runID, RunID: runID, TaskIndex: 1, PartitionSpec: []byte(`{}`), Status: "PENDING"}})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("start run concurrently: %v", err)
		}
	}

	assertRunAdmissionCounts(t, st, 2, runs-2, runs)

	var terminalRun string
	if err := st.db.QueryRow(`SELECT id FROM runs WHERE status='RUNNING' ORDER BY id LIMIT 1`).Scan(&terminalRun); err != nil {
		t.Fatalf("select active run: %v", err)
	}
	reason := "test terminal capacity release"
	if err := st.UpdateRunStatus(ctx, terminalRun, "FAILED", true, &reason); err != nil {
		t.Fatalf("finish active run: %v", err)
	}

	const promoters = 6
	wg = sync.WaitGroup{}
	promoteErrs := make(chan error, promoters)
	for i := 0; i < promoters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := st.AdmitPendingRuns(ctx)
			promoteErrs <- err
		}()
	}
	wg.Wait()
	close(promoteErrs)
	for err := range promoteErrs {
		if err != nil {
			t.Fatalf("promote pending runs concurrently: %v", err)
		}
	}

	assertRunAdmissionCounts(t, st, 2, runs-3, runs)
	var attempts int
	if err := st.db.QueryRow(`SELECT COALESCE(SUM(attempt_count),0) FROM tasks`).Scan(&attempts); err != nil {
		t.Fatalf("sum attempts: %v", err)
	}
	if attempts != 0 {
		t.Fatalf("admission throttling consumed %d task attempts", attempts)
	}
}

func assertRunAdmissionCounts(t *testing.T, st *Store, active, planning, pendingTasks int) {
	t.Helper()
	var gotActive, gotPlanning, gotPending int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM runs WHERE status IN ('RUNNING','COMMITTING')`).Scan(&gotActive); err != nil {
		t.Fatalf("count active runs: %v", err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM runs WHERE status='PLANNING'`).Scan(&gotPlanning); err != nil {
		t.Fatalf("count planning runs: %v", err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE status='PENDING'`).Scan(&gotPending); err != nil {
		t.Fatalf("count pending tasks: %v", err)
	}
	if gotActive != active || gotPlanning != planning || gotPending != pendingTasks {
		t.Fatalf("active=%d planning=%d pending_tasks=%d want %d/%d/%d", gotActive, gotPlanning, gotPending, active, planning, pendingTasks)
	}
}

func TestGlobalRunAdmissionPreservesPerJobTaskLimit(t *testing.T) {
	st := openTestStore(t)
	st.SetMaxActiveRuns(1)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC)
	if err := st.CreateJob(ctx, Job{ID: "limited-job", Name: "limited-job", SourceConnectionID: "src", TargetConnectionID: "dst", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: []byte(`{"max_in_flight_tasks":1}`)}); err != nil {
		t.Fatal(err)
	}
	run := Run{ID: "limited-run", JobID: "limited-job", DatasetKey: "limited-dataset", Status: "PLANNING", CorrelationID: "corr", StartedAt: now.Format(time.RFC3339Nano)}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	tasks := []TaskInsert{
		{ID: "limited-task-1", RunID: run.ID, TaskIndex: 1, PartitionSpec: []byte(`{}`), Status: "PENDING"},
		{ID: "limited-task-2", RunID: run.ID, TaskIndex: 2, PartitionSpec: []byte(`{}`), Status: "PENDING"},
	}
	if admitted, err := st.StartRunWithTasks(ctx, run, tasks); err != nil || !admitted {
		t.Fatalf("start limited run admitted=%v err=%v", admitted, err)
	}
	policy := LeasePolicy{Duration: time.Minute, MaxAttempts: 3, MaxActiveTasks: 4}
	first, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-1", now, policy, fixedGenerator("limited-attempt-1"), fixedGenerator("limited-fence-1"))
	if err != nil || !ok {
		t.Fatalf("first assignment ok=%v err=%v", ok, err)
	}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-2", now, policy, fixedGenerator("unused-attempt"), fixedGenerator("unused-fence")); err != nil || ok {
		t.Fatalf("per-job limit did not block second task ok=%v err=%v", ok, err)
	}
	if _, _, _, err := st.CompleteTaskAttemptAt(ctx, "", first.ID, first.AttemptID, first.FencingToken, "worker-1", "SUCCEEDED", nil, []byte(`[]`), 0, 0, 0, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-2", now.Add(2*time.Second), policy, fixedGenerator("limited-attempt-2"), fixedGenerator("limited-fence-2")); err != nil || !ok {
		t.Fatalf("per-job capacity was not reused ok=%v err=%v", ok, err)
	}
}
