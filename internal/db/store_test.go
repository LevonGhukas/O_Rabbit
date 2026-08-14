package db

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	st, err := Open(context.Background(), Config{Path: dbPath}, slog.Default())
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestUpdateWorkerHeartbeatPreservesAddrOnEmptyUpdate(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	if err := st.UpdateWorkerHeartbeat(ctx, "", "worker-1", "127.0.0.1:9102", `{"ver":1}`, "", "", 0); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}
	if err := st.UpdateWorkerHeartbeat(ctx, "", "worker-1", "", `{"ver":2}`, "", "", 0); err != nil {
		t.Fatalf("follow-up heartbeat: %v", err)
	}

	ws, err := st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers: %v", err)
	}
	if len(ws) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(ws))
	}
	if ws[0].Addr != "127.0.0.1:9102" {
		t.Fatalf("worker addr changed unexpectedly: %q", ws[0].Addr)
	}

	var cap map[string]int
	if err := json.Unmarshal(ws[0].Capabilities, &cap); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	if cap["ver"] != 2 {
		t.Fatalf("capabilities were not updated, got: %+v", cap)
	}
}

func TestStoreReadySucceedsForHealthyStore(t *testing.T) {
	st := openTestStore(t)
	if err := st.Ready(context.Background()); err != nil {
		t.Fatalf("store readiness failed: %v", err)
	}
}

func TestStoreReadyFailsForClosedStore(t *testing.T) {
	st := openTestStore(t)
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := st.Ready(context.Background()); err == nil {
		t.Fatalf("expected readiness failure for closed store")
	}
}

func TestStartRunWithTasksAuditedTransitionsRunAndPersistsAudit(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	run := Run{
		ID:            "run-audit",
		JobID:         "job-audit",
		DatasetKey:    "dataset-key",
		Status:        "PLANNING",
		CorrelationID: "corr-audit",
		StartedAt:     nowUTC(),
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	tasks := []TaskInsert{{
		ID:            "task-audit-1",
		RunID:         run.ID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        "PENDING",
	}}
	audit := AuditRecord{
		ActorType:    "token",
		ActorID:      "sha256:test",
		Action:       "job.run_start",
		ResourceType: "run",
		ResourceID:   run.ID,
	}
	if admitted, err := st.StartRunWithTasksAudited(ctx, run, tasks, audit); err != nil {
		t.Fatalf("start run with audit: %v", err)
	} else if !admitted {
		t.Fatal("expected run admission")
	}

	gotRun, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if gotRun.Status != "RUNNING" {
		t.Fatalf("run status=%q want RUNNING", gotRun.Status)
	}

	gotTasks, err := st.ListTasksForRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(gotTasks) != 1 {
		t.Fatalf("task count=%d want=1", len(gotTasks))
	}

	audits, err := st.ListAuditRecords(ctx, 10)
	if err != nil {
		t.Fatalf("list audits: %v", err)
	}
	if len(audits) != 1 {
		t.Fatalf("audit count=%d want=1", len(audits))
	}
	if audits[0].Action != "job.run_start" {
		t.Fatalf("audit action=%q want job.run_start", audits[0].Action)
	}
	var meta map[string]any
	if err := json.Unmarshal(audits[0].MetadataJSON, &meta); err != nil {
		t.Fatalf("decode audit metadata: %v", err)
	}
	if meta["task_count"] != float64(1) {
		t.Fatalf("task_count=%v want=1", meta["task_count"])
	}
}

func TestCreateRunPersistsRegistrationConfigSnapshot(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	run := Run{
		ID:                     "run-reg-config",
		JobID:                  "job-reg-config",
		DatasetKey:             "dataset-key",
		Status:                 "PLANNING",
		CorrelationID:          "corr-reg-config",
		StartedAt:              nowUTC(),
		RegistrationConfigJSON: json.RawMessage(`{"enabled":true,"engine":"rest-go","table":"mssql.orders","uri":"http://catalog:8181","bearer_token":"token"}`),
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	got, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if string(got.RegistrationConfigJSON) != string(run.RegistrationConfigJSON) {
		t.Fatalf("registration_config_json=%s want %s", got.RegistrationConfigJSON, run.RegistrationConfigJSON)
	}
}

func TestFailRunningRunsForJobFailsPlanningRunsToo(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	jobID := "job-1"
	planningRun := Run{
		ID:            "run-planning",
		JobID:         jobID,
		Status:        "PLANNING",
		CorrelationID: "corr-planning",
		StartedAt:     nowUTC(),
	}
	runningRun := Run{
		ID:            "run-running",
		JobID:         jobID,
		Status:        "RUNNING",
		CorrelationID: "corr-running",
		StartedAt:     nowUTC(),
	}
	if err := st.CreateRun(ctx, planningRun); err != nil {
		t.Fatalf("create planning run: %v", err)
	}
	if err := st.CreateRun(ctx, runningRun); err != nil {
		t.Fatalf("create running run: %v", err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{{
		ID:            "task-1",
		RunID:         runningRun.ID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        "PENDING",
	}}); err != nil {
		t.Fatalf("insert task: %v", err)
	}

	n, err := st.FailRunningRunsForJob(ctx, jobID, "test cleanup")
	if err != nil {
		t.Fatalf("fail runs for job: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 failed runs, got %d", n)
	}

	gotPlanning, err := st.GetRun(ctx, planningRun.ID)
	if err != nil {
		t.Fatalf("get planning run: %v", err)
	}
	if gotPlanning.Status != "FAILED" {
		t.Fatalf("planning run status = %q, want FAILED", gotPlanning.Status)
	}

	gotRunning, err := st.GetRun(ctx, runningRun.ID)
	if err != nil {
		t.Fatalf("get running run: %v", err)
	}
	if gotRunning.Status != "FAILED" {
		t.Fatalf("running run status = %q, want FAILED", gotRunning.Status)
	}

	tasks, err := st.ListTasksForRun(ctx, runningRun.ID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Status != "FAILED" {
		t.Fatalf("task status = %q, want FAILED", tasks[0].Status)
	}
}

func TestFailAllRunningRunsFailsActiveRunsOnly(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	planningRun := Run{
		ID:            "run-planning-all",
		JobID:         "job-1",
		Status:        "PLANNING",
		CorrelationID: "corr-planning-all",
		StartedAt:     nowUTC(),
	}
	runningRun := Run{
		ID:            "run-running-all",
		JobID:         "job-2",
		Status:        "RUNNING",
		CorrelationID: "corr-running-all",
		StartedAt:     nowUTC(),
	}
	succeededRun := Run{
		ID:            "run-succeeded-all",
		JobID:         "job-3",
		Status:        "SUCCEEDED",
		CorrelationID: "corr-succeeded-all",
		StartedAt:     nowUTC(),
	}
	if err := st.CreateRun(ctx, planningRun); err != nil {
		t.Fatalf("create planning run: %v", err)
	}
	if err := st.CreateRun(ctx, runningRun); err != nil {
		t.Fatalf("create running run: %v", err)
	}
	if err := st.CreateRun(ctx, succeededRun); err != nil {
		t.Fatalf("create succeeded run: %v", err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{
		{
			ID:            "task-planning-all",
			RunID:         planningRun.ID,
			TaskIndex:     1,
			PartitionSpec: []byte(`{"type":"single"}`),
			Status:        "RUNNING",
		},
		{
			ID:            "task-running-all",
			RunID:         runningRun.ID,
			TaskIndex:     1,
			PartitionSpec: []byte(`{"type":"single"}`),
			Status:        "PENDING",
		},
		{
			ID:            "task-succeeded-all",
			RunID:         succeededRun.ID,
			TaskIndex:     1,
			PartitionSpec: []byte(`{"type":"single"}`),
			Status:        "SUCCEEDED",
		},
	}); err != nil {
		t.Fatalf("insert tasks: %v", err)
	}

	n, err := st.FailAllRunningRuns(ctx, "master restarted")
	if err != nil {
		t.Fatalf("fail all running runs: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 failed runs, got %d", n)
	}

	gotPlanning, err := st.GetRun(ctx, planningRun.ID)
	if err != nil {
		t.Fatalf("get planning run: %v", err)
	}
	if gotPlanning.Status != "FAILED" {
		t.Fatalf("planning run status = %q, want FAILED", gotPlanning.Status)
	}
	if gotPlanning.ErrorSummary == nil || *gotPlanning.ErrorSummary != "master restarted" {
		t.Fatalf("planning run error summary = %v, want %q", gotPlanning.ErrorSummary, "master restarted")
	}

	gotRunning, err := st.GetRun(ctx, runningRun.ID)
	if err != nil {
		t.Fatalf("get running run: %v", err)
	}
	if gotRunning.Status != "FAILED" {
		t.Fatalf("running run status = %q, want FAILED", gotRunning.Status)
	}
	if gotRunning.ErrorSummary == nil || *gotRunning.ErrorSummary != "master restarted" {
		t.Fatalf("running run error summary = %v, want %q", gotRunning.ErrorSummary, "master restarted")
	}

	gotSucceeded, err := st.GetRun(ctx, succeededRun.ID)
	if err != nil {
		t.Fatalf("get succeeded run: %v", err)
	}
	if gotSucceeded.Status != "SUCCEEDED" {
		t.Fatalf("succeeded run status = %q, want SUCCEEDED", gotSucceeded.Status)
	}

	planningTasks, err := st.ListTasksForRun(ctx, planningRun.ID)
	if err != nil {
		t.Fatalf("list planning tasks: %v", err)
	}
	if len(planningTasks) != 1 {
		t.Fatalf("expected 1 planning task, got %d", len(planningTasks))
	}
	if planningTasks[0].Status != "FAILED" {
		t.Fatalf("planning task status = %q, want FAILED", planningTasks[0].Status)
	}
	if planningTasks[0].ErrorMessage == nil || *planningTasks[0].ErrorMessage != "master restarted" {
		t.Fatalf("planning task error message = %v, want %q", planningTasks[0].ErrorMessage, "master restarted")
	}

	runningTasks, err := st.ListTasksForRun(ctx, runningRun.ID)
	if err != nil {
		t.Fatalf("list running tasks: %v", err)
	}
	if len(runningTasks) != 1 {
		t.Fatalf("expected 1 running task, got %d", len(runningTasks))
	}
	if runningTasks[0].Status != "FAILED" {
		t.Fatalf("running task status = %q, want FAILED", runningTasks[0].Status)
	}
	if runningTasks[0].ErrorMessage == nil || *runningTasks[0].ErrorMessage != "master restarted" {
		t.Fatalf("running task error message = %v, want %q", runningTasks[0].ErrorMessage, "master restarted")
	}

	succeededTasks, err := st.ListTasksForRun(ctx, succeededRun.ID)
	if err != nil {
		t.Fatalf("list succeeded tasks: %v", err)
	}
	if len(succeededTasks) != 1 {
		t.Fatalf("expected 1 succeeded task, got %d", len(succeededTasks))
	}
	if succeededTasks[0].Status != "SUCCEEDED" {
		t.Fatalf("succeeded task status = %q, want SUCCEEDED", succeededTasks[0].Status)
	}
}

func TestCreateRunRejectsActiveDatasetCollision(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	first := Run{
		ID:            "run-1",
		JobID:         "job-a",
		DatasetKey:    "http://minio:9000|bucket|mssql/orders",
		Status:        "RUNNING",
		CorrelationID: "corr-1",
		StartedAt:     nowUTC(),
	}
	if err := st.CreateRun(ctx, first); err != nil {
		t.Fatalf("create first run: %v", err)
	}

	second := Run{
		ID:            "run-2",
		JobID:         "job-b",
		DatasetKey:    first.DatasetKey,
		Status:        "PLANNING",
		CorrelationID: "corr-2",
		StartedAt:     nowUTC(),
	}
	err := st.CreateRun(ctx, second)
	if err == nil {
		t.Fatalf("expected collision error for active dataset run")
	}
	if !errors.Is(err, ErrActiveDatasetRun) {
		t.Fatalf("expected ErrActiveDatasetRun, got %v", err)
	}
}

func TestCreateRunRejectsDatasetCollisionWhileCommitting(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	first := Run{ID: "run-committing", JobID: "job-a", DatasetKey: "bucket|dataset", Status: "COMMITTING", CorrelationID: "c1", StartedAt: nowUTC()}
	if err := st.CreateRun(ctx, first); err != nil {
		t.Fatalf("create committing run: %v", err)
	}
	second := Run{ID: "run-next", JobID: "job-b", DatasetKey: first.DatasetKey, Status: "PLANNING", CorrelationID: "c2", StartedAt: nowUTC()}
	if err := st.CreateRun(ctx, second); !errors.Is(err, ErrActiveDatasetRun) {
		t.Fatalf("error=%v want ErrActiveDatasetRun", err)
	}
	if got, ok, err := st.FindActiveRunByDatasetKey(ctx, first.DatasetKey); err != nil || !ok || got.ID != first.ID {
		t.Fatalf("active run=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestTryFinalizeRunEntersCommittingBeforeSucceeded(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	run := Run{ID: "run-finalize", JobID: "job", DatasetKey: "dataset-finalize", Status: "RUNNING", CorrelationID: "c", StartedAt: nowUTC()}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{{ID: "task-finalize", RunID: run.ID, TaskIndex: 1, PartitionSpec: []byte(`{}`), Status: "SUCCEEDED"}}); err != nil {
		t.Fatal(err)
	}
	changed, status, err := st.TryFinalizeRun(ctx, run.ID)
	if err != nil || !changed || status != "COMMITTING" {
		t.Fatalf("changed=%v status=%q err=%v", changed, status, err)
	}
	got, _ := st.GetRun(ctx, run.ID)
	if got.Status != "COMMITTING" {
		t.Fatalf("stored status=%q", got.Status)
	}
	intent := []byte(`{"commit_id":"abc"}`)
	if err := st.SaveCommitIntent(ctx, run.ID, "abc", intent); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveCommitIntent(ctx, run.ID, "different", []byte(`{}`)); err == nil {
		t.Fatal("expected conflicting intent rejection")
	}
	if err := st.CompleteRunCommit(ctx, run.ID); err == nil {
		t.Fatal("expected completion before verification to fail")
	}
	if err := st.SetCommitPhase(ctx, run.ID, "VERIFIED"); err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteRunCommit(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = st.GetRun(ctx, run.ID)
	if got.Status != "SUCCEEDED" {
		t.Fatalf("stored status=%q", got.Status)
	}
}

func TestCancelRunRejectsCommittingRun(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	run := Run{ID: "run-too-late", JobID: "job", DatasetKey: "dataset-too-late", Status: "COMMITTING", CorrelationID: "c", StartedAt: nowUTC()}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	changed, status, _, err := st.CancelRun(ctx, run.ID, "too late")
	if err != nil || changed || status != "COMMITTING" {
		t.Fatalf("changed=%v status=%q err=%v", changed, status, err)
	}
}

func TestRequeueTaskAssignmentMovesTaskBackToPending(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	run := Run{
		ID:            "run-requeue",
		JobID:         "job-requeue",
		DatasetKey:    "k",
		Status:        "RUNNING",
		CorrelationID: "corr-requeue",
		StartedAt:     nowUTC(),
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{{
		ID:            "task-requeue",
		RunID:         run.ID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        "PENDING",
	}}); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, _, err := st.AssignNextPendingTask(ctx, "worker-1"); err != nil {
		t.Fatalf("assign pending task: %v", err)
	}

	if err := st.RequeueTaskAssignment(ctx, "task-requeue", "worker-1"); err != nil {
		t.Fatalf("requeue task: %v", err)
	}

	tasks, err := st.ListTasksForRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Status != "PENDING" {
		t.Fatalf("task status = %q, want PENDING", tasks[0].Status)
	}
	if tasks[0].WorkerID != nil {
		t.Fatalf("worker_id should be nil after requeue, got %q", *tasks[0].WorkerID)
	}
}

func TestCancelRunMarksPendingTasksCanceledAndPreventsFurtherAssignment(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	run := Run{
		ID:            "run-cancel",
		JobID:         "job-cancel",
		Status:        "RUNNING",
		CorrelationID: "corr-cancel",
		StartedAt:     nowUTC(),
	}
	if err := st.CreateJob(ctx, Job{
		ID:                 run.JobID,
		Name:               "job-cancel",
		SourceConnectionID: "src",
		TargetConnectionID: "tgt",
		SourceSQL:          "select 1",
		TargetNamespace:    "ns",
		TargetTable:        "tbl",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{}`),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{
		{
			ID:            "task-pending",
			RunID:         run.ID,
			TaskIndex:     1,
			PartitionSpec: []byte(`{"type":"single"}`),
			Status:        "PENDING",
		},
		{
			ID:            "task-running",
			RunID:         run.ID,
			TaskIndex:     2,
			PartitionSpec: []byte(`{"type":"single"}`),
			Status:        "PENDING",
		},
	}); err != nil {
		t.Fatalf("insert tasks: %v", err)
	}
	if _, ok, err := st.AssignNextPendingTask(ctx, "worker-1"); err != nil {
		t.Fatalf("assign next pending task: %v", err)
	} else if !ok {
		t.Fatalf("expected a task to be assigned before cancellation")
	}

	changed, status, pendingCanceled, err := st.CancelRun(ctx, run.ID, "canceled by test")
	if err != nil {
		t.Fatalf("cancel run: %v", err)
	}
	if !changed {
		t.Fatalf("expected cancel to change run state")
	}
	if status != "CANCELED" {
		t.Fatalf("cancel status=%q want CANCELED", status)
	}
	if pendingCanceled != 1 {
		t.Fatalf("pending tasks canceled=%d want=1", pendingCanceled)
	}

	gotRun, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if gotRun.Status != "CANCELED" {
		t.Fatalf("run status=%q want CANCELED", gotRun.Status)
	}

	tasks, err := st.ListTasksForRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("task count=%d want=2", len(tasks))
	}
	if tasks[0].Status != "CANCELED" {
		t.Fatalf("running task status=%q want CANCELED", tasks[0].Status)
	}
	if tasks[1].Status != "CANCELED" {
		t.Fatalf("pending task status=%q want CANCELED", tasks[1].Status)
	}

	if _, ok, err := st.AssignNextPendingTask(ctx, "worker-2"); err != nil {
		t.Fatalf("assign next pending task after cancellation: %v", err)
	} else if ok {
		t.Fatalf("expected no task assignment after cancellation")
	}
}

func TestTryFinalizeRunLeavesCanceledRunTerminal(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	run := Run{
		ID:            "run-cancel-final",
		JobID:         "job-cancel-final",
		Status:        "RUNNING",
		CorrelationID: "corr-cancel-final",
		StartedAt:     nowUTC(),
	}
	if err := st.CreateJob(ctx, Job{
		ID:                 run.JobID,
		Name:               "job-cancel-final",
		SourceConnectionID: "src",
		TargetConnectionID: "tgt",
		SourceSQL:          "select 1",
		TargetNamespace:    "ns",
		TargetTable:        "tbl",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{}`),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{{
		ID:            "task-cancel-final",
		RunID:         run.ID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        "RUNNING",
	}}); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, _, _, err := st.CancelRun(ctx, run.ID, "canceled by test"); err != nil {
		t.Fatalf("cancel run: %v", err)
	}
	accepted, msg, finalStatus, err := st.CompleteTask(ctx, "task-cancel-final", "SUCCEEDED", nil, []byte(`[]`), 10, 20, 30)
	if err != nil {
		t.Fatalf("complete task after cancel: %v", err)
	}
	if !accepted || msg == "" {
		t.Fatalf("expected late running task result to be accepted, got accepted=%v msg=%q", accepted, msg)
	}
	if finalStatus != "CANCELED" {
		t.Fatalf("finalStatus=%q want CANCELED", finalStatus)
	}

	tasks, err := st.ListTasksForRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("task count=%d want=1", len(tasks))
	}
	if tasks[0].Status != "CANCELED" {
		t.Fatalf("task status=%q want CANCELED", tasks[0].Status)
	}

	changed, status, err := st.TryFinalizeRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("try finalize run: %v", err)
	}
	if changed {
		t.Fatalf("canceled run should remain terminal without status change")
	}
	if status != "CANCELED" {
		t.Fatalf("status=%q want CANCELED", status)
	}
}

func TestCompleteTaskAllowsExplicitCanceledStatusForCanceledRun(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	run := Run{
		ID:            "run-explicit-cancel",
		JobID:         "job-explicit-cancel",
		Status:        "RUNNING",
		CorrelationID: "corr-explicit-cancel",
		StartedAt:     nowUTC(),
	}
	if err := st.CreateJob(ctx, Job{
		ID:                 run.JobID,
		Name:               "job-explicit-cancel",
		SourceConnectionID: "src",
		TargetConnectionID: "tgt",
		SourceSQL:          "select 1",
		TargetNamespace:    "ns",
		TargetTable:        "tbl",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{}`),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{{
		ID:            "task-explicit-cancel",
		RunID:         run.ID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        "RUNNING",
	}}); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, _, _, err := st.CancelRun(ctx, run.ID, "canceled by test"); err != nil {
		t.Fatalf("cancel run: %v", err)
	}

	reason := "worker checkpoint noticed cancellation"
	accepted, msg, finalStatus, err := st.CompleteTask(ctx, "task-explicit-cancel", "CANCELED", &reason, []byte(`[]`), 3, 4, 5)
	if err != nil {
		t.Fatalf("complete canceled task: %v", err)
	}
	if !accepted {
		t.Fatalf("expected explicit legacy canceled completion to be idempotent")
	}
	if msg != "already canceled" {
		t.Fatalf("msg=%q want already canceled", msg)
	}
	if finalStatus != "CANCELED" {
		t.Fatalf("finalStatus=%q want CANCELED", finalStatus)
	}

	tasks, err := st.ListTasksForRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if tasks[0].Status != "CANCELED" {
		t.Fatalf("task status=%q want CANCELED", tasks[0].Status)
	}
	if tasks[0].ErrorMessage == nil || *tasks[0].ErrorMessage != "canceled by test" {
		t.Fatalf("task error=%v want cancellation reason", tasks[0].ErrorMessage)
	}
}
