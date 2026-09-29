package db

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	secretcrypto "github.com/LevonGhukas/O_Rabbit/internal/crypto"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	st, err := Open(context.Background(), Config{Path: dbPath}, slog.Default())
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	st.SetMasterKey(testMasterKey(t))
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

	createRunConnections(t, st, "src-audit", "tgt-audit")
	if err := st.CreateJob(ctx, Job{ID: "job-audit", Name: "job-audit", SourceConnectionID: "src-audit", TargetConnectionID: "tgt-audit", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
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

	t.Setenv(
		"ORABBIT_MASTER_KEY",
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
	)

	k, err := secretcrypto.LoadMasterKeyFromEnv()
	if err != nil {
		t.Fatalf("load test master key: %v", err)
	}
	st.SetMasterKey(k)

	ctx := context.Background()

	run := Run{
		ID:            "run-reg-config",
		JobID:         "job-reg-config",
		DatasetKey:    "dataset-key",
		Status:        "PLANNING",
		CorrelationID: "corr-reg-config",
		StartedAt:     nowUTC(),
		RegistrationConfigJSON: json.RawMessage(
			`{"enabled":true,"engine":"rest-go","table":"mssql.orders","uri":"http://catalog:8181","bearer_token":"token"}`,
		),
	}

	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Verify the database does NOT contain the plaintext config.
	var stored string
	if err := st.db.QueryRowContext(
		ctx,
		`SELECT registration_config_json FROM runs WHERE id=?`,
		run.ID,
	).Scan(&stored); err != nil {
		t.Fatalf("read raw registration config: %v", err)
	}

	if !strings.HasPrefix(stored, encryptedRegistrationConfigPrefix) {
		t.Fatalf("registration config not encrypted: %q", stored)
	}

	if strings.Contains(stored, "bearer_token") ||
		strings.Contains(stored, "token") {
		t.Fatalf("registration config contains plaintext secret: %q", stored)
	}

	// Verify normal application reads still get the original JSON.
	got, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	if string(got.RegistrationConfigJSON) != string(run.RegistrationConfigJSON) {
		t.Fatalf(
			"registration_config_json=%s want %s",
			got.RegistrationConfigJSON,
			run.RegistrationConfigJSON,
		)
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

func TestFailAbandonedPlanningRunsFreesOnlyTasklessPlanningRuns(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	runs := []Run{
		{ID: "run-abandoned", JobID: "job-a", DatasetKey: "dataset-a", Status: "PLANNING", CorrelationID: "c1", StartedAt: nowUTC()},
		{ID: "run-queued", JobID: "job-b", DatasetKey: "dataset-b", Status: "PLANNING", CorrelationID: "c2", StartedAt: nowUTC()},
		{ID: "run-running", JobID: "job-c", DatasetKey: "dataset-c", Status: "RUNNING", CorrelationID: "c3", StartedAt: nowUTC()},
	}
	for _, r := range runs {
		if err := st.CreateRun(ctx, r); err != nil {
			t.Fatalf("create run %s: %v", r.ID, err)
		}
	}
	for _, runID := range []string{"run-queued", "run-running"} {
		if err := st.InsertTasks(ctx, []TaskInsert{{ID: "task-" + runID, RunID: runID, TaskIndex: 1, PartitionSpec: []byte(`{}`), Status: "PENDING"}}); err != nil {
			t.Fatal(err)
		}
	}

	failed, err := st.FailAbandonedPlanningRuns(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || failed[0] != "run-abandoned" {
		t.Fatalf("failed=%v, want only run-abandoned", failed)
	}
	for id, want := range map[string]string{"run-abandoned": "FAILED", "run-queued": "PLANNING", "run-running": "RUNNING"} {
		got, err := st.GetRun(ctx, id)
		if err != nil || got.Status != want {
			t.Fatalf("run %s status=%q err=%v, want %s", id, got.Status, err, want)
		}
	}
	events, err := st.ListEventsForRun(ctx, "run-abandoned", 10)
	if err != nil || len(events) != 1 || !strings.Contains(string(events[0].FieldsJSON), "RUN_PLANNING_ABANDONED") {
		t.Fatalf("abandoned run must record an event: events=%+v err=%v", events, err)
	}
	// The dataset is free for a new run again.
	if err := st.CreateRun(ctx, Run{ID: "run-retry", JobID: "job-a", DatasetKey: "dataset-a", Status: "PLANNING", CorrelationID: "c4", StartedAt: nowUTC()}); err != nil {
		t.Fatalf("dataset must be free after recovery: %v", err)
	}
}

func TestCommitClaimIsExclusiveAndExpires(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if err := st.CreateRun(ctx, Run{ID: "run-claim", JobID: "job", Status: "RUNNING", CorrelationID: "c", StartedAt: nowUTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`UPDATE runs SET status='COMMITTING', commit_reconciliation_status='PENDING' WHERE id='run-claim'`); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if ok, err := st.ClaimCommittingRun(ctx, "run-claim", "a", now, time.Minute); err != nil || !ok {
		t.Fatalf("first claim ok=%v err=%v", ok, err)
	}
	if ok, _ := st.ClaimCommittingRun(ctx, "run-claim", "b", now, time.Minute); ok {
		t.Fatal("a live claim must be exclusive")
	}
	if err := st.RenewCommitClaim(ctx, "run-claim", "b", now, time.Minute); !errors.Is(err, ErrCommitClaimLost) {
		t.Fatalf("non-holder renewal err=%v", err)
	}
	// A crashed holder stops renewing; its claim can be taken after expiry.
	later := now.Add(2 * time.Minute)
	if err := st.RenewCommitClaim(ctx, "run-claim", "a", later, time.Minute); !errors.Is(err, ErrCommitClaimLost) {
		t.Fatalf("expired claim renewal err=%v", err)
	}
	if ok, err := st.ClaimCommittingRun(ctx, "run-claim", "b", later, time.Minute); err != nil || !ok {
		t.Fatalf("takeover after expiry ok=%v err=%v", ok, err)
	}
	// Releasing with a stale token must not drop the new holder's claim.
	if err := st.ReleaseCommitClaim(ctx, "run-claim", "a"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.ClaimCommittingRun(ctx, "run-claim", "c", later, time.Minute); ok {
		t.Fatal("stale release must not free another holder's claim")
	}
}
