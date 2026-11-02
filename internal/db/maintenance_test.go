package db

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func createMaintenanceJob(t *testing.T, st *Store, id string) {
	t.Helper()
	createRunConnections(t, st, id+"-src", id+"-dst")
	if err := st.CreateJob(context.Background(), Job{ID: id, Name: id, SourceConnectionID: id + "-src", TargetConnectionID: id + "-dst", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceLeaseFencesAStaleHolder(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	createMaintenanceJob(t, st, "job")
	if err := st.CreateMaintenanceOperation(ctx, MaintenanceOperation{ID: "op", Kind: MaintenanceOrphanCleanup, JobID: "job", DryRun: false}, now); err != nil {
		t.Fatal(err)
	}

	first, ok, err := st.ClaimMaintenanceOperation(ctx, now, time.Minute)
	if err != nil || !ok || first.Status != MaintenanceRunning || first.LeaseToken == "" {
		t.Fatalf("claim=%+v ok=%v err=%v", first, ok, err)
	}
	if _, ok, err := st.ClaimMaintenanceOperation(ctx, now.Add(30*time.Second), time.Minute); err != nil || ok {
		t.Fatalf("a live lease must not be claimed twice ok=%v err=%v", ok, err)
	}
	if err := st.RenewMaintenanceLease(ctx, "op", "wrong-token", now, time.Minute); !errors.Is(err, ErrMaintenanceFenced) {
		t.Fatalf("renew with a wrong token err=%v", err)
	}

	// The first holder stalls past its lease; a new claim takes over.
	second, ok, err := st.ClaimMaintenanceOperation(ctx, now.Add(2*time.Minute), time.Minute)
	if err != nil || !ok || second.LeaseToken == first.LeaseToken || second.AttemptCount != 2 {
		t.Fatalf("reclaim=%+v ok=%v err=%v", second, ok, err)
	}
	later := now.Add(2*time.Minute + time.Second)
	for name, err := range map[string]error{
		"renew":     st.RenewMaintenanceLease(ctx, "op", first.LeaseToken, later, time.Minute),
		"plan":      st.RecordMaintenancePlan(ctx, "op", first.LeaseToken, MaintenanceTable{}, nil, map[string]any{}, later),
		"candidate": st.SetMaintenanceCandidateStatus(ctx, "op", first.LeaseToken, "k", "DELETED", later),
		"run":       st.SetMaintenanceRunID(ctx, "op", first.LeaseToken, "run", later),
		"finish":    st.FinishMaintenanceOperation(ctx, "op", first.LeaseToken, MaintenanceSucceeded, "", nil, "", "", later),
	} {
		if !errors.Is(err, ErrMaintenanceFenced) {
			t.Fatalf("stale holder %s err=%v, want ErrMaintenanceFenced", name, err)
		}
	}

	candidates := []MaintenanceCandidate{{ObjectKey: "p/_runs/run-1/part-000001-000.parquet", SizeBytes: 10, Decision: "ORPHAN", Reason: "UNREFERENCED_REGISTERED_RUN", Evidence: json.RawMessage(`{"referenced":false}`), Status: "WOULD_DELETE"}}
	if err := st.RecordMaintenancePlan(ctx, "op", second.LeaseToken, MaintenanceTable{Identifier: "ns.t", SnapshotID: "7"}, candidates, map[string]any{"orphan_files": 1}, later); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMaintenanceCandidateStatus(ctx, "op", second.LeaseToken, candidates[0].ObjectKey, "DELETED", later); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishMaintenanceOperation(ctx, "op", second.LeaseToken, MaintenanceSucceeded, "", map[string]any{"deleted_files": 1}, "", "", later); err != nil {
		t.Fatal(err)
	}
	op, err := st.GetMaintenanceOperation(ctx, "op")
	if err != nil || op.Status != MaintenanceSucceeded || op.SnapshotID != "7" || op.FinishedAt == "" {
		t.Fatalf("op=%+v err=%v", op, err)
	}
	got, err := st.ListMaintenanceCandidates(ctx, "op")
	if err != nil || len(got) != 1 || got[0].Status != "DELETED" {
		t.Fatalf("candidates=%+v err=%v", got, err)
	}
}

func TestMaintenanceAllowsOneActiveOperationPerJob(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Now()
	createMaintenanceJob(t, st, "job")
	if err := st.CreateMaintenanceOperation(ctx, MaintenanceOperation{ID: "a", Kind: MaintenanceCompaction, JobID: "job"}, now); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMaintenanceOperation(ctx, MaintenanceOperation{ID: "b", Kind: MaintenanceOrphanCleanup, JobID: "job"}, now); !errors.Is(err, ErrMaintenanceBusy) {
		t.Fatalf("second active operation err=%v", err)
	}
	claimed, _, err := st.ClaimMaintenanceOperation(ctx, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.FinishMaintenanceOperation(ctx, "a", claimed.LeaseToken, MaintenanceSucceeded, "", nil, "", "", now); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMaintenanceOperation(ctx, MaintenanceOperation{ID: "b", Kind: MaintenanceOrphanCleanup, JobID: "job"}, now); err != nil {
		t.Fatalf("a finished operation must free the job: %v", err)
	}
}

func TestSubmittedCompactionFollowsItsRun(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Now()
	for _, id := range []string{"ok", "bad"} {
		createMaintenanceJob(t, st, "job-"+id)
		if err := st.CreateRun(ctx, Run{ID: "run-" + id, JobID: "job-" + id, DatasetKey: "ds-" + id, Status: "PLANNING", CorrelationID: "c", StartedAt: now.Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
		if err := st.CreateMaintenanceOperation(ctx, MaintenanceOperation{ID: "op-" + id, Kind: MaintenanceCompaction, JobID: "job-" + id}, now); err != nil {
			t.Fatal(err)
		}
		op, _, err := st.ClaimMaintenanceOperation(ctx, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.SetMaintenanceRunID(ctx, op.ID, op.LeaseToken, "run-"+id, now); err != nil {
			t.Fatal(err)
		}
		if err := st.RecordMaintenancePlan(ctx, op.ID, op.LeaseToken, MaintenanceTable{}, []MaintenanceCandidate{{ObjectKey: "k-" + id, Decision: "COMPACT", Reason: "SMALL_FILE", Status: "PLANNED"}}, map[string]any{}, now); err != nil {
			t.Fatal(err)
		}
		if err := st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, MaintenanceSubmitted, "run-"+id, nil, "", "", now); err != nil {
			t.Fatal(err)
		}
		if found, ok, err := st.GetMaintenanceOperationForRun(ctx, "run-"+id); err != nil || !ok || found.ID != op.ID || found.Status != MaintenanceSubmitted {
			t.Fatalf("op for run=%+v ok=%v err=%v", found, ok, err)
		}
	}

	if err := st.CompleteCompactionOperation(ctx, "run-ok", MaintenanceSucceeded, "", "", now); err != nil {
		t.Fatal(err)
	}
	msg := "task failed"
	if err := st.UpdateRunStatus(ctx, "run-bad", "FAILED", true, &msg); err != nil {
		t.Fatal(err)
	}
	if n, err := st.FailSubmittedCompactions(ctx, now); err != nil || n != 1 {
		t.Fatalf("failed submitted=%d err=%v", n, err)
	}
	ok, _ := st.GetMaintenanceOperation(ctx, "op-ok")
	bad, _ := st.GetMaintenanceOperation(ctx, "op-bad")
	if ok.Status != MaintenanceSucceeded || bad.Status != MaintenanceFailed || bad.ErrorClass != "COMPACTION_RUN_FAILED" {
		t.Fatalf("ok=%+v bad=%+v", ok, bad)
	}
	cands, _ := st.ListMaintenanceCandidates(ctx, "op-ok")
	if len(cands) != 1 || cands[0].Status != "COMPACTED" {
		t.Fatalf("candidates=%+v", cands)
	}
	if _, ok, err := st.GetMaintenanceOperationForRun(ctx, "run-unrelated"); err != nil || ok {
		t.Fatalf("unrelated run ok=%v err=%v", ok, err)
	}
}
