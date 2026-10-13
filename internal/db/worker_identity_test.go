package db

import (
	"context"
	"errors"
	"testing"
	"time"
)

var testAudit = AuditRecord{Action: "test", ResourceType: "test", ResourceID: "test"}

func TestEnrollmentTokenIsBoundedByUsesAndExpiry(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	now := time.Now().UTC()
	if _, err := st.CreateEnrollmentTokenAudited(ctx, "tok-1", "digest-1", "gpu", 2, now.Add(time.Hour), testAudit); err != nil {
		t.Fatal(err)
	}

	first, err := st.EnrollWorkerIdentity(ctx, "digest-1", "id-1", "w1", "h1", "s1", now.Add(time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	if first.Pool != "gpu" || first.Status != WorkerIdentityActive || first.EnrollmentTokenID != "tok-1" {
		t.Fatalf("identity=%+v", first)
	}
	if _, err := st.EnrollWorkerIdentity(ctx, "digest-1", "id-2", "w2", "h2", "s2", now.Add(time.Hour), now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnrollWorkerIdentity(ctx, "digest-1", "id-3", "w3", "h3", "s3", now.Add(time.Hour), now); !errors.Is(err, ErrEnrollmentTokenInvalid) {
		t.Fatalf("exhausted token err=%v", err)
	}
	if _, err := st.EnrollWorkerIdentity(ctx, "unknown", "id-4", "w", "h", "s", now.Add(time.Hour), now); !errors.Is(err, ErrEnrollmentTokenInvalid) {
		t.Fatalf("unknown token err=%v", err)
	}

	if _, err := st.CreateEnrollmentTokenAudited(ctx, "tok-2", "digest-2", "default", 1, now.Add(time.Minute), testAudit); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnrollWorkerIdentity(ctx, "digest-2", "id-5", "w", "h", "s", now.Add(time.Hour), now.Add(2*time.Minute)); !errors.Is(err, ErrEnrollmentTokenInvalid) {
		t.Fatalf("expired token err=%v", err)
	}
	if _, err := st.GetWorkerIdentity(ctx, "id-5"); err == nil {
		t.Fatal("rejected enrollment must not create an identity")
	}
}

func TestRevokedIdentityCannotRenew(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	now := time.Now().UTC()
	if _, err := st.CreateEnrollmentTokenAudited(ctx, "tok", "digest", "default", 1, now.Add(time.Hour), testAudit); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnrollWorkerIdentity(ctx, "digest", "id", "w", "h", "s1", now.Add(time.Hour), now); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordWorkerCertificate(ctx, "id", "s2", now.Add(2*time.Hour), now); err != nil {
		t.Fatal(err)
	}
	revoked, err := st.RevokeWorkerIdentityAudited(ctx, "id", now, testAudit)
	if err != nil || revoked.Status != WorkerIdentityRevoked || revoked.RevokedAt == nil {
		t.Fatalf("revoke=%+v err=%v", revoked, err)
	}
	if err := st.RecordWorkerCertificate(ctx, "id", "s3", now.Add(3*time.Hour), now); err == nil {
		t.Fatal("revoked identity must not receive a renewed certificate")
	}
	records, err := st.ListAuditRecords(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("token creation and revocation must be audited, got %d records", len(records))
	}
}

func TestPoolFilterLimitsAssignment(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	for _, c := range []Connection{{ID: "src", Name: "src", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: []byte{1}}, {ID: "tgt", Name: "tgt", Kind: "target", Engine: "s3", MetadataJSON: []byte(`{}`), SecretEncBlob: []byte{1}}} {
		if err := st.CreateConnection(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.CreateJob(ctx, Job{ID: "job", Name: "job", SourceConnectionID: "src", TargetConnectionID: "tgt", TargetNamespace: "ns", TargetTable: "t", WriteMode: "append", OptionsJSON: []byte(`{"worker_pool":"gpu"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, Run{ID: "run", JobID: "job", Status: "RUNNING", CorrelationID: "c", StartedAt: nowUTC()}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{{ID: "task", RunID: "run", TaskIndex: 1, PartitionSpec: []byte(`{}`), Status: "PENDING"}}); err != nil {
		t.Fatal(err)
	}
	policy := LeasePolicy{Duration: time.Minute, MaxAttempts: 3}
	if _, ok, err := st.AssignNextPendingTaskInPool(ctx, "", "w-default", DefaultWorkerPool, time.Now(), policy, nil, nil); err != nil || ok {
		t.Fatalf("default-pool worker must not receive gpu task ok=%v err=%v", ok, err)
	}
	task, ok, err := st.AssignNextPendingTaskInPool(ctx, "", "w-gpu", "gpu", time.Now(), policy, nil, nil)
	if err != nil || !ok || task.ID != "task" {
		t.Fatalf("gpu worker assignment task=%+v ok=%v err=%v", task, ok, err)
	}
	if _, err := st.VerifyTaskAttemptOwner(ctx, "", "task", task.AttemptID, task.FencingToken, "w-default", time.Now()); !IsAttemptFenced(err) {
		t.Fatalf("non-owner must be fenced, err=%v", err)
	}
	if runID, err := st.VerifyTaskAttemptOwner(ctx, "", "task", task.AttemptID, task.FencingToken, "w-gpu", time.Now()); err != nil || runID != "run" {
		t.Fatalf("owner verification run=%q err=%v", runID, err)
	}
}
