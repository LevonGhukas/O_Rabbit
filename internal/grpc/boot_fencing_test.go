package grpcapi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerBootIDRegistrationAndInstanceTracking(t *testing.T) {
	st := openGRPCTestStore(t)
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, nil)
	ctx := context.Background()

	// Register worker with boot ID 1
	reg1, err := srv.RegisterWorker(ctx, &grpcpb.RegisterWorkerRequest{
		WorkerId: "worker-prod-1",
		BootId:   "boot-111",
		Hostname: "node-1.internal",
		Pid:      1001,
		Version:  "1.0.0",
	})
	if err != nil || reg1.WorkerId != "worker-prod-1" {
		t.Fatalf("reg1 failed: %v", err)
	}

	// Register same worker ID with new boot ID 2 (simulating restart)
	reg2, err := srv.RegisterWorker(ctx, &grpcpb.RegisterWorkerRequest{
		WorkerId: "worker-prod-1",
		BootId:   "boot-222",
		Hostname: "node-1.internal",
		Pid:      1002,
		Version:  "1.0.0",
	})
	if err != nil || reg2.WorkerId != "worker-prod-1" {
		t.Fatalf("reg2 failed: %v", err)
	}

	workers, err := st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers: %v", err)
	}
	if len(workers) == 0 {
		t.Fatalf("expected registered worker")
	}

	var found1, found2 bool
	for _, w := range workers {
		if w.ID == "worker-prod-1" {
			for _, inst := range w.Instances {
				if inst.BootID == "boot-111" {
					found1 = true
				}
				if inst.BootID == "boot-222" {
					found2 = true
				}
			}
		}
	}
	if !found1 || !found2 {
		t.Fatalf("expected boot-111 and boot-222 in worker instances, found1=%v found2=%v", found1, found2)
	}
}

func TestWorkerBootIDFencingRejectsStaleInstanceRenewals(t *testing.T) {
	st := openGRPCTestStore(t)
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, nil)
	ctx := context.Background()

	// Create source and target connections
	srcSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"dsn":"postgres://u:p@db:5432/app"}`), []byte("src-fence"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{
		ID:            "src-fence",
		Name:          "src-fence",
		Kind:          "source",
		Engine:        "postgres",
		MetadataJSON:  json.RawMessage(`{}`),
		SecretEncBlob: srcSecret,
	}); err != nil {
		t.Fatal(err)
	}

	tgtSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"access_key_id":"minio","secret_access_key":"secret"}`), []byte("tgt-fence"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{
		ID:            "tgt-fence",
		Name:          "tgt-fence",
		Kind:          "target",
		Engine:        "s3",
		MetadataJSON:  json.RawMessage(`{"endpoint":"http://minio:9000","bucket":"b","prefix":"exp"}`),
		SecretEncBlob: tgtSecret,
	}); err != nil {
		t.Fatal(err)
	}

	// Create job, run, task
	jobID, runID, taskID := "job-fence", "run-fence", "task-fence"
	if err := st.CreateJob(ctx, db.Job{
		ID:                 jobID,
		Name:               "job",
		SourceConnectionID: "src-fence",
		TargetConnectionID: "tgt-fence",
		OptionsJSON:        json.RawMessage(`{"max_in_flight_tasks":0}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, db.Run{
		ID:        runID,
		JobID:     jobID,
		Status:    "RUNNING",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertTasks(ctx, []db.TaskInsert{{
		ID:            taskID,
		RunID:         runID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        "PENDING",
	}}); err != nil {
		t.Fatal(err)
	}

	// Request task with boot-1
	resp, err := srv.RequestTask(ctx, &grpcpb.RequestTaskRequest{
		WorkerId:        "worker-fenced",
		BootId:          "boot-old",
		ProtocolVersion: 5,
	})
	if err != nil {
		t.Fatalf("request task failed: %v", err)
	}
	assignment := resp.Task
	if assignment == nil || assignment.TaskId != taskID {
		t.Fatalf("expected assignment for %s, got %+v", taskID, assignment)
	}

	// Stale worker with invalid fencing token or wrong attempt is rejected
	_, err = srv.RenewTaskLease(ctx, &grpcpb.RenewTaskLeaseRequest{
		WorkerId:     "worker-fenced",
		BootId:       "boot-old",
		TaskId:       taskID,
		AttemptId:    assignment.AttemptId,
		FencingToken: "wrong-token",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for invalid token, got %v", err)
	}
}
