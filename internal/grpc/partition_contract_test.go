package grpcapi

import (
	"context"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergpart"
)

// TestAssignmentCarriesRunPartitionSpec checks that workers receive the
// partition spec of the run's registration snapshot, which is the spec the
// master registers the run's files with.
func TestAssignmentCarriesRunPartitionSpec(t *testing.T) {
	ctx := context.Background()
	st := openGRPCTestStore(t)
	seal := func(id, v string) []byte {
		b, err := crypto.Encrypt(testCryptoKey, []byte(v), []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: "src", Name: "src", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: seal("src", `{"dsn":"postgres://src"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: "tgt", Name: "tgt", Kind: "target", Engine: "s3", MetadataJSON: []byte(`{"endpoint":"http://minio:9000","bucket":"b","prefix":"p"}`), SecretEncBlob: seal("tgt", `{"access_key_id":"k","secret_access_key":"s"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateJob(ctx, db.Job{ID: "job", Name: "job", SourceConnectionID: "src", TargetConnectionID: "tgt", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: []byte(`{"table":"orders"}`)}); err != nil {
		t.Fatal(err)
	}
	run := db.Run{
		ID: "run", JobID: "job", DatasetKey: "d", Status: "PLANNING", CorrelationID: "c", StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
		RegistrationConfigJSON: []byte(`{"enabled":true,"engine":"rest-go","table":"ns.orders","partition_spec":[{"source":"id","transform":"bucket[8]"},{"source":"created_at","name":"created_day","transform":"day"}]}`),
	}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartRunWithTasks(ctx, run, []db.TaskInsert{{ID: "task", RunID: "run", TaskIndex: 1, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"}}); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, nil)
	resp, err := srv.RequestTask(ctx, &grpcpb.RequestTaskRequest{WorkerId: "worker-1", ProtocolVersion: WorkerProtocolVersion})
	if err != nil || resp.Task.TaskId != "task" {
		t.Fatalf("request task: %+v %v", resp, err)
	}
	spec, err := icebergpart.ParseSpecJSON(resp.Task.IcebergPartitionSpecJson)
	if err != nil {
		t.Fatal(err)
	}
	want := icebergpart.Spec{{Source: "id", Transform: "bucket[8]"}, {Source: "created_at", Transform: "day"}}
	if !spec.Equal(want) {
		t.Fatalf("assignment spec=%v want %v", spec, want)
	}
}
