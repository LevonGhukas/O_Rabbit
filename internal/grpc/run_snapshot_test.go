package grpcapi

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// TestRunUsesPlannedConfigurationAfterEdits edits the job and its target
// connection while a run is in flight; the run must keep writing, and scope
// its credentials, to the destination it was planned with.
func TestRunUsesPlannedConfigurationAfterEdits(t *testing.T) {
	ctx := context.Background()
	st := openGRPCTestStore(t)
	seal := func(id, v string) []byte {
		b, err := crypto.Encrypt(testCryptoKey, []byte(v), []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	planned := `{"endpoint":"http://minio:9000","bucket":"planned-bucket","prefix":"planned/prefix","credential_mode":"sts","sts_role_arn":"arn:aws:iam::1:role/w"}`
	if err := st.CreateConnection(ctx, db.Connection{ID: "src", Name: "src", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: seal("src", `{"dsn":"postgres://src"}`)}); err != nil {
		t.Fatal(err)
	}
	tgt := db.Connection{ID: "tgt", Name: "tgt", Kind: "target", Engine: "s3", MetadataJSON: []byte(planned), SecretEncBlob: seal("tgt", `{"access_key_id":"k","secret_access_key":"s"}`)}
	if err := st.CreateConnection(ctx, tgt); err != nil {
		t.Fatal(err)
	}
	job := db.Job{ID: "job", Name: "job", SourceConnectionID: "src", TargetConnectionID: "tgt", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: []byte(`{"table":"orders","target_file_bytes":1048576}`)}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	run := db.Run{ID: "run", JobID: "job", DatasetKey: "d", Status: "PLANNING", CorrelationID: "c", StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartRunWithTasks(ctx, run, []db.TaskInsert{{ID: "task", RunID: "run", TaskIndex: 1, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"}}); err != nil {
		t.Fatal(err)
	}

	// Edit the job and the target connection mid-run.
	job.SourceSQL, job.OptionsJSON = "select 2", []byte(`{"table":"orders","target_file_bytes":999}`)
	if err := st.UpdateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	edited := tgt
	edited.MetadataJSON = []byte(`{"endpoint":"http://other:9000","bucket":"other-bucket","prefix":"other/prefix"}`)
	if _, err := st.UpdateConnectionAudited(ctx, tgt, edited, db.AuditRecord{Action: "test", ResourceType: "test", ResourceID: "test"}); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, nil)
	var policy string
	srv.assumeRoleFn = func(_ context.Context, req stsRequest) (aws.Credentials, error) {
		policy = req.Policy
		return aws.Credentials{AccessKeyID: "tmp", SecretAccessKey: "tmp", SessionToken: "tok"}, nil
	}
	resp, err := srv.RequestTask(ctx, &grpcpb.RequestTaskRequest{WorkerId: "worker-1", ProtocolVersion: WorkerProtocolVersion})
	if err != nil || resp.Task.TaskId != "task" {
		t.Fatalf("request task: %+v %v", resp, err)
	}
	a := resp.Task
	if a.S3Bucket != "planned-bucket" || a.S3Endpoint != "http://minio:9000" || a.S3Prefix != "planned/prefix" || a.SourceSql != "select 1" || a.TargetFileBytes != 1048576 {
		t.Fatalf("assignment must use the planned configuration: bucket=%s endpoint=%s prefix=%s sql=%q file_bytes=%d", a.S3Bucket, a.S3Endpoint, a.S3Prefix, a.SourceSql, a.TargetFileBytes)
	}
	if _, err := srv.GetTaskCredentials(ctx, &grpcpb.GetTaskCredentialsRequest{WorkerId: "worker-1", TaskId: a.TaskId, AttemptId: a.AttemptId, FencingToken: a.FencingToken}); err != nil {
		t.Fatal(err)
	}
	var p struct{ Statement []struct{ Resource []string } }
	if err := json.Unmarshal([]byte(policy), &p); err != nil || !strings.Contains(p.Statement[0].Resource[0], "planned-bucket/planned/prefix/_runs/run-run/") {
		t.Fatalf("STS scope must use the planned destination: %s", policy)
	}
}
