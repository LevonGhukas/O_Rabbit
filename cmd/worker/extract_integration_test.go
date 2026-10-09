package main

// Worker extraction against a real PostgreSQL source: rows → Arrow → local
// Parquet files. Runs only when ORABBIT_IT_POSTGRES_DSN is set.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/workerworkspace"
)

// progressSink accepts progress reports; every other RPC is unused here.
type progressSink struct {
	grpcpb.ControlPlaneClient
	reports int
}

func (p *progressSink) ReportTaskProgress(context.Context, *grpcpb.ReportTaskProgressRequest, ...grpc.CallOption) (*grpcpb.ReportTaskProgressResponse, error) {
	p.reports++
	return &grpcpb.ReportTaskProgressResponse{}, nil
}

func TestExtractSQLCursorTaskIntegration(t *testing.T) {
	dsn := os.Getenv("ORABBIT_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set ORABBIT_IT_POSTGRES_DSN to extract from PostgreSQL")
	}
	ctx := context.Background()
	pg, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pg.Close()
	for _, s := range []string{
		"DROP TABLE IF EXISTS orabbit_it_extract",
		"CREATE TABLE orabbit_it_extract (id BIGINT NOT NULL PRIMARY KEY, name TEXT, amount NUMERIC(10,2), created TIMESTAMP)",
		"INSERT INTO orabbit_it_extract SELECT g, 'n' || g, g * 1.5, TIMESTAMP '2026-01-01' + g * INTERVAL '1 minute' FROM generate_series(1, 500) g",
	} {
		if _, err := pg.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	t.Cleanup(func() { _, _ = pg.ExecContext(context.Background(), "DROP TABLE IF EXISTS orabbit_it_extract") })

	clients := &clientCache{}
	defer clients.Close()
	sink := &progressSink{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	task := &grpcpb.TaskAssignment{TaskId: "task-1", RunId: "run-1", TaskIndex: 1}
	spec := normalizePartitionSpec(partitionSpec{
		Type: "sql_cursor_range", Table: "public.orabbit_it_extract", CursorColumn: "id", CursorDomain: "int64",
		Lower: "100", LowerExclusive: true, Upper: "300", UpperInclusive: true, WhereClause: "amount > 200",
	})
	taskCtx := withTaskCredentials(ctx, taskCredentials{SourceDSN: dsn})

	res, err := extractSQLCursorTask(taskCtx, log, sink, "worker-1", task, spec, clients, "postgres")
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	// (100, 300] with amount = 1.5*id > 200 → id 134..300.
	if res.Rows != 167 || res.MaxCursor != "300" {
		t.Fatalf("rows=%d max=%s, want 167 rows up to 300", res.Rows, res.MaxCursor)
	}
	if len(res.ParquetFiles) == 0 {
		t.Fatal("no parquet files written")
	}
	var fileRows int64
	for _, f := range res.ParquetFiles {
		info, err := os.Stat(f.Path)
		if err != nil || info.Size() == 0 || f.SHA256 == "" {
			t.Fatalf("parquet file %+v: stat=%v err=%v", f, info, err)
		}
		fileRows += f.Rows
		t.Cleanup(func() { _ = os.Remove(f.Path) })
	}
	if fileRows != res.Rows {
		t.Fatalf("parquet rows=%d, extracted=%d", fileRows, res.Rows)
	}
}

func TestExtractDocumentTaskIntegration(t *testing.T) {
	dsn := os.Getenv("ORABBIT_IT_MONGODB_DSN")
	if dsn == "" {
		t.Skip("set ORABBIT_IT_MONGODB_DSN to extract from MongoDB")
	}
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dsn))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(context.Background())
	coll := client.Database("orabbit_it").Collection("orabbit_it_extract")
	_ = coll.Drop(ctx)
	docs := make([]any, 0, 200)
	for i := 1; i <= 200; i++ {
		docs = append(docs, bson.M{"seq": int64(i), "name": fmt.Sprintf("n%d", i), "tags": bson.A{"a", "b"}})
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("insert: %v", err)
	}
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	clients := &clientCache{}
	defer clients.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	task := &grpcpb.TaskAssignment{TaskId: "task-1", RunId: "run-1", TaskIndex: 1}
	spec := normalizePartitionSpec(partitionSpec{
		Type: "sql_cursor_range", Table: "orabbit_it_extract", CursorColumn: "seq", CursorDomain: "int64",
		Lower: "50", LowerExclusive: true, Upper: "150", UpperInclusive: true,
	})

	res, err := extractDocumentTask(withTaskCredentials(ctx, taskCredentials{SourceDSN: dsn}), log, &progressSink{}, "worker-1", task, spec, clients, "mongodb")
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if res.Rows != 100 || len(res.ParquetFiles) == 0 {
		t.Fatalf("rows=%d files=%d, want 100 rows in parquet", res.Rows, len(res.ParquetFiles))
	}
	for _, f := range res.ParquetFiles {
		t.Cleanup(func() { _ = os.Remove(f.Path) })
	}
}

// fakeMaster records the worker's calls for one task attempt.
type fakeMaster struct {
	progressSink
	result     *grpcpb.ReportTaskResultRequest
	lifecycles int
}

func (m *fakeMaster) AcquireUploadCapacity(context.Context, *grpcpb.AcquireUploadCapacityRequest, ...grpc.CallOption) (*grpcpb.AcquireUploadCapacityResponse, error) {
	// A real master grants a deadline; without one the worker (correctly)
	// treats the capacity lease as already expired.
	return &grpcpb.AcquireUploadCapacityResponse{Acquired: true, LeaseId: "lease-1", LeaseToken: "tok", LeaseDeadlineUnixMs: time.Now().Add(time.Minute).UnixMilli()}, nil
}

func (m *fakeMaster) ReleaseUploadCapacity(context.Context, *grpcpb.ReleaseUploadCapacityRequest, ...grpc.CallOption) (*grpcpb.ReleaseUploadCapacityResponse, error) {
	return &grpcpb.ReleaseUploadCapacityResponse{}, nil
}

func (m *fakeMaster) ReportMultipartLifecycle(context.Context, *grpcpb.ReportMultipartLifecycleRequest, ...grpc.CallOption) (*grpcpb.ReportMultipartLifecycleResponse, error) {
	m.lifecycles++
	return &grpcpb.ReportMultipartLifecycleResponse{}, nil
}

func (m *fakeMaster) ReportTaskResult(_ context.Context, req *grpcpb.ReportTaskResultRequest, _ ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error) {
	m.result = req
	return &grpcpb.ReportTaskResultResponse{Accepted: true}, nil
}

// A whole task attempt: read Postgres, write Parquet, upload to S3 (MinIO),
// report the verified artifacts to the master.
func TestExecuteTaskBodyUploadsAndReportsIntegration(t *testing.T) {
	dsn, endpoint := os.Getenv("ORABBIT_IT_POSTGRES_DSN"), os.Getenv("ORABBIT_IT_S3_ENDPOINT")
	if dsn == "" || endpoint == "" {
		t.Skip("set ORABBIT_IT_POSTGRES_DSN and ORABBIT_IT_S3_* to run a full task")
	}
	ctx := context.Background()
	pg, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pg.Close()
	for _, s := range []string{
		"DROP TABLE IF EXISTS orabbit_it_task",
		"CREATE TABLE orabbit_it_task (id BIGINT NOT NULL PRIMARY KEY, v TEXT)",
		"INSERT INTO orabbit_it_task SELECT g, 'v' || g FROM generate_series(1, 250) g",
	} {
		if _, err := pg.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	t.Cleanup(func() { _, _ = pg.ExecContext(context.Background(), "DROP TABLE IF EXISTS orabbit_it_task") })

	spec, _ := json.Marshal(map[string]any{"type": "sql_cursor_range", "table": "public.orabbit_it_task", "cursor_column": "id", "cursor_domain": "int64", "lower": "0", "lower_exclusive": true, "upper": "250", "upper_inclusive": true})
	task := &grpcpb.TaskAssignment{
		TaskId: "task-it", RunId: "run-it", AttemptId: "attempt-it", AttemptNumber: 1, FencingToken: "fence", TaskIndex: 1,
		SourceEngine: "postgres", PartitionSpecJson: string(spec),
		S3Endpoint: endpoint, S3Region: "us-east-1", S3Bucket: os.Getenv("ORABBIT_IT_S3_BUCKET"), S3ForcePathStyle: true,
		S3Prefix: "worker-it/" + fmt.Sprint(time.Now().UnixNano()),
	}
	s3creds := awscreds.NewStaticCredentialsProvider(os.Getenv("ORABBIT_IT_S3_ACCESS_KEY_ID"), os.Getenv("ORABBIT_IT_S3_SECRET_ACCESS_KEY"), "")
	taskCtx := withTaskCredentials(ctx, taskCredentials{SourceDSN: dsn, S3: s3creds})
	master := &fakeMaster{}
	clients := &clientCache{}
	defer clients.Close()

	if err := executeTaskBody(taskCtx, slog.New(slog.NewTextHandler(io.Discard, nil)), master, "worker-1", task, clients); err != nil {
		t.Fatalf("execute task: %v", err)
	}
	r := master.result
	if r == nil || r.Status != "SUCCEEDED" || r.RowsRead != 250 {
		t.Fatalf("result=%+v, want SUCCEEDED with 250 rows", r)
	}
	if len(r.Artifacts) == 0 || r.Artifacts[0].Sha256 == "" || r.Artifacts[0].RowCount != 250 {
		t.Fatalf("artifacts=%+v, want one verified artifact with 250 rows", r.Artifacts)
	}
}

func (m *fakeMaster) GetTaskCredentials(context.Context, *grpcpb.GetTaskCredentialsRequest, ...grpc.CallOption) (*grpcpb.GetTaskCredentialsResponse, error) {
	return &grpcpb.GetTaskCredentialsResponse{
		SourceDsn:         os.Getenv("ORABBIT_IT_POSTGRES_DSN"),
		S3AccessKeyId:     os.Getenv("ORABBIT_IT_S3_ACCESS_KEY_ID"),
		S3SecretAccessKey: os.Getenv("ORABBIT_IT_S3_SECRET_ACCESS_KEY"),
	}, nil
}

func (m *fakeMaster) RenewTaskLease(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
	return &grpcpb.RenewTaskLeaseResponse{LeaseDeadlineUnixMs: time.Now().Add(time.Minute).UnixMilli()}, nil
}

// The managed path a real worker runs: workspace, lease, credentials from
// the master, then the task body; the workspace is cleaned up afterwards.
func TestExecuteTaskManagedIntegration(t *testing.T) {
	dsn, endpoint := os.Getenv("ORABBIT_IT_POSTGRES_DSN"), os.Getenv("ORABBIT_IT_S3_ENDPOINT")
	if dsn == "" || endpoint == "" {
		t.Skip("set ORABBIT_IT_POSTGRES_DSN and ORABBIT_IT_S3_* to run a managed task")
	}
	ctx := context.Background()
	pg, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pg.Close()
	for _, s := range []string{
		"DROP TABLE IF EXISTS orabbit_it_managed",
		"CREATE TABLE orabbit_it_managed (id BIGINT NOT NULL PRIMARY KEY)",
		"INSERT INTO orabbit_it_managed SELECT g FROM generate_series(1, 40) g",
	} {
		if _, err := pg.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	t.Cleanup(func() { _, _ = pg.ExecContext(context.Background(), "DROP TABLE IF EXISTS orabbit_it_managed") })

	root := t.TempDir()
	manager, err := workerworkspace.Open(workerworkspace.Config{Root: root})
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	spec, _ := json.Marshal(map[string]any{"type": "sql_cursor_single", "table": "public.orabbit_it_managed", "cursor_column": "id", "cursor_domain": "int64"})
	task := &grpcpb.TaskAssignment{
		TaskId: "task-m", RunId: "run-m", AttemptId: "attempt-m", AttemptNumber: 1, FencingToken: "fence", TaskIndex: 1,
		LeaseDeadlineUnixMs: time.Now().Add(time.Minute).UnixMilli(),
		SourceEngine:        "postgres", PartitionSpecJson: string(spec),
		S3Endpoint: endpoint, S3Region: "us-east-1", S3Bucket: os.Getenv("ORABBIT_IT_S3_BUCKET"), S3ForcePathStyle: true,
		S3Prefix: "worker-it/managed-" + fmt.Sprint(time.Now().UnixNano()),
	}
	master := &fakeMaster{}
	clients := &clientCache{}
	defer clients.Close()

	if err := executeTaskManaged(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), master, "worker-1", "instance-1", task, clients, manager); err != nil {
		t.Fatalf("managed task: %v", err)
	}
	if master.result == nil || master.result.Status != "SUCCEEDED" || master.result.RowsRead != 40 {
		t.Fatalf("result=%+v, want SUCCEEDED with 40 rows", master.result)
	}
}
