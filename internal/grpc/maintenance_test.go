package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergpart"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// bucketFS serves s3://<bucket>/<key> from <root>/<key> on the local disk,
// so a table can reference worker files by their real object paths.
type bucketFS struct{ root, bucket string }

func (b bucketFS) path(name string) string {
	if key, ok := icebergreg.ObjectKey(name, b.bucket); ok {
		return filepath.Join(b.root, key)
	}
	return name
}

func (b bucketFS) Open(name string) (iceio.File, error) { return iceio.LocalFS{}.Open(b.path(name)) }
func (b bucketFS) Remove(name string) error             { return iceio.LocalFS{}.Remove(b.path(name)) }
func (b bucketFS) Create(name string) (iceio.FileWriter, error) {
	p := b.path(name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	return iceio.LocalFS{}.Create(p)
}
func (b bucketFS) WriteFile(name string, content []byte) error {
	p := b.path(name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return iceio.LocalFS{}.WriteFile(p, content)
}

// memCatalog applies commits to in-memory metadata, checking requirements
// the way a REST catalog does.
type memCatalog struct {
	metadata icetable.Metadata
	location string
}

func (c *memCatalog) LoadTable(context.Context, icetable.Identifier) (*icetable.Table, error) {
	return nil, errors.New("not used")
}

func (c *memCatalog) CommitTable(_ context.Context, _ icetable.Identifier, reqs []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	for _, r := range reqs {
		if err := r.Validate(c.metadata); err != nil {
			return nil, "", err
		}
	}
	b, err := icetable.MetadataBuilderFromBase(c.metadata, c.location)
	if err != nil {
		return nil, "", err
	}
	for _, u := range updates {
		if err := u.Apply(b); err != nil {
			return nil, "", err
		}
	}
	c.metadata, err = b.Build()
	return c.metadata, c.location, err
}

// writeContractFile writes rows of one region with the worker's partition
// contract to <root>/<key> and returns the file's bytes.
func writeContractFile(t *testing.T, root, key string, ids []int64, region string) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "region", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for _, id := range ids {
		b.Field(0).(*array.Int64Builder).Append(id)
		b.Field(1).(*array.StringBuilder).Append(region)
	}
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()
	p, err := icebergpart.NewPartitioner(icebergpart.Spec{{Source: "region"}})
	if err != nil {
		t.Fatal(err)
	}
	groups, err := p.Split(rec)
	if err != nil || len(groups) != 1 {
		t.Fatalf("groups=%d err=%v", len(groups), err)
	}
	footer, err := groups[0].Contract.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	w, tmp, err := parquetio.NewTempFileWriterInDir(schema, parquetio.Options{}, filepath.Dir(dst))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendKeyValueMetadata(icebergpart.FooterKey, footer); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestCompactionRunCommitReplacesFilesAndRecovers drives a compaction run
// through the commit path: artifact verification, one atomic replace, and a
// crash after the catalog commit that the next pass completes without a
// second snapshot. The dataset's export state is never written.
func TestCompactionRunCommitReplacesFilesAndRecovers(t *testing.T) {
	ctx := context.Background()
	st := openGRPCTestStore(t)
	root := t.TempDir()
	const bucket, prefix = "bucket1", "ds"
	fs := bucketFS{root: root, bucket: bucket}

	srcSecret, _ := crypto.Encrypt(testCryptoKey, []byte(`{"dsn":"postgres://src"}`), []byte("src"))
	tgtSecret, _ := crypto.Encrypt(testCryptoKey, []byte(`{"access_key_id":"a","secret_access_key":"b"}`), []byte("tgt"))
	if err := st.CreateConnection(ctx, db.Connection{ID: "src", Name: "src", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: srcSecret}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: "tgt", Name: "tgt", Kind: "target", Engine: "s3", MetadataJSON: mustJSONRaw(t, map[string]any{"endpoint": "http://minio:9000", "region": "us-east-1", "bucket": bucket, "prefix": prefix, "force_path_style": true}), SecretEncBlob: tgtSecret}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateJob(ctx, db.Job{ID: "job", Name: "job", SourceConnectionID: "src", TargetConnectionID: "tgt", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", Incremental: true, OptionsJSON: []byte(`{"table":"public.t"}`)}); err != nil {
		t.Fatal(err)
	}
	datasetKey := dataset.StorageKey("http://minio:9000", bucket, prefix)
	regJSON := `{"enabled":true,"engine":"rest-go","uri":"http://catalog","table":"ns.tbl","partition_spec":[{"source":"region"}]}`
	if err := st.CreateRun(ctx, db.Run{ID: "src-run", JobID: "job", DatasetKey: datasetKey, Status: "SUCCEEDED", CorrelationID: "c", StartedAt: "2026-10-01T00:00:00Z", RegistrationConfigJSON: json.RawMessage(regJSON)}); err != nil {
		t.Fatal(err)
	}

	// The table: three one-row files of region eu, as three small runs wrote.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{}})
	metadata, err := icetable.NewMetadata(schema, &spec, icetable.UnsortedSortOrder, "s3://"+bucket+"/"+prefix, iceberg.Properties{"format-version": "2"})
	if err != nil {
		t.Fatal(err)
	}
	cat := &memCatalog{metadata: metadata, location: "s3://" + bucket + "/" + prefix + "/metadata/v1.json"}
	load := func(context.Context) (*icetable.Table, error) {
		return icetable.New(icetable.Identifier{"ns", "tbl"}, cat.metadata, cat.location, func(context.Context) (iceio.IO, error) { return fs, nil }, cat), nil
	}
	var inputs []string
	for i := 1; i <= 3; i++ {
		key := prefix + "/_runs/run-src-run/part-00000" + string(rune('0'+i)) + "-000.parquet"
		writeContractFile(t, root, key, []int64{int64(i)}, "eu")
		inputs = append(inputs, "s3://"+bucket+"/"+key)
	}
	tbl, _ := load(ctx)
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(ctx, inputs, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tbl, _ = load(ctx)
	before := len(tbl.Metadata().Snapshots())

	// The compaction operation and its run, whose worker wrote one file.
	if err := st.CreateMaintenanceOperation(ctx, db.MaintenanceOperation{ID: "op", Kind: db.MaintenanceCompaction, JobID: "job", DryRun: false}, time.Now()); err != nil {
		t.Fatal(err)
	}
	op, _, err := st.ClaimMaintenanceOperation(ctx, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var candidates []db.MaintenanceCandidate
	for _, in := range inputs {
		key, _ := icebergreg.ObjectKey(in, bucket)
		candidates = append(candidates, db.MaintenanceCandidate{ObjectKey: key, Decision: "COMPACT", Reason: "SMALL_FILE", Evidence: mustJSONRaw(t, map[string]any{"path": in}), Status: "PLANNED"})
	}
	if err := st.RecordMaintenancePlan(ctx, op.ID, op.LeaseToken, db.MaintenanceTable{SourceRunID: "src-run", Identifier: "ns.tbl", UUID: tbl.Metadata().TableUUID().String()}, candidates, map[string]any{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, db.Run{ID: "cmp-run", JobID: "job", DatasetKey: datasetKey, Status: "RUNNING", CorrelationID: "c2", StartedAt: "2026-10-02T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMaintenanceRunID(ctx, op.ID, op.LeaseToken, "cmp-run", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, db.MaintenanceSubmitted, "cmp-run", nil, "", "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertTasks(ctx, []db.TaskInsert{{ID: "cmp-task", RunID: "cmp-run", TaskIndex: 1, PartitionSpec: []byte(`{"type":"iceberg_compact"}`), Status: "PENDING"}}); err != nil {
		t.Fatal(err)
	}
	one := func(v string) func() (string, error) { return func() (string, error) { return v, nil } }
	assigned, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "w", time.Now(), db.LeasePolicy{Duration: time.Hour, MaxAttempts: 3}, one("cmp-attempt"), one("cmp-token"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	outKey := prefix + "/_runs/run-cmp-run/part-000001-000.parquet"
	outBytes := writeContractFile(t, root, outKey, []int64{1, 2, 3}, "eu")
	digest := sha256.Sum256(outBytes)
	record := artifact.Record{ObjectKey: outKey, ByteSize: int64(len(outBytes)), SHA256: hex.EncodeToString(digest[:]), RowCount: 3, SchemaFingerprint: strings.Repeat("a", 64), RunID: "cmp-run", TaskID: "cmp-task", AttemptID: assigned.AttemptID, AttemptNumber: assigned.AttemptNumber, FormatVersion: artifact.FormatVersion, VerificationMethod: artifact.VerificationPortable, VerificationStatus: artifact.VerificationVerified}
	if accepted, _, _, err := st.CompleteTaskAttemptWithArtifactsAt(ctx, "", "cmp-task", assigned.AttemptID, assigned.FencingToken, "w", "SUCCEEDED", nil, []artifact.Record{record}, 3, 0, int64(len(outBytes)), time.Now()); err != nil || !accepted {
		t.Fatalf("complete accepted=%v err=%v", accepted, err)
	}
	if changed, status, err := st.TryFinalizeRun(ctx, "cmp-run"); err != nil || !changed || status != "COMMITTING" {
		t.Fatalf("finalize changed=%v status=%s err=%v", changed, status, err)
	}

	objects := newScriptedCommitStore()
	objects.objects[outKey] = outBytes
	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, nil)
	srv.newCommitObjectStoreFn = func(context.Context, s3io.Config) (commitObjectStore, error) { return objects, nil }
	srv.loadCompactionTableFn = load

	// The master crashes after the catalog commit, before completing the run.
	srv.completeRunCommitFn = func(context.Context, string) error { return errors.New("injected crash") }
	if err := srv.finalizeRunCommit(ctx, "cmp-run"); err == nil {
		t.Fatal("expected the injected crash")
	}
	srv.completeRunCommitFn = st.CompleteRunCommit
	if err := srv.finalizeRunCommit(ctx, "cmp-run"); err != nil {
		t.Fatal(err)
	}

	run, _ := st.GetRun(ctx, "cmp-run")
	if run.Status != "SUCCEEDED" {
		t.Fatalf("run status=%s", run.Status)
	}
	got, _ := st.GetMaintenanceOperation(ctx, "op")
	if got.Status != db.MaintenanceSucceeded {
		t.Fatalf("operation=%+v", got)
	}
	tbl, _ = load(ctx)
	if n := len(tbl.Metadata().Snapshots()); n != before+1 {
		t.Fatalf("snapshots=%d want %d: the retry must not commit twice", n, before+1)
	}
	files, err := icebergreg.CompactionFiles(ctx, tbl, icebergreg.BucketKeys(bucket))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Key != outKey || files[0].Records != 3 {
		t.Fatalf("live files=%+v", files)
	}
	if objects.putCount(prefix+"/_state.json") != 0 || objects.putCount(prefix+"/_commits/run-cmp-run.json") != 0 {
		t.Fatal("a compaction must not publish dataset state or a run manifest")
	}
}

func TestCompactionCredentialsCanReadTheDataset(t *testing.T) {
	policy := s3SessionPolicy("aws", "b", "p/_runs/run-1", true, "p")
	var parsed struct {
		Statement []struct {
			Action   []string
			Resource []string
		}
	}
	if err := json.Unmarshal([]byte(policy), &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed.Statement) != 2 || parsed.Statement[1].Resource[0] != "arn:aws:s3:::b/p/*" || len(parsed.Statement[1].Action) != 1 || parsed.Statement[1].Action[0] != "s3:GetObject" {
		t.Fatalf("policy=%s", policy)
	}
	if plain := s3SessionPolicy("aws", "b", "p/_runs/run-1", false, "p"); strings.Contains(plain, "arn:aws:s3:::b/p/*\"") {
		t.Fatalf("export credentials must stay scoped to the run: %s", plain)
	}
}

func TestMaintenanceRunStateKeepsFilesOfRunsThatMayNeedThem(t *testing.T) {
	ctx := context.Background()
	st := openGRPCTestStore(t)
	createRunConnections := func() {
		for _, c := range []db.Connection{
			{ID: "s", Name: "s", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: []byte{1}},
			{ID: "d", Name: "d", Kind: "target", Engine: "s3", MetadataJSON: []byte(`{"bucket":"b"}`), SecretEncBlob: []byte{1}},
		} {
			if err := st.CreateConnection(ctx, c); err != nil {
				t.Fatal(err)
			}
		}
	}
	createRunConnections()
	if err := st.CreateJob(ctx, db.Job{ID: "j", Name: "j", SourceConnectionID: "s", TargetConnectionID: "d", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "t", WriteMode: "append", OptionsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	for id, status := range map[string]string{"active": "RUNNING", "failed": "FAILED", "canceled": "CANCELED", "plain": "SUCCEEDED"} {
		if err := st.CreateRun(ctx, db.Run{ID: id, JobID: "j", DatasetKey: "ds-" + id, Status: status, CorrelationID: "c", StartedAt: "2026-10-01T00:00:00Z"}); err != nil {
			t.Fatal(err)
		}
	}
	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, nil)
	for id, want := range map[string]icebergreg.RunState{
		"active":   icebergreg.RunStateActive,
		"failed":   icebergreg.RunStateFailed,
		"canceled": icebergreg.RunStateCanceled,
		"plain":    icebergreg.RunStateNotRegistered,
		"missing":  icebergreg.RunStateUnknown,
	} {
		if got := srv.maintenanceRunState(ctx, id); got != want {
			t.Fatalf("%s: state=%s want %s", id, got, want)
		}
	}
}

// A holder that crashed after starting the compaction run must not make the
// next holder plan a second run; the operation follows the first one.
func TestReclaimedCompactionFollowsTheRunItStarted(t *testing.T) {
	ctx := context.Background()
	st := openGRPCTestStore(t)
	for _, c := range []db.Connection{
		{ID: "s", Name: "s", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: []byte{1}},
		{ID: "d", Name: "d", Kind: "target", Engine: "s3", MetadataJSON: []byte(`{"bucket":"b"}`), SecretEncBlob: []byte{1}},
	} {
		if err := st.CreateConnection(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.CreateJob(ctx, db.Job{ID: "j", Name: "j", SourceConnectionID: "s", TargetConnectionID: "d", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "t", WriteMode: "append", OptionsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := st.CreateMaintenanceOperation(ctx, db.MaintenanceOperation{ID: "op", Kind: db.MaintenanceCompaction, JobID: "j", DryRun: false}, now); err != nil {
		t.Fatal(err)
	}
	first, _, err := st.ClaimMaintenanceOperation(ctx, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, db.Run{ID: "cmp", JobID: "j", DatasetKey: "ds", Status: "RUNNING", CorrelationID: "c", StartedAt: "2026-10-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMaintenanceRunID(ctx, first.ID, first.LeaseToken, "cmp", now); err != nil {
		t.Fatal(err)
	}
	// The holder crashes; its lease expires and another claim takes over.
	second, ok, err := st.ClaimMaintenanceOperation(ctx, now.Add(2*time.Minute), time.Minute)
	if err != nil || !ok {
		t.Fatalf("reclaim ok=%v err=%v", ok, err)
	}
	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, nil)
	srv.nowFn = func() time.Time { return now.Add(2 * time.Minute) }
	if err := srv.planCompaction(ctx, second); err != nil {
		t.Fatal(err)
	}
	op, _ := st.GetMaintenanceOperation(ctx, "op")
	if op.Status != db.MaintenanceSubmitted || op.RunID != "cmp" {
		t.Fatalf("operation=%+v", op)
	}
}
