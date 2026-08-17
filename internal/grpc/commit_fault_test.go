package grpcapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

const (
	faultHead = "HEAD"
	faultGet  = "GET"
	faultPut  = "PUT"

	failBefore          = "FAIL_BEFORE"
	writeThenError      = "WRITE_THEN_ERROR"
	returnNotFound      = "RETURN_NOT_FOUND"
	returnError         = "RETURN_ERROR"
	returnWrongContents = "RETURN_CONFLICTING_CONTENT"
)

type storageFault struct {
	op, key string
	n       int
	mode    string
}

type storageCall struct {
	op, key string
	n       int
	body    []byte
}

type scriptedCommitStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	faults  []storageFault
	calls   []storageCall
	counts  map[string]int
	durable map[string]int
}

func newScriptedCommitStore() *scriptedCommitStore {
	return &scriptedCommitStore{objects: map[string][]byte{}, counts: map[string]int{}, durable: map[string]int{}}
}

func (s *scriptedCommitStore) addFault(op, key string, occurrence int, mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = append(s.faults, storageFault{op: op, key: key, n: occurrence, mode: mode})
}

func (s *scriptedCommitStore) operation(op, key string, body []byte) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := op + "\x00" + key
	s.counts[id]++
	n := s.counts[id]
	s.calls = append(s.calls, storageCall{op: op, key: key, n: n, body: append([]byte(nil), body...)})
	for _, f := range s.faults {
		if f.op == op && f.key == key && f.n == n {
			return f.mode
		}
	}
	return ""
}

func (s *scriptedCommitStore) Head(_ context.Context, key string) error {
	mode := s.operation(faultHead, key, nil)
	if mode == returnError {
		return errors.New("injected HEAD failure")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; !ok {
		return errors.New("object not found")
	}
	return nil
}

func (s *scriptedCommitStore) GetObjectBytes(_ context.Context, key string) ([]byte, bool, error) {
	mode := s.operation(faultGet, key, nil)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch mode {
	case returnError:
		return nil, false, errors.New("injected GET failure")
	case returnNotFound:
		return nil, false, nil
	case returnWrongContents:
		return []byte(`{"corrupted":true}`), true, nil
	}
	b, ok := s.objects[key]
	return append([]byte(nil), b...), ok, nil
}

func (s *scriptedCommitStore) OpenObject(ctx context.Context, key string) (io.ReadCloser, bool, error) {
	b, found, err := s.GetObjectBytes(ctx, key)
	if err != nil || !found {
		return nil, found, err
	}
	return io.NopCloser(bytes.NewReader(b)), true, nil
}

func (s *scriptedCommitStore) PutObjectBytes(_ context.Context, key string, b []byte, _ string, _ map[string]string) error {
	mode := s.operation(faultPut, key, b)
	if mode == failBefore {
		return errors.New("injected PUT failure before durability")
	}
	s.mu.Lock()
	s.objects[key] = append([]byte(nil), b...)
	s.durable[key]++
	s.mu.Unlock()
	if mode == writeThenError {
		return errors.New("injected lost PUT response")
	}
	return nil
}

func (s *scriptedCommitStore) putCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[faultPut+"\x00"+key]
}

func (s *scriptedCommitStore) durablePutCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durable[key]
}

func (s *scriptedCommitStore) get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	return append([]byte(nil), b...), ok
}

type commitFixture struct {
	t           *testing.T
	ctx         context.Context
	st          *db.Store
	srv         *Server
	objects     *scriptedCommitStore
	runID       string
	jobID       string
	datasetKey  string
	parquetKey  string
	manifestKey string
	stateKey    string
	hwmCalls    int
	hwmFailures int
}

func newCommitFixture(t *testing.T, suffix string) *commitFixture {
	return newCommitFixtureWithRegistration(t, suffix, false)
}

func newCommitFixtureWithRegistration(t *testing.T, suffix string, registrationEnabled bool) *commitFixture {
	return newCommitFixtureWithOutput(t, suffix, registrationEnabled, true)
}

func newEmptyCommitFixture(t *testing.T, suffix string) *commitFixture {
	return newCommitFixtureWithOutput(t, suffix, false, false)
}

func newCommitFixtureWithOutput(t *testing.T, suffix string, registrationEnabled, hasOutput bool) *commitFixture {
	t.Helper()
	ctx := context.Background()
	st := openGRPCTestStore(t)
	runID, jobID := "run-"+suffix, "job-"+suffix
	prefix := "cert/" + suffix
	parquetKey := prefix + "/_runs/run-" + runID + "/part-000001-000.parquet"
	datasetKey := dataset.StorageKey("http://minio:9000", "bucket1", prefix)
	srcID, tgtID := "src-"+suffix, "tgt-"+suffix
	srcSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"dsn":"sqlserver://example"}`), []byte(srcID))
	if err != nil {
		t.Fatal(err)
	}
	tgtSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"access_key_id":"a","secret_access_key":"b"}`), []byte(tgtID))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: srcID, Name: srcID, Kind: "source", Engine: "mssql", MetadataJSON: []byte(`{}`), SecretEncBlob: srcSecret}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: tgtID, Name: tgtID, Kind: "target", Engine: "s3", MetadataJSON: mustJSONRaw(t, map[string]any{"endpoint": "http://minio:9000", "region": "us-east-1", "bucket": "bucket1", "prefix": prefix, "force_path_style": true}), SecretEncBlob: tgtSecret}); err != nil {
		t.Fatal(err)
	}
	hwmColumn := "id"
	if err := st.CreateJob(ctx, db.Job{ID: jobID, Name: jobID, SourceConnectionID: srcID, TargetConnectionID: tgtID, SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", Incremental: true, HWMColumn: &hwmColumn, OptionsJSON: mustJSONRaw(t, map[string]any{"table": "dbo.orders", "partition_strategy": "ordered_cursor", "cursor_column": "id"})}); err != nil {
		t.Fatal(err)
	}
	var registrationConfig json.RawMessage
	if registrationEnabled {
		registrationConfig = json.RawMessage(`{"enabled":true,"engine":"rest-go","uri":"http://catalog","table":"ns.tbl"}`)
	}
	if err := st.CreateRun(ctx, db.Run{ID: runID, JobID: jobID, DatasetKey: datasetKey, Status: "RUNNING", CorrelationID: "corr-" + suffix, StartedAt: "2026-07-22T10:00:00Z", RegistrationConfigJSON: registrationConfig}); err != nil {
		t.Fatal(err)
	}
	taskID := "task-" + suffix
	if err := st.InsertTasks(ctx, []db.TaskInsert{{ID: taskID, RunID: runID, TaskIndex: 1, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"}}); err != nil {
		t.Fatal(err)
	}
	objects := newScriptedCommitStore()
	parquetBytes := []byte("PAR1-test")
	if hasOutput {
		objects.objects[parquetKey] = parquetBytes
	}
	one := func(v string) func() (string, error) { return func() (string, error) { return v, nil } }
	assigned, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-"+suffix, time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC), db.LeasePolicy{Duration: time.Hour, MaxAttempts: 3}, one("attempt-"+suffix), one("token-"+suffix))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	var records []artifact.Record
	var rows, written int64
	if hasOutput {
		digest := sha256.Sum256(parquetBytes)
		records = []artifact.Record{{ObjectKey: parquetKey, ByteSize: int64(len(parquetBytes)), SHA256: hex.EncodeToString(digest[:]), RowCount: 10, SchemaFingerprint: strings.Repeat("a", 64), RunID: runID, TaskID: taskID, AttemptID: assigned.AttemptID, AttemptNumber: assigned.AttemptNumber, FileIndex: 0, FormatVersion: artifact.FormatVersion, VerificationMethod: artifact.VerificationPortable, VerificationStatus: artifact.VerificationVerified, MaxHWM: "42"}}
		rows = 10
		written = int64(len(parquetBytes))
	}
	if accepted, _, _, err := st.CompleteTaskAttemptWithArtifactsAt(ctx, "", taskID, assigned.AttemptID, assigned.FencingToken, "worker-"+suffix, "SUCCEEDED", nil, records, rows, 0, written, time.Date(2026, 7, 22, 10, 1, 0, 0, time.UTC)); err != nil || !accepted {
		t.Fatalf("complete accepted=%v err=%v", accepted, err)
	}
	if changed, status, err := st.TryFinalizeRun(ctx, runID); err != nil || !changed || status != "COMMITTING" {
		t.Fatalf("finalize transition changed=%v status=%s err=%v", changed, status, err)
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, time.Second, nil)
	srv.runIcebergRegistrationFn = nil
	srv.newCommitObjectStoreFn = func(context.Context, s3io.Config) (commitObjectStore, error) { return objects, nil }
	f := &commitFixture{t: t, ctx: ctx, st: st, srv: srv, objects: objects, runID: runID, jobID: jobID, datasetKey: datasetKey, parquetKey: parquetKey, manifestKey: prefix + "/_commits/run-" + runID + ".json", stateKey: prefix + "/_state.json"}
	srv.upsertHWMFn = func(ctx context.Context, jobID, value string) error {
		f.hwmCalls++
		if f.hwmFailures > 0 {
			f.hwmFailures--
			return errors.New("injected HWM failure")
		}
		return st.UpsertHWM(ctx, jobID, value)
	}
	return f
}

func TestEmptyDataOnlyCommitPublishesCanonicalZeroObjectManifest(t *testing.T) {
	f := newEmptyCommitFixture(t, "empty-data-only")
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	run := f.assertRun("SUCCEEDED", "COMPLETE")
	var manifest durableCommitManifest
	if err := json.Unmarshal(f.objects.objects[f.manifestKey], &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != 2 || len(manifest.Objects) != 0 || len(manifest.Artifacts) != 0 {
		t.Fatalf("empty manifest=%+v", manifest)
	}
	if run.CommitID == "" || f.objects.putCount(f.stateKey) != 1 {
		t.Fatalf("empty commit projection=%+v state writes=%d", run, f.objects.putCount(f.stateKey))
	}
}

func (f *commitFixture) finalize() error { return f.srv.finalizeRunCommit(f.ctx, f.runID) }

func TestCommitReconciliationUsesPersistedPrefixAfterConnectionDrift(t *testing.T) {
	f := newCommitFixture(t, "prefix-drift")
	f.objects.addFault(faultPut, f.manifestKey, 1, failBefore)
	if err := f.finalize(); err == nil {
		t.Fatal("expected first manifest write to fail after intent persistence")
	}
	run, err := f.st.GetRun(f.ctx, f.runID)
	if err != nil || len(run.CommitIntentJSON) == 0 {
		t.Fatalf("persisted intent missing: run=%+v err=%v", run, err)
	}
	job, err := f.st.GetJob(f.ctx, f.jobID)
	if err != nil {
		t.Fatal(err)
	}
	target, err := f.st.GetConnection(f.ctx, job.TargetConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	target.MetadataJSON = mustJSONRaw(t, map[string]any{"endpoint": "http://minio:9000", "region": "us-east-1", "bucket": "bucket1", "prefix": "cert/prefix-b", "force_path_style": true})
	if err := f.st.UpdateConnection(f.ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := f.srv.commitRun(f.ctx, f.runID); err != nil {
		t.Fatalf("reconcile persisted prefix: %v", err)
	}
	if err := f.st.CompleteRunCommit(f.ctx, f.runID); err != nil {
		t.Fatal(err)
	}
	f.objects.mu.Lock()
	defer f.objects.mu.Unlock()
	for _, call := range f.objects.calls {
		if strings.HasPrefix(call.key, "cert/prefix-b/") {
			t.Fatalf("mutable prefix was accessed during reconciliation: %+v", call)
		}
	}
}

func (f *commitFixture) assertRun(status, phase string) db.Run {
	f.t.Helper()
	run, err := f.st.GetRun(f.ctx, f.runID)
	if err != nil {
		f.t.Fatal(err)
	}
	if run.Status != status || run.CommitPhase != phase {
		f.t.Fatalf("run status/phase=%s/%s want %s/%s", run.Status, run.CommitPhase, status, phase)
	}
	if status == "SUCCEEDED" && run.CommitID == "" {
		f.t.Fatal("successful run has no commit ID")
	}
	return run
}

func (f *commitFixture) assertLocked() {
	f.t.Helper()
	err := f.st.CreateRun(f.ctx, db.Run{ID: f.runID + "-next", JobID: f.jobID, DatasetKey: f.datasetKey, Status: "PLANNING", CorrelationID: "next", StartedAt: "2026-07-22T10:01:00Z"})
	if !errors.Is(err, db.ErrActiveDatasetRun) {
		f.t.Fatalf("same-dataset create error=%v", err)
	}
	if err := f.st.CreateRun(f.ctx, db.Run{ID: f.runID + "-other", JobID: f.jobID, DatasetKey: f.datasetKey + "-other", Status: "PLANNING", CorrelationID: "other", StartedAt: "2026-07-22T10:01:00Z"}); err != nil {
		f.t.Fatalf("other dataset blocked: %v", err)
	}
}

func (f *commitFixture) retrySucceeds(stableCommitID string) db.Run {
	f.t.Helper()
	if err := f.finalize(); err != nil {
		f.t.Fatalf("retry finalize: %v", err)
	}
	run := f.assertRun("SUCCEEDED", "COMPLETE")
	if stableCommitID != "" && run.CommitID != stableCommitID {
		f.t.Fatalf("commit ID changed: %s -> %s", stableCommitID, run.CommitID)
	}
	return run
}

func TestCommitFaultInjectionManifestBoundaries(t *testing.T) {
	tests := []struct {
		name, op, mode string
		occurrence     int
		conflict       bool
	}{
		{"before_manifest_write", faultPut, failBefore, 1, false},
		{"manifest_write_then_error", faultPut, writeThenError, 1, false},
		{"manifest_readback_transient", faultGet, returnError, 2, false},
		{"manifest_readback_wrong", faultGet, returnWrongContents, 2, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCommitFixture(t, tc.name)
			f.objects.addFault(tc.op, f.manifestKey, tc.occurrence, tc.mode)
			err := f.finalize()
			if err == nil {
				t.Fatal("expected injected failure")
			}
			run := f.assertRun("COMMITTING", "RETRY_REQUIRED")
			f.assertLocked()
			if _, ok := f.objects.get(f.stateKey); ok {
				t.Fatal("state advanced after manifest failure")
			}
			f.retrySucceeds(run.CommitID)
			if f.objects.durablePutCount(f.manifestKey) != 1 {
				t.Fatalf("durable manifest writes=%d", f.objects.durablePutCount(f.manifestKey))
			}
		})
	}
}

func TestCommitExistingManifestMatchingAndConflicting(t *testing.T) {
	f := newCommitFixture(t, "existing-manifest")
	f.objects.addFault(faultPut, f.stateKey, 1, failBefore)
	if err := f.finalize(); err == nil {
		t.Fatal("expected state failure")
	}
	run := f.assertRun("COMMITTING", "RETRY_REQUIRED")
	if f.objects.putCount(f.manifestKey) != 1 {
		t.Fatal("manifest was not created once")
	}
	f.retrySucceeds(run.CommitID)
	if f.objects.putCount(f.manifestKey) != 1 {
		t.Fatal("matching manifest was overwritten")
	}

	g := newCommitFixture(t, "conflicting-manifest")
	g.objects.objects[g.manifestKey] = []byte(`{"run_id":"other"}`)
	err := g.finalize()
	if err == nil || !strings.Contains(err.Error(), "integrity conflict") {
		t.Fatalf("error=%v", err)
	}
	run = g.assertRun("FAILED", "FAILED")
	if run.CommitReconciliationStatus != db.CommitReconciliationTerminal || run.FailureClass != commitFailureIntegrity {
		t.Fatalf("terminal conflict projection=%+v", run)
	}
	if got, _ := g.objects.get(g.manifestKey); string(got) != `{"run_id":"other"}` {
		t.Fatal("conflicting manifest overwritten")
	}
	if g.objects.putCount(g.manifestKey) != 0 {
		t.Fatal("conflicting manifest was written")
	}
	if _, ok := g.objects.get(g.stateKey); ok {
		t.Fatal("state advanced")
	}
}

func TestCommitFaultInjectionStateBoundaries(t *testing.T) {
	tests := []struct {
		name, op, mode string
		occurrence     int
	}{
		{"before_state_write", faultPut, failBefore, 1},
		{"state_write_then_error", faultPut, writeThenError, 1},
		{"state_readback_transient", faultGet, returnError, 2},
		{"state_readback_wrong", faultGet, returnWrongContents, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCommitFixture(t, tc.name)
			f.objects.addFault(tc.op, f.stateKey, tc.occurrence, tc.mode)
			if err := f.finalize(); err == nil {
				t.Fatal("expected injected state failure")
			}
			run := f.assertRun("COMMITTING", "RETRY_REQUIRED")
			if _, ok := f.objects.get(f.manifestKey); !ok {
				t.Fatal("manifest should remain durable")
			}
			f.assertLocked()
			f.retrySucceeds(run.CommitID)
			if f.objects.putCount(f.manifestKey) != 1 {
				t.Fatalf("manifest writes=%d", f.objects.putCount(f.manifestKey))
			}
			if f.objects.durablePutCount(f.stateKey) != 1 {
				t.Fatalf("durable state writes=%d", f.objects.durablePutCount(f.stateKey))
			}
		})
	}
}

func TestCommitExistingStateOlderMatchingAndNewer(t *testing.T) {
	t.Run("matching", func(t *testing.T) {
		f := newCommitFixture(t, "matching-state")
		f.hwmFailures = 1
		if err := f.finalize(); err == nil {
			t.Fatal("expected HWM failure")
		}
		run := f.assertRun("COMMITTING", "RETRY_REQUIRED")
		state, ok := f.objects.get(f.stateKey)
		if !ok {
			t.Fatal("state not durable")
		}
		f.retrySucceeds(run.CommitID)
		if got, _ := f.objects.get(f.stateKey); string(got) != string(state) {
			t.Fatal("matching state changed")
		}
		if f.objects.putCount(f.stateKey) != 1 {
			t.Fatal("matching state was rewritten")
		}
		v, ok, err := f.st.GetHWM(f.ctx, f.jobID)
		if err != nil || !ok || v != "42" {
			t.Fatalf("HWM=%q ok=%v err=%v", v, ok, err)
		}
	})
	t.Run("older", func(t *testing.T) {
		f := newCommitFixture(t, "older-state")
		f.objects.objects[f.stateKey] = []byte(`{"last_committed_run_id":"old","committed_at":"2026-07-21T10:00:00Z","max_hwm_value":"10","max_part":0}`)
		if err := f.finalize(); err != nil {
			t.Fatal(err)
		}
		state, _ := f.objects.get(f.stateKey)
		if !strings.Contains(string(state), f.runID) || f.objects.putCount(f.stateKey) != 1 {
			t.Fatalf("state=%s writes=%d", state, f.objects.putCount(f.stateKey))
		}
	})
	t.Run("newer", func(t *testing.T) {
		f := newCommitFixture(t, "newer-state")
		newer := []byte(`{"last_committed_run_id":"newer","committed_at":"2026-07-23T10:00:00Z","commit_id":"new","manifest_key":"new.json"}`)
		f.objects.objects[f.stateKey] = newer
		err := f.finalize()
		if err == nil || !strings.Contains(err.Error(), "fencing conflict") {
			t.Fatalf("error=%v", err)
		}
		run := f.assertRun("FAILED", "FAILED")
		if run.CommitReconciliationStatus != db.CommitReconciliationActionRequired || !run.OperatorActionRequired {
			t.Fatalf("fencing conflict projection=%+v", run)
		}
		if got, _ := f.objects.get(f.stateKey); string(got) != string(newer) {
			t.Fatal("newer state overwritten")
		}
		if f.objects.putCount(f.stateKey) != 0 {
			t.Fatal("newer state write attempted")
		}
	})
	t.Run("same_run_conflicting_identity", func(t *testing.T) {
		f := newCommitFixture(t, "conflicting-state")
		conflict := []byte(fmt.Sprintf(`{"last_committed_run_id":%q,"committed_at":"2026-07-22T10:00:00Z","commit_id":"wrong","manifest_key":"wrong.json"}`, f.runID))
		f.objects.objects[f.stateKey] = conflict
		err := f.finalize()
		if err == nil || !strings.Contains(err.Error(), "checkpoint integrity conflict") {
			t.Fatalf("error=%v", err)
		}
		run := f.assertRun("FAILED", "FAILED")
		if run.CommitReconciliationStatus != db.CommitReconciliationTerminal || run.FailureClass != commitFailureIntegrity {
			t.Fatalf("state conflict projection=%+v", run)
		}
		if got, _ := f.objects.get(f.stateKey); string(got) != string(conflict) {
			t.Fatal("conflicting state overwritten")
		}
		if f.objects.putCount(f.stateKey) != 0 {
			t.Fatal("conflicting state write attempted")
		}
	})
}

func TestCommitExpectedObjectFailures(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		f := newCommitFixture(t, "missing-object")
		delete(f.objects.objects, f.parquetKey)
		err := f.finalize()
		if err == nil || !strings.Contains(err.Error(), f.parquetKey) {
			t.Fatalf("error=%v", err)
		}
		f.assertRun("COMMITTING", "RETRY_REQUIRED")
		f.assertLocked()
		if f.objects.putCount(f.manifestKey) != 0 || f.objects.putCount(f.stateKey) != 0 {
			t.Fatal("publication occurred with missing object")
		}
		f.objects.objects[f.parquetKey] = []byte("PAR1-test")
		f.retrySucceeds("")
	})
	t.Run("head_transient", func(t *testing.T) {
		f := newCommitFixture(t, "head-transient")
		f.objects.addFault(faultHead, f.parquetKey, 1, returnError)
		if err := f.finalize(); err == nil {
			t.Fatal("expected HEAD failure")
		}
		f.assertRun("COMMITTING", "RETRY_REQUIRED")
		f.assertLocked()
		f.retrySucceeds("")
	})
}

func TestCommitHWMFailureRequiresRepairBeforeSuccess(t *testing.T) {
	f := newCommitFixture(t, "hwm-repair")
	f.hwmFailures = 1
	if err := f.finalize(); err == nil || !strings.Contains(err.Error(), "HWM") {
		t.Fatalf("error=%v", err)
	}
	run := f.assertRun("COMMITTING", "RETRY_REQUIRED")
	f.assertLocked()
	if _, ok := f.objects.get(f.stateKey); !ok {
		t.Fatal("authoritative state not durable")
	}
	if _, ok, _ := f.st.GetHWM(f.ctx, f.jobID); ok {
		t.Fatal("HWM unexpectedly repaired")
	}
	f.retrySucceeds(run.CommitID)
	if f.hwmCalls != 2 || f.objects.putCount(f.stateKey) != 1 {
		t.Fatalf("hwm calls=%d state writes=%d", f.hwmCalls, f.objects.putCount(f.stateKey))
	}
}

func TestCommitStartupCrashBoundariesAndRepeat(t *testing.T) {
	t.Run("after_entering_committing", func(t *testing.T) {
		f := newCommitFixture(t, "restart-entered")
		if err := f.srv.ReconcileCommittingRuns(f.ctx); err != nil {
			t.Fatal(err)
		}
		f.assertRun("SUCCEEDED", "COMPLETE")
	})
	t.Run("after_manifest", func(t *testing.T) {
		f := newCommitFixture(t, "restart-manifest")
		f.objects.addFault(faultPut, f.stateKey, 1, failBefore)
		if err := f.finalize(); err == nil {
			t.Fatal("expected state failure")
		}
		before := f.assertRun("COMMITTING", "RETRY_REQUIRED")
		manifest, _ := f.objects.get(f.manifestKey)
		f.srv.nowFn = func() time.Time { return time.Now().UTC().Add(time.Hour) }
		if err := f.srv.ReconcileCommittingRuns(f.ctx); err != nil {
			t.Fatal(err)
		}
		after := f.assertRun("SUCCEEDED", "COMPLETE")
		if before.CommitID == "" || after.CommitID != before.CommitID {
			t.Fatalf("commit ID changed across restart: %q -> %q", before.CommitID, after.CommitID)
		}
		if got, _ := f.objects.get(f.manifestKey); string(got) != string(manifest) || f.objects.putCount(f.manifestKey) != 1 {
			t.Fatal("manifest duplicated or changed")
		}
	})
	t.Run("after_state_and_repeat", func(t *testing.T) {
		f := newCommitFixture(t, "restart-state")
		f.hwmFailures = 1
		if err := f.finalize(); err == nil {
			t.Fatal("expected HWM failure")
		}
		before := f.assertRun("COMMITTING", "RETRY_REQUIRED")
		state, _ := f.objects.get(f.stateKey)
		f.srv.nowFn = func() time.Time { return time.Now().UTC().Add(time.Hour) }
		if err := f.srv.ReconcileCommittingRuns(f.ctx); err != nil {
			t.Fatal(err)
		}
		if err := f.srv.ReconcileCommittingRuns(f.ctx); err != nil {
			t.Fatal(err)
		}
		after := f.assertRun("SUCCEEDED", "COMPLETE")
		if after.CommitID != before.CommitID {
			t.Fatalf("commit ID changed across restart: %q -> %q", before.CommitID, after.CommitID)
		}
		if got, _ := f.objects.get(f.stateKey); string(got) != string(state) || f.objects.putCount(f.stateKey) != 1 {
			t.Fatal("state duplicated or changed")
		}
		events, err := f.st.ListEventsForRun(f.ctx, f.runID, 100)
		if err != nil {
			t.Fatal(err)
		}
		committed := 0
		for _, event := range events {
			if event.Message == "run committed" {
				committed++
			}
		}
		if committed != 1 {
			t.Fatalf("run committed events=%d", committed)
		}
	})
}

func TestCommittingRecoveryPhaseMatrixIsIdempotent(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*commitFixture)
	}{
		{"PREPARING", func(*commitFixture) {}},
		{"INTENT", func(f *commitFixture) {
			f.objects.addFault(faultPut, f.manifestKey, 1, failBefore)
			if err := f.srv.commitRun(f.ctx, f.runID); err == nil {
				f.t.Fatal("expected manifest failure")
			}
		}},
		{"MANIFEST_VERIFIED", func(f *commitFixture) {
			f.objects.addFault(faultPut, f.stateKey, 1, failBefore)
			if err := f.srv.commitRun(f.ctx, f.runID); err == nil {
				f.t.Fatal("expected state failure")
			}
		}},
		{"STATE_VERIFIED", func(f *commitFixture) {
			f.hwmFailures = 1
			if err := f.srv.commitRun(f.ctx, f.runID); err == nil {
				f.t.Fatal("expected HWM failure")
			}
		}},
		{"VERIFIED", func(f *commitFixture) {
			if err := f.srv.commitRun(f.ctx, f.runID); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"RETRY_REQUIRED", func(f *commitFixture) {
			f.hwmFailures = 1
			if err := f.finalize(); err == nil {
				f.t.Fatal("expected retry-required failure")
			}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCommitFixtureWithRegistration(t, "phase-"+strings.ToLower(tc.name), true)
			tc.prepare(f)
			run := f.assertRun("COMMITTING", tc.name)
			if tc.name == "RETRY_REQUIRED" {
				f.srv.nowFn = func() time.Time { return time.Now().UTC().Add(time.Hour) }
			}

			if err := f.srv.ReconcileCommittingRuns(f.ctx); err != nil {
				t.Fatal(err)
			}
			recovered := f.assertRun("SUCCEEDED", "COMPLETE")
			if run.CommitID != "" && recovered.CommitID != run.CommitID {
				t.Fatalf("commit id changed: %s -> %s", run.CommitID, recovered.CommitID)
			}
			manifestWrites := f.objects.durablePutCount(f.manifestKey)
			stateWrites := f.objects.durablePutCount(f.stateKey)
			hwmCalls := f.hwmCalls
			registrationID := recovered.RegistrationID
			registration, err := f.st.GetRegistrationForRun(f.ctx, f.runID)
			if err != nil || registrationID == "" || registration.ID != registrationID || registration.CommitID != recovered.CommitID || registration.ManifestKey != f.manifestKey {
				t.Fatalf("registration not queued exactly with recovered run: run=%q registration=%+v err=%v", registrationID, registration, err)
			}

			if err := f.srv.ReconcileCommittingRuns(f.ctx); err != nil {
				t.Fatal(err)
			}
			if f.objects.durablePutCount(f.manifestKey) != manifestWrites || f.objects.durablePutCount(f.stateKey) != stateWrites || f.hwmCalls != hwmCalls {
				t.Fatalf("repeat scan mutated durable commit: manifest=%d/%d state=%d/%d hwm=%d/%d",
					f.objects.durablePutCount(f.manifestKey), manifestWrites,
					f.objects.durablePutCount(f.stateKey), stateWrites,
					f.hwmCalls, hwmCalls)
			}
			repeatedRun, err := f.st.GetRun(f.ctx, f.runID)
			if err != nil || repeatedRun.RegistrationID != registrationID {
				t.Fatalf("registration projection changed: before=%q after=%q err=%v", registrationID, repeatedRun.RegistrationID, err)
			}
			repeatedRegistration, err := f.st.GetRegistrationForRun(f.ctx, f.runID)
			if err != nil || repeatedRegistration.ID != registration.ID || repeatedRegistration.DatasetSequence != registration.DatasetSequence {
				t.Fatalf("registration changed after repeat scan: before=%+v after=%+v err=%v", registration, repeatedRegistration, err)
			}
			events, err := f.st.ListEventsForRun(f.ctx, f.runID, 100)
			if err != nil {
				t.Fatal(err)
			}
			committed, queued := 0, 0
			for _, event := range events {
				if event.Message == "run committed" {
					committed++
				}
				if event.Message == "iceberg registration queued" {
					queued++
				}
			}
			if committed != 1 {
				t.Fatalf("run committed events=%d want 1", committed)
			}
			if queued != 1 {
				t.Fatalf("registration queued events=%d want 1", queued)
			}
		})
	}
}

func TestCommitCompletionEventWaitsForDurableRunSuccess(t *testing.T) {
	f := newCommitFixture(t, "sqlite-completion-event")
	f.srv.completeRunCommitFn = func(context.Context, string) error {
		return errors.New("injected sqlite completion failure")
	}

	if err := f.finalize(); err == nil {
		t.Fatal("expected sqlite completion failure")
	}
	f.assertRun("COMMITTING", "VERIFIED")
	events, err := f.st.ListEventsForRun(f.ctx, f.runID, 100)
	if err != nil {
		t.Fatal(err)
	}
	storageVerified, recoveryPending, final := 0, 0, 0
	for _, event := range events {
		switch event.Message {
		case "storage publication verified":
			storageVerified++
		case "run completion pending recovery":
			recoveryPending++
		case "run committed", "run SUCCEEDED":
			final++
		}
	}
	if storageVerified != 1 || recoveryPending != 1 || final != 0 {
		t.Fatalf("events storage=%d recovery=%d final=%d", storageVerified, recoveryPending, final)
	}

	f.srv.completeRunCommitFn = f.st.CompleteRunCommit
	if err := f.srv.ReconcileCommittingRuns(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.srv.ReconcileCommittingRuns(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.assertRun("SUCCEEDED", "COMPLETE")
	events, err = f.st.ListEventsForRun(f.ctx, f.runID, 100)
	if err != nil {
		t.Fatal(err)
	}
	storageVerified, final = 0, 0
	for _, event := range events {
		if event.Message == "storage publication verified" {
			storageVerified++
		}
		if event.Message == "run committed" || event.Message == "run SUCCEEDED" {
			final++
		}
	}
	if storageVerified != 1 || final != 1 {
		t.Fatalf("retry events storage=%d final=%d", storageVerified, final)
	}
}

func TestCommitDatasetLockReleaseAndCancellationBoundaries(t *testing.T) {
	stages := []struct {
		name    string
		prepare func(*commitFixture)
	}{
		{"before_manifest", func(f *commitFixture) { f.objects.addFault(faultPut, f.manifestKey, 1, failBefore) }},
		{"after_manifest", func(f *commitFixture) { f.objects.addFault(faultPut, f.stateKey, 1, failBefore) }},
		{"after_state", func(f *commitFixture) { f.hwmFailures = 1 }},
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			f := newCommitFixture(t, "cancel-"+stage.name)
			stage.prepare(f)
			if err := f.finalize(); err == nil {
				t.Fatal("expected staged failure")
			}
			f.assertLocked()
			changed, status, _, err := f.st.CancelRun(f.ctx, f.runID, "cancel during commit")
			if err != nil || changed || status != "COMMITTING" {
				t.Fatalf("cancel changed=%v status=%s err=%v", changed, status, err)
			}
			f.retrySucceeds("")
			if err := f.st.CreateRun(f.ctx, db.Run{ID: f.runID + "-after", JobID: f.jobID, DatasetKey: f.datasetKey, Status: "PLANNING", CorrelationID: "after", StartedAt: "2026-07-22T10:02:00Z"}); err != nil {
				t.Fatalf("lock not released after success: %v", err)
			}
		})
	}
	t.Run("before_sqlite_success", func(t *testing.T) {
		f := newCommitFixture(t, "cancel-before-sqlite-success")
		if err := f.srv.commitRun(f.ctx, f.runID); err != nil {
			t.Fatal(err)
		}
		f.assertRun("COMMITTING", "VERIFIED")
		f.assertLocked()
		changed, status, _, err := f.st.CancelRun(f.ctx, f.runID, "cancel after verification")
		if err != nil || changed || status != "COMMITTING" {
			t.Fatalf("cancel changed=%v status=%s err=%v", changed, status, err)
		}
		manifestWrites, stateWrites := f.objects.putCount(f.manifestKey), f.objects.putCount(f.stateKey)
		f.retrySucceeds("")
		if f.objects.putCount(f.manifestKey) != manifestWrites || f.objects.putCount(f.stateKey) != stateWrites {
			t.Fatal("verified retry rewrote durable objects")
		}
	})
}

func TestSuccessfulCommitOperationOrderAndIntent(t *testing.T) {
	f := newCommitFixture(t, "operation-order")
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	run := f.assertRun("SUCCEEDED", "COMPLETE")
	if len(run.CommitIntentJSON) == 0 {
		t.Fatal("commit intent missing")
	}
	var intent map[string]any
	if err := json.Unmarshal(run.CommitIntentJSON, &intent); err != nil {
		t.Fatal(err)
	}
	if intent["commit_id"] != run.CommitID || intent["manifest_key"] != f.manifestKey || intent["state_key"] != f.stateKey {
		t.Fatalf("intent=%v", intent)
	}
	want := []string{fmt.Sprintf("HEAD %s", f.parquetKey), fmt.Sprintf("GET %s", f.parquetKey), fmt.Sprintf("GET %s", f.stateKey), fmt.Sprintf("GET %s", f.manifestKey), fmt.Sprintf("PUT %s", f.manifestKey), fmt.Sprintf("GET %s", f.manifestKey), fmt.Sprintf("PUT %s", f.stateKey), fmt.Sprintf("GET %s", f.stateKey)}
	got := make([]string, 0, len(f.objects.calls))
	for _, c := range f.objects.calls {
		got = append(got, c.op+" "+c.key)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("operation order:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestCommitRejectsSameSizeArtifactCorruptionAsTerminal(t *testing.T) {
	f := newCommitFixture(t, "artifact-same-size-corruption")
	f.objects.objects[f.parquetKey] = []byte("PAR1-evil")
	if err := f.finalize(); err == nil || !strings.Contains(err.Error(), "artifact sha256 mismatch") {
		t.Fatalf("error=%v", err)
	}
	run := f.assertRun("FAILED", "FAILED")
	if run.CommitReconciliationStatus != db.CommitReconciliationTerminal || run.FailureClass != commitFailureIntegrity {
		t.Fatalf("corruption projection=%+v", run)
	}
	if _, ok := f.objects.objects[f.manifestKey]; ok {
		t.Fatal("manifest published for corrupt artifact")
	}
	if _, ok := f.objects.objects[f.stateKey]; ok {
		t.Fatal("state published for corrupt artifact")
	}
	events, err := f.st.ListEventsForRun(f.ctx, f.runID, 100)
	if err != nil {
		t.Fatal(err)
	}
	integrityEvents := 0
	for _, event := range events {
		if strings.Contains(string(event.FieldsJSON), `"event_type":"ARTIFACT_INTEGRITY_REJECTED"`) {
			integrityEvents++
			if !strings.Contains(string(event.FieldsJSON), `"classification":"ARTIFACT_DIGEST_MISMATCH"`) {
				t.Fatalf("event=%s", event.FieldsJSON)
			}
			if !strings.Contains(string(event.FieldsJSON), `"object_key":"`+f.parquetKey+`"`) || !strings.Contains(string(event.FieldsJSON), `"attempt_id":"attempt-artifact-same-size-corruption"`) {
				t.Fatalf("event lacks artifact identity: %s", event.FieldsJSON)
			}
		}
	}
	if integrityEvents != 1 {
		t.Fatalf("bounded integrity events=%d", integrityEvents)
	}
}

func TestCommitRestartRevalidatesArtifactAfterManifestDurability(t *testing.T) {
	f := newCommitFixture(t, "artifact-restart-revalidation")
	f.objects.addFault(faultPut, f.stateKey, 1, failBefore)
	if err := f.finalize(); err == nil {
		t.Fatal("expected injected state failure")
	}
	run := f.assertRun("COMMITTING", "RETRY_REQUIRED")
	if run.CommitID == "" || f.objects.putCount(f.manifestKey) != 1 {
		t.Fatal("durable manifest identity missing")
	}
	f.objects.objects[f.parquetKey] = []byte("PAR1-evil")
	if err := f.finalize(); err == nil || !strings.Contains(err.Error(), "artifact sha256 mismatch") {
		t.Fatalf("restart error=%v", err)
	}
	if _, ok := f.objects.objects[f.stateKey]; ok {
		t.Fatal("restart advanced state after artifact corruption")
	}
	if f.objects.putCount(f.manifestKey) != 1 {
		t.Fatal("restart rewrote manifest")
	}
	terminal := f.assertRun("FAILED", "FAILED")
	if terminal.CommitID != run.CommitID || terminal.CommitReconciliationStatus != db.CommitReconciliationTerminal {
		t.Fatalf("restart corruption projection=%+v", terminal)
	}
}

func TestVerifiedCommitManifestV2ContainsCanonicalArtifacts(t *testing.T) {
	f := newCommitFixture(t, "manifest-v2")
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		SchemaVersion int               `json:"schema_version"`
		Objects       []string          `json:"objects"`
		Artifacts     []artifact.Record `json:"artifacts"`
	}
	if err := json.Unmarshal(f.objects.objects[f.manifestKey], &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != 2 || len(manifest.Artifacts) != 1 || len(manifest.Objects) != 1 {
		t.Fatalf("manifest=%+v", manifest)
	}
	record := manifest.Artifacts[0]
	if record.ObjectKey != f.parquetKey || record.SHA256 == "" || record.ByteSize <= 0 || record.SchemaFingerprint == "" || record.VerificationStatus != artifact.VerificationVerified {
		t.Fatalf("artifact=%+v", record)
	}
}
