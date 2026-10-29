package db

// Benchmarks of the SQLite writes behind the busiest worker RPCs. Every one
// goes through the master's single writer connection, so their cost bounds
// how many worker calls per second one master can absorb (see
// docs/master-scaling.md). Run with:
//
//	go test ./internal/db -run '^$' -bench Hotpath -benchtime 2000x

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
)

var benchPolicy = LeasePolicy{Duration: 30 * time.Second, MaxAttempts: 3, MaxActiveTasks: 1 << 30, BackoffBase: time.Second, BackoffMax: 30 * time.Second}

// openBenchStore opens a file-backed store with the leadership write fence
// active, as a serving master has it.
func openBenchStore(b *testing.B) *Store {
	b.Helper()
	ctx := context.Background()
	st, err := Open(ctx, Config{Path: filepath.Join(b.TempDir(), "bench.sqlite"), ReadConns: 4}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.Close() })
	lease, err := st.AcquireLeadership(ctx, "bench", time.Hour, nil)
	if err != nil {
		b.Fatal(err)
	}
	if err := st.ActivateLeadershipFence(ctx, "bench", lease.Epoch); err != nil {
		b.Fatal(err)
	}
	return st
}

// seedBenchTasks creates one run with n PENDING tasks.
func seedBenchTasks(b *testing.B, st *Store, n int) {
	b.Helper()
	ctx := context.Background()
	if err := st.CreateJob(ctx, Job{ID: "job", Name: "job", SourceConnectionID: "src", TargetConnectionID: "dst", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: []byte(`{}`)}); err != nil {
		b.Fatal(err)
	}
	if err := st.CreateRun(ctx, Run{ID: "run", JobID: "job", DatasetKey: "dataset", Status: "RUNNING", CorrelationID: "corr", StartedAt: nowUTC()}); err != nil {
		b.Fatal(err)
	}
	for start := 0; start < n; start += 500 {
		batch := make([]TaskInsert, 0, 500)
		for i := start; i < min(start+500, n); i++ {
			batch = append(batch, TaskInsert{ID: fmt.Sprintf("task-%07d", i), RunID: "run", TaskIndex: i + 1, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"})
		}
		if err := st.InsertTasks(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func counter(prefix string) func() (string, error) {
	n := 0
	return func() (string, error) {
		n++
		return fmt.Sprintf("%s-%07d", prefix, n), nil
	}
}

// assignAll leases every pending task and returns them.
func assignAll(b *testing.B, st *Store, n int) []Task {
	b.Helper()
	ids, tokens := counter("attempt"), counter("token")
	out := make([]Task, 0, n)
	for i := 0; i < n; i++ {
		task, ok, err := st.AssignNextPendingTaskInPool(context.Background(), "", "worker", "", time.Now(), benchPolicy, ids, tokens)
		if err != nil || !ok {
			b.Fatalf("assign %d: ok=%t err=%v", i, ok, err)
		}
		out = append(out, task)
	}
	return out
}

// BenchmarkHotpathAssignTask is RequestTask finding work, with a backlog of
// pending tasks behind the ones assigned.
func BenchmarkHotpathAssignTask(b *testing.B) {
	for _, backlog := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprintf("backlog-%d", backlog), func(b *testing.B) {
			st := openBenchStore(b)
			seedBenchTasks(b, st, backlog+b.N)
			ids, tokens := counter("attempt"), counter("token")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok, err := st.AssignNextPendingTaskInPool(context.Background(), "", "worker", "", time.Now(), benchPolicy, ids, tokens); err != nil || !ok {
					b.Fatalf("ok=%t err=%v", ok, err)
				}
			}
		})
	}
}

// BenchmarkHotpathIdlePoll is RequestTask from an idle worker when nothing
// is pending: the heartbeat write, run admission and an empty assignment,
// which every poll pays.
func BenchmarkHotpathIdlePoll(b *testing.B) {
	st := openBenchStore(b)
	seedBenchTasks(b, st, 64)
	assignAll(b, st, 64) // running work, nothing pending
	ids, tokens := counter("idle-attempt"), counter("idle-token")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.UpdateWorkerHeartbeat(ctx, "", "idle-worker", "", `{}`, "", "", 0); err != nil {
			b.Fatal(err)
		}
		if _, err := st.AdmitPendingRuns(ctx); err != nil {
			b.Fatal(err)
		}
		if _, ok, err := st.AssignNextPendingTaskInPool(ctx, "", "idle-worker", "", time.Now(), benchPolicy, ids, tokens); err != nil || ok {
			b.Fatalf("ok=%t err=%v", ok, err)
		}
	}
}

// BenchmarkHotpathProgress is ReportTaskProgress.
func BenchmarkHotpathProgress(b *testing.B) {
	st := openBenchStore(b)
	seedBenchTasks(b, st, 64)
	tasks := assignAll(b, st, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := tasks[i%len(tasks)]
		if err := st.UpdateTaskProgressFencedAt(context.Background(), "", task.ID, task.AttemptID, task.FencingToken, "worker", int64(i), int64(i)*100, int64(i)*20, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHotpathRenewLease is RenewTaskLease.
func BenchmarkHotpathRenewLease(b *testing.B) {
	st := openBenchStore(b)
	seedBenchTasks(b, st, 64)
	tasks := assignAll(b, st, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := tasks[i%len(tasks)]
		if _, err := st.RenewTaskLease(context.Background(), "", task.ID, task.AttemptID, task.FencingToken, "worker", time.Now(), 30*time.Second); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHotpathCompleteTask is ReportTaskResult with one verified
// artifact.
func BenchmarkHotpathCompleteTask(b *testing.B) {
	st := openBenchStore(b)
	seedBenchTasks(b, st, b.N+1) // one task stays pending so the run never finalizes
	tasks := assignAll(b, st, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := tasks[i]
		key := fmt.Sprintf("dataset/_runs/run-run/part-%06d-000.parquet", i+1)
		record := artifact.Record{ObjectKey: key, ByteSize: 4096, SHA256: fmt.Sprintf("%064x", i), RowCount: 1000, SchemaFingerprint: fmt.Sprintf("%064x", 0), RunID: "run", TaskID: task.ID, AttemptID: task.AttemptID, AttemptNumber: task.AttemptNumber, FormatVersion: artifact.FormatVersion, VerificationMethod: artifact.VerificationPortable, VerificationStatus: artifact.VerificationVerified, MaxHWM: fmt.Sprint(i)}
		if _, _, _, err := st.CompleteTaskAttemptWithArtifactsAndFailureClassAt(context.Background(), "", task.ID, task.AttemptID, task.FencingToken, "worker", "SUCCEEDED", nil, "", []artifact.Record{record}, 1000, 0, 4096, time.Now(), nil); err != nil {
			b.Fatal(err)
		}
	}
}
