package grpcapi

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

const reconcileTaskSpec = `{"type":"sql_cursor_range","source_mode":"table","table":"dbo.orders","cursor_column":"id","cursor_domain":"int64","lower":"0","lower_exclusive":true,"upper":"100","upper_inclusive":true}`

type fakeSourceCounter struct {
	mu      sync.Mutex
	rows    int64
	err     error
	queries []connectors.CursorQuery
	closed  bool
}

func (c *fakeSourceCounter) CountRange(_ context.Context, q connectors.CursorQuery) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, q)
	return c.rows, c.err
}

func (c *fakeSourceCounter) Close() error { c.closed = true; return nil }

func reconcileFixture(t *testing.T, suffix string, recon map[string]any, counter *fakeSourceCounter) *commitFixture {
	t.Helper()
	f := newCommitFixtureWithChecksum(t, suffix, false, true, false, func(s *commitFixtureSettings) {
		s.partitionSpec = reconcileTaskSpec
		if recon != nil {
			s.jobOptions["reconciliation"] = recon
		}
	})
	f.srv.openSourceCounterFn = func(context.Context, string, string) (sourceRangeCounter, error) { return counter, nil }
	return f
}

func (f *commitFixture) reconciliations() []db.SourceReconciliation {
	f.t.Helper()
	recs, err := f.st.ListSourceReconciliations(f.ctx, f.runID)
	if err != nil {
		f.t.Fatal(err)
	}
	return recs
}

func (f *commitFixture) assertNothingPublished() {
	f.t.Helper()
	if _, ok := f.objects.objects[f.manifestKey]; ok {
		f.t.Fatal("manifest published")
	}
	if _, ok := f.objects.objects[f.stateKey]; ok {
		f.t.Fatal("state published")
	}
	run, err := f.st.GetRun(f.ctx, f.runID)
	if err != nil {
		f.t.Fatal(err)
	}
	if len(run.CommitIntentJSON) != 0 || run.CommitID != "" {
		f.t.Fatal("commit intent persisted for a rejected run")
	}
}

func immutableCount(onMismatch string) map[string]any {
	return map[string]any{"mode": "count", "consistency": "immutable", "on_mismatch": onMismatch}
}

func TestCommitFailsTerminallyWhenSourceHasMoreRowsThanWritten(t *testing.T) {
	counter := &fakeSourceCounter{rows: 15} // task reported 10 rows with valid checksums
	f := reconcileFixture(t, "recon-fail", immutableCount("fail"), counter)
	err := f.finalize()
	if err == nil || !strings.Contains(err.Error(), "source reconciliation mismatch") {
		t.Fatalf("error=%v", err)
	}
	run := f.assertRun("FAILED", "FAILED")
	if run.CommitReconciliationStatus != db.CommitReconciliationTerminal || run.FailureClass != commitFailureValidation || run.OperatorActionRequired {
		t.Fatalf("projection=%+v", run)
	}
	f.assertNothingPublished()
	recs := f.reconciliations()
	if len(recs) != 1 || recs[0].Status != db.SourceReconciliationMismatch || recs[0].SourceRows != 15 || recs[0].OutputRows != 10 || recs[0].Consistency != "immutable" {
		t.Fatalf("records=%+v", recs)
	}
	if !counter.closed {
		t.Fatal("source counter not closed")
	}
}

func TestCommitHoldsForOperatorReviewOnMismatch(t *testing.T) {
	f := reconcileFixture(t, "recon-review", immutableCount("review"), &fakeSourceCounter{rows: 15})
	if err := f.finalize(); err == nil || !strings.Contains(err.Error(), "held for operator review") {
		t.Fatalf("error=%v", err)
	}
	run := f.assertRun("FAILED", "FAILED")
	if run.CommitReconciliationStatus != db.CommitReconciliationActionRequired || !run.OperatorActionRequired || run.FailureClass != commitFailureOperator {
		t.Fatalf("projection=%+v", run)
	}
	f.assertNothingPublished()
	if recs := f.reconciliations(); len(recs) != 1 || recs[0].Status != db.SourceReconciliationMismatch {
		t.Fatalf("records=%+v", recs)
	}
}

func TestCommitProceedsWithWarningWhenMutableSourceCountDiffers(t *testing.T) {
	f := reconcileFixture(t, "recon-indeterminate", map[string]any{"mode": "count"}, &fakeSourceCounter{rows: 15})
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	f.assertRun("SUCCEEDED", "COMPLETE")
	recs := f.reconciliations()
	if len(recs) != 1 || recs[0].Status != db.SourceReconciliationIndeterminate || recs[0].Consistency != "none" {
		t.Fatalf("records=%+v", recs)
	}
	events, err := f.st.ListEventsForRun(f.ctx, f.runID, 100)
	if err != nil {
		t.Fatal(err)
	}
	warned := false
	for _, e := range events {
		warned = warned || strings.Contains(string(e.FieldsJSON), "SOURCE_RECONCILIATION_INDETERMINATE")
	}
	if !warned {
		t.Fatal("no indeterminate warning event")
	}
}

func TestCommitRecordsMatch(t *testing.T) {
	f := reconcileFixture(t, "recon-match", immutableCount("fail"), &fakeSourceCounter{rows: 10})
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	f.assertRun("SUCCEEDED", "COMPLETE")
	if recs := f.reconciliations(); len(recs) != 1 || recs[0].Status != db.SourceReconciliationMatch {
		t.Fatalf("records=%+v", recs)
	}
}

func TestCommitNeverCountsWhenReconciliationIsOff(t *testing.T) {
	for name, recon := range map[string]map[string]any{"absent": nil, "off": {"mode": "off", "consistency": "immutable"}} {
		t.Run(name, func(t *testing.T) {
			counter := &fakeSourceCounter{rows: 999}
			f := reconcileFixture(t, "recon-off-"+name, recon, counter)
			f.srv.openSourceCounterFn = func(context.Context, string, string) (sourceRangeCounter, error) {
				t.Fatal("source opened although reconciliation is off")
				return counter, nil
			}
			if err := f.finalize(); err != nil {
				t.Fatal(err)
			}
			f.assertRun("SUCCEEDED", "COMPLETE")
			if len(counter.queries) != 0 || len(f.reconciliations()) != 0 {
				t.Fatalf("counter called or rows recorded: %d %v", len(counter.queries), f.reconciliations())
			}
		})
	}
}

func TestCommitRetriesWhenSourceCountFailsAndRecordsNoMismatch(t *testing.T) {
	counter := &fakeSourceCounter{err: errors.New("source unreachable")}
	f := reconcileFixture(t, "recon-error", immutableCount("fail"), counter)
	if err := f.finalize(); err == nil || !strings.Contains(err.Error(), "source reconciliation") {
		t.Fatalf("error=%v", err)
	}
	run := f.assertRun("COMMITTING", "RETRY_REQUIRED")
	if run.CommitReconciliationStatus != db.CommitReconciliationRetryRequired {
		t.Fatalf("projection=%+v", run)
	}
	recs := f.reconciliations()
	if len(recs) != 1 || recs[0].Status != db.SourceReconciliationError {
		t.Fatalf("records=%+v", recs)
	}
	f.assertNothingPublished()

	counter.err, counter.rows = nil, 10
	if err := f.finalize(); err != nil {
		t.Fatalf("retry: %v", err)
	}
	f.assertRun("SUCCEEDED", "COMPLETE")
	if recs := f.reconciliations(); len(recs) != 1 || recs[0].Status != db.SourceReconciliationMatch {
		t.Fatalf("upsert did not replace the error row: %+v", recs)
	}
}

func TestCommitCountsPlannedBoundsNotWorkerHighWaterMark(t *testing.T) {
	counter := &fakeSourceCounter{rows: 10}
	f := reconcileFixture(t, "recon-bounds", immutableCount("fail"), counter)
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	if len(counter.queries) != 1 {
		t.Fatalf("counted %d times, want once per task", len(counter.queries))
	}
	q := counter.queries[0]
	// The artifact reports MaxHWM 42; the plan's upper bound is 100.
	if q.UpperBound != "100" || !q.UpperInclusive || q.LowerBound != "0" || !q.LowerExclusive {
		t.Fatalf("bounds=%+v", q)
	}
	if q.Table != "dbo.orders" || q.CursorColumn != "id" || q.CursorDomain != connectors.CursorDomainInt64 || q.SourceQuery != "" {
		t.Fatalf("query=%+v", q)
	}
}

func TestCommitCountsQueryModeSourceSQL(t *testing.T) {
	counter := &fakeSourceCounter{rows: 10}
	f := newCommitFixtureWithChecksum(t, "recon-query", false, true, false, func(s *commitFixtureSettings) {
		s.partitionSpec = strings.Replace(reconcileTaskSpec, `"source_mode":"table"`, `"source_mode":"query"`, 1)
		s.jobOptions["source_mode"] = "query"
		s.jobOptions["reconciliation"] = immutableCount("fail")
	})
	f.srv.openSourceCounterFn = func(context.Context, string, string) (sourceRangeCounter, error) { return counter, nil }
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	if len(counter.queries) != 1 || counter.queries[0].SourceQuery != "select 1" {
		t.Fatalf("queries=%+v", counter.queries)
	}
}

func TestCommitDoesNotRecountAfterIntentIsPersisted(t *testing.T) {
	counter := &fakeSourceCounter{rows: 10}
	f := reconcileFixture(t, "recon-retry-intent", immutableCount("fail"), counter)
	f.objects.addFault(faultPut, f.stateKey, 1, failBefore)
	if err := f.finalize(); err == nil {
		t.Fatal("expected injected state failure")
	}
	counter.rows = 500 // the source grew after the first pass
	if err := f.finalize(); err != nil {
		t.Fatalf("retry after durable intent must not re-count: %v", err)
	}
	f.assertRun("SUCCEEDED", "COMPLETE")
	if len(counter.queries) != 1 {
		t.Fatalf("counted %d times", len(counter.queries))
	}
}

func TestCommitRejectsTaskWithoutCountableRangeTerminally(t *testing.T) {
	counter := &fakeSourceCounter{rows: 10}
	f := newCommitFixtureWithChecksum(t, "recon-nospec", false, true, false, func(s *commitFixtureSettings) {
		s.jobOptions["reconciliation"] = immutableCount("fail") // default fixture spec is {"type":"single"}
	})
	f.srv.openSourceCounterFn = func(context.Context, string, string) (sourceRangeCounter, error) { return counter, nil }
	if err := f.finalize(); err == nil || !strings.Contains(err.Error(), "no countable cursor range") {
		t.Fatalf("error=%v", err)
	}
	if run := f.assertRun("FAILED", "FAILED"); run.CommitReconciliationStatus != db.CommitReconciliationTerminal {
		t.Fatalf("projection=%+v", run)
	}
	if len(counter.queries) != 0 {
		t.Fatal("counted a task with no range")
	}
}

func TestClassifySourceCount(t *testing.T) {
	tests := []struct {
		consistency string
		source, out int64
		want        string
	}{
		{"none", 10, 10, db.SourceReconciliationMatch},
		{"immutable", 10, 10, db.SourceReconciliationMatch},
		{"none", 11, 10, db.SourceReconciliationIndeterminate},
		{"immutable", 11, 10, db.SourceReconciliationMismatch},
		{"immutable", 9, 10, db.SourceReconciliationMismatch},
	}
	for _, tc := range tests {
		if got, _ := classifySourceCount(tc.consistency, tc.source, tc.out); got != tc.want {
			t.Errorf("classify(%s,%d,%d)=%s want %s", tc.consistency, tc.source, tc.out, got, tc.want)
		}
	}
}

func TestCommitSkipsTasksWithPersistedMatchOrIndeterminateRows(t *testing.T) {
	for _, status := range []string{db.SourceReconciliationMatch, db.SourceReconciliationIndeterminate} {
		t.Run(status, func(t *testing.T) {
			counter := &fakeSourceCounter{rows: 999}
			f := reconcileFixture(t, "recon-skip-"+status, immutableCount("fail"), counter)
			tasks, err := f.st.ListTasksForRun(f.ctx, f.runID)
			if err != nil {
				t.Fatal(err)
			}
			err = f.st.UpsertSourceReconciliation(f.ctx, db.SourceReconciliation{RunID: f.runID, TaskID: tasks[0].ID, Method: db.SourceReconciliationMethodCount,
				Consistency: "immutable", ObservedAt: "2026-07-22T10:02:00Z", SourceRows: 10, OutputRows: 10, Status: status})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.finalize(); err != nil {
				t.Fatal(err)
			}
			if len(counter.queries) != 0 {
				t.Fatalf("re-counted a task with a persisted %s row", status)
			}
			f.assertRun("SUCCEEDED", "COMPLETE")
		})
	}
}

func TestCommitRecountsPersistedMismatchRow(t *testing.T) {
	counter := &fakeSourceCounter{rows: 10}
	f := reconcileFixture(t, "recon-recount-mismatch", immutableCount("fail"), counter)
	tasks, _ := f.st.ListTasksForRun(f.ctx, f.runID)
	_ = f.st.UpsertSourceReconciliation(f.ctx, db.SourceReconciliation{RunID: f.runID, TaskID: tasks[0].ID, Method: "count", Consistency: "immutable", ObservedAt: "2026-07-22T10:02:00Z", SourceRows: 15, OutputRows: 10, Status: db.SourceReconciliationMismatch})
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	if len(counter.queries) != 1 {
		t.Fatalf("counts=%d", len(counter.queries))
	}
}

func TestPersistedMismatchStillBlocksWithoutRecount(t *testing.T) {
	// A mismatch is always re-counted, so only match/indeterminate are skipped.
	if skipPersistedSourceCount(db.SourceReconciliationMismatch) || skipPersistedSourceCount(db.SourceReconciliationError) {
		t.Fatal("mismatch and error rows must be re-counted")
	}
}

type blockingSourceCounter struct{}

func (blockingSourceCounter) CountRange(ctx context.Context, _ connectors.CursorQuery) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}
func (blockingSourceCounter) Close() error { return nil }

func TestSlowCountFailsOnItsOwnTimeoutBeforeParentDeadline(t *testing.T) {
	f := reconcileFixture(t, "recon-slow", immutableCount("fail"), &fakeSourceCounter{})
	f.srv.openSourceCounterFn = func(context.Context, string, string) (sourceRangeCounter, error) { return blockingSourceCounter{}, nil }
	ctx, cancel := context.WithTimeout(f.ctx, 400*time.Millisecond)
	defer cancel()
	err := f.srv.commitRun(ctx, f.runID)
	if err == nil || !strings.Contains(err.Error(), "source reconciliation") {
		t.Fatalf("error=%v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("parent context expired before the count failed on its own timeout")
	}
	recs := f.reconciliations()
	if len(recs) != 1 || recs[0].Status != db.SourceReconciliationError {
		t.Fatalf("records=%+v", recs)
	}
}
