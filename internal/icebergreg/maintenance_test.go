package icebergreg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergpart"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// localKeys treats local file paths as object keys, so the tests run against
// the local file system the way production runs against a bucket.
func localKeys(path string) (string, bool) { return path, true }

// maintenanceTestTable is a partitioned table in the in-memory catalog with a
// loader that always returns the catalog's current version, like LoadTable.
type maintenanceTestTable struct {
	catalog *optionTestCatalog
	reg     RunConfig
	spec    icebergpart.Spec
}

func newMaintenanceTestTable(t *testing.T) *maintenanceTestTable {
	t.Helper()
	fields := []PartitionFieldConfig{{Source: "region", Transform: "identity"}}
	schema := partitionTestTableSchema()
	spec, err := buildPartitionSpec(schema, fields)
	if err != nil {
		t.Fatal(err)
	}
	location := t.TempDir()
	metadata, err := icetable.NewMetadata(schema, &spec, icetable.UnsortedSortOrder, location, iceberg.Properties{"format-version": "2"})
	if err != nil {
		t.Fatal(err)
	}
	return &maintenanceTestTable{
		catalog: &optionTestCatalog{metadata: metadata, metadataLocation: location + "/metadata/v1.json"},
		reg:     partitionTestReg(fields),
		spec:    icebergpart.Spec{{Source: "region"}},
	}
}

func (m *maintenanceTestTable) load(context.Context) (*icetable.Table, error) {
	return icetable.New(icetable.Identifier{"analytics", "orders"}, m.catalog.metadata, m.catalog.metadataLocation, func(context.Context) (iceio.IO, error) {
		return iceio.LocalFS{}, nil
	}, m.catalog), nil
}

func (m *maintenanceTestTable) current(t *testing.T) *icetable.Table {
	t.Helper()
	tbl, _ := m.load(context.Background())
	return tbl
}

// register publishes rows as one run would: worker files, then a
// registration commit (append or full refresh).
func (m *maintenanceTestTable) register(t *testing.T, fullRefresh bool, rows []partitionTestRow) []string {
	t.Helper()
	paths := writeWorkerFiles(t, m.spec, "UTC", rows)
	if _, err := registerTestFiles(t, m.current(t), m.reg, fullRefresh, paths); err != nil {
		t.Fatal(err)
	}
	return paths
}

func objectsFor(paths []string, modified time.Time) []s3io.ObjectInfo {
	out := make([]s3io.ObjectInfo, len(paths))
	for i, p := range paths {
		out[i] = s3io.ObjectInfo{Key: p, Size: 100, LastModified: modified}
	}
	return out
}

func TestTableReferencesKeepFilesOfEveryRetainedSnapshot(t *testing.T) {
	ctx := context.Background()
	m := newMaintenanceTestTable(t)
	first := m.register(t, false, partitionTestRows[:2])
	// A full refresh removes first from the current snapshot, but the
	// previous snapshot still references it.
	second := m.register(t, true, partitionTestRows[2:])
	// A registration that commits after the cleanup started.
	concurrent := m.register(t, false, []partitionTestRow{{9, "eu", "2024-01-05T00:00:00Z"}})

	refs, snapshots, err := TableReferences(ctx, m.current(t), localKeys)
	if err != nil {
		t.Fatal(err)
	}
	if snapshots != 3 {
		t.Fatalf("snapshots=%d want 3", snapshots)
	}
	for _, p := range append(append(append([]string{}, first...), second...), concurrent...) {
		if _, ok := refs[p]; !ok {
			t.Fatalf("%s is referenced by a retained snapshot but missing from references", p)
		}
	}

	stray := writeWorkerFiles(t, nil, "UTC", partitionTestRows[:1])
	tablePrefix := commonDir(append(first, stray...))
	all := append(append(append(append([]string{}, first...), second...), concurrent...), stray...)
	decisions, referenced, _ := ClassifyOrphans(objectsFor(all, time.Now().Add(-30*24*time.Hour)), OrphanPolicy{
		TableKeyPrefix: tablePrefix, MinAge: time.Hour, Now: time.Now(), References: refs, Snapshots: snapshots,
		RunState: func(string) RunState { return RunStateRegistered },
	})
	if referenced != len(all)-1 || len(decisions) != 1 || decisions[0].Object.Key != stray[0] || !decisions[0].Orphan {
		t.Fatalf("referenced=%d decisions=%+v", referenced, decisions)
	}

	// Once the old snapshots expire, the replaced files become orphans.
	tx := m.current(t).NewTransaction()
	if err := tx.ExpireSnapshots(icetable.WithOlderThan(0), icetable.WithRetainLast(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	refs, _, err = TableReferences(ctx, m.current(t), localKeys)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range first {
		if _, ok := refs[p]; ok {
			t.Fatalf("%s is still referenced after its snapshot expired", p)
		}
	}
	for _, p := range append(append([]string{}, second...), concurrent...) {
		if _, ok := refs[p]; !ok {
			t.Fatalf("live file %s lost its reference", p)
		}
	}
}

func commonDir(paths []string) string {
	prefix := paths[0]
	for _, p := range paths[1:] {
		for !strings.HasPrefix(p, prefix) {
			prefix = prefix[:len(prefix)-1]
		}
	}
	return strings.TrimSuffix(prefix[:strings.LastIndex(prefix, "/")+1], "/")
}

func TestClassifyOrphansKeepsEverythingNotProvablyOrphan(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	old, young := now.Add(-10*24*time.Hour), now.Add(-time.Minute)
	states := map[string]RunState{
		"registered": RunStateRegistered, "failed": RunStateFailed, "active": RunStateActive,
		"pending": RunStateRegistrationPending, "plain": RunStateNotRegistered, "canceled": RunStateCanceled,
	}
	run := func(id string) string { return "t/_runs/run-" + id + "/part-000001-000.parquet" }
	objects := []s3io.ObjectInfo{
		{Key: run("registered"), LastModified: old},
		{Key: run("failed"), LastModified: old},
		{Key: run("active"), LastModified: old},
		{Key: run("pending"), LastModified: old},
		{Key: run("plain"), LastModified: old},
		{Key: run("canceled"), LastModified: old},
		{Key: run("unknown"), LastModified: old},
		{Key: "t/_runs/run-registered/part-000002-000.parquet", LastModified: young},
		{Key: "t/data/00000-0-abc.parquet", LastModified: old},
		{Key: "t/data/live.parquet", LastModified: old},
		{Key: "t/metadata/snap-1.avro", LastModified: old},
		{Key: "t/metadata/00001.metadata.json", LastModified: old},
		{Key: "t/_state.json", LastModified: old},
		{Key: "t/_staging/x.parquet", LastModified: old},
		{Key: "other/_runs/run-registered/part-000001-000.parquet", LastModified: old},
	}
	decisions, referenced, skipped := ClassifyOrphans(objects, OrphanPolicy{
		TableKeyPrefix: "t", MinAge: 7 * 24 * time.Hour, Now: now,
		References: map[string]int64{"t/data/live.parquet": 1}, Snapshots: 1,
		RunState: func(id string) RunState {
			if s, ok := states[id]; ok {
				return s
			}
			return RunStateUnknown
		},
	})
	got := map[string]string{}
	for _, d := range decisions {
		verdict := "keep:" + d.Reason
		if d.Orphan {
			verdict = "orphan:" + d.Reason
		}
		got[d.Object.Key] = verdict
	}
	want := map[string]string{
		run("registered"): "orphan:UNREFERENCED_REGISTERED_RUN",
		run("failed"):     "orphan:UNREFERENCED_FAILED_RUN",
		run("active"):     "keep:RUN_ACTIVE",
		run("pending"):    "keep:REGISTRATION_NOT_FINISHED",
		run("plain"):      "keep:RUN_PUBLISHED_WITHOUT_ICEBERG",
		run("canceled"):   "keep:OWNED_BY_CANCELED_OBJECT_CLEANUP",
		run("unknown"):    "keep:RUN_UNKNOWN",
		"t/_runs/run-registered/part-000002-000.parquet": "keep:YOUNGER_THAN_MIN_AGE",
		"t/data/00000-0-abc.parquet":                     "orphan:UNREFERENCED_TABLE_FILE",
		"t/_staging/x.parquet":                           "keep:STAGING_OBJECT",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("decisions:\n got %v\nwant %v", got, want)
	}
	// metadata/, non-Parquet objects and keys outside the table are never
	// candidates.
	if referenced != 1 || skipped != 4 {
		t.Fatalf("referenced=%d skipped=%d", referenced, skipped)
	}
	for _, d := range decisions {
		if d.Evidence["referenced"] != false || d.Evidence["snapshots_examined"] != 1 {
			t.Fatalf("missing reference evidence: %+v", d)
		}
	}
}

func TestPlanCompactionGroupsSmallFilesByPartitionAndSchema(t *testing.T) {
	f := func(key, partition string, size int64) CompactionFile {
		return CompactionFile{Path: "s3://b/" + key, Key: key, Size: size, Records: 1, Partition: partition}
	}
	files := []CompactionFile{
		f("a1", "eu", 10), f("a2", "eu", 10), f("a3", "eu", 10), f("a4", "eu", 10), f("a5", "eu", 10),
		f("b1", "us", 10), f("b2", "us", 10),
		f("c1", "eu", 10), // other schema, alone
		f("big", "eu", 500),
		f("foreign", "eu", 10),
	}
	fps := map[string]string{"a1": "x", "a2": "x", "a3": "x", "a4": "x", "a5": "x", "b1": "x", "b2": "x", "c1": "y", "big": "x"}
	groups, skips := PlanCompaction(files, fps, CompactionOptions{TargetFileBytes: 25, SmallFileBytes: 100, MinInputFiles: 2, MaxInputFiles: 100})

	var gotGroups []string
	for _, g := range groups {
		var keys []string
		for _, in := range g.Inputs {
			keys = append(keys, in.Key)
		}
		gotGroups = append(gotGroups, g.Partition+":"+strings.Join(keys, ","))
	}
	// eu/x is packed into tasks of about 25 bytes; a5 alone is too few.
	wantGroups := []string{"eu:a1,a2", "eu:a3,a4", "us:b1,b2"}
	if fmt.Sprint(gotGroups) != fmt.Sprint(wantGroups) {
		t.Fatalf("groups=%v want %v", gotGroups, wantGroups)
	}
	gotSkips := map[string]string{}
	for _, s := range skips {
		gotSkips[s.File.Key] = s.Reason
	}
	wantSkips := map[string]string{"a5": "TOO_FEW_FILES_IN_PARTITION", "c1": "TOO_FEW_FILES_IN_PARTITION", "big": "NOT_SMALL", "foreign": "NOT_WRITTEN_BY_ORABBIT"}
	if fmt.Sprint(gotSkips) != fmt.Sprint(wantSkips) {
		t.Fatalf("skips=%v want %v", gotSkips, wantSkips)
	}

	limited, _ := PlanCompaction(files, fps, CompactionOptions{TargetFileBytes: 1000, SmallFileBytes: 100, MinInputFiles: 2, MaxInputFiles: 3})
	total := 0
	for _, g := range limited {
		total += len(g.Inputs)
	}
	if total > 3 {
		t.Fatalf("max_input_files exceeded: %d", total)
	}
}

// compactionFixture registers four small runs (eight files, two partitions)
// and writes the outputs a compaction run's workers would.
func compactionFixture(t *testing.T) (m *maintenanceTestTable, inputs, outputs []string, rows []partitionTestRow) {
	t.Helper()
	m = newMaintenanceTestTable(t)
	for i := 0; i < 4; i++ {
		run := []partitionTestRow{
			{int64(10*i + 1), "eu", "2024-01-01T00:00:00Z"},
			{int64(10*i + 2), "us", "2024-01-01T00:00:00Z"},
		}
		m.register(t, false, run)
		rows = append(rows, run...)
	}
	files, err := CompactionFiles(context.Background(), m.current(t), localKeys)
	if err != nil {
		t.Fatal(err)
	}
	fps := map[string]string{}
	for _, f := range files {
		fps[f.Key] = "fp"
	}
	groups, _ := PlanCompaction(files, fps, CompactionOptions{TargetFileBytes: 1 << 30, SmallFileBytes: 1 << 30, MinInputFiles: 2})
	if len(groups) != 2 {
		t.Fatalf("groups=%d want one per partition", len(groups))
	}
	for _, g := range groups {
		for _, in := range g.Inputs {
			inputs = append(inputs, in.Path)
		}
	}
	outputs = writeWorkerFiles(t, m.spec, "UTC", rows)
	return m, inputs, outputs, rows
}

func snapshotCount(tbl *icetable.Table) int { return len(tbl.Metadata().Snapshots()) }

func TestCommitCompactionReplacesSmallFilesAndKeepsHistory(t *testing.T) {
	ctx := context.Background()
	m, inputs, outputs, rows := compactionFixture(t)
	before := snapshotCount(m.current(t))

	committed, err := CommitCompaction(ctx, m.load, m.reg, "run-c", inputs, outputs, int64(len(rows)), map[string]string{"orabbit.commit_id": "c1"})
	if err != nil || committed {
		t.Fatalf("committed=%v err=%v", committed, err)
	}
	tbl := m.current(t)
	got, count := partitionsByPath(t, tbl)
	samePaths(t, got, outputs)
	if count != int64(len(rows)) {
		t.Fatalf("rows=%d want %d", count, len(rows))
	}
	parts := map[string]bool{}
	for _, p := range got {
		parts[p] = true
	}
	if !parts["region=eu"] || !parts["region=us"] || len(parts) != 2 {
		t.Fatalf("partitions=%v", parts)
	}
	if snapshotCount(tbl) != before+1 {
		t.Fatalf("snapshots=%d want %d: compaction must add one snapshot and keep history", snapshotCount(tbl), before+1)
	}
	if props := tbl.CurrentSnapshot().Summary.Properties; props[operationProperty] != CompactionOperation || props["orabbit.run_id"] != "run-c" {
		t.Fatalf("compaction snapshot properties=%v", props)
	}
	// The replaced inputs stay referenced by the history until it expires.
	refs, _, err := TableReferences(ctx, tbl, localKeys)
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range inputs {
		if _, ok := refs[in]; !ok {
			t.Fatalf("input %s lost its history reference", in)
		}
	}

	// A retry after a crash finds its own snapshot and changes nothing.
	again, err := CommitCompaction(ctx, m.load, m.reg, "run-c", inputs, outputs, int64(len(rows)), nil)
	if err != nil || !again || snapshotCount(m.current(t)) != before+1 {
		t.Fatalf("retry committed=%v err=%v snapshots=%d", again, err, snapshotCount(m.current(t)))
	}
}

func TestCommitCompactionKeepsAConcurrentAppend(t *testing.T) {
	m, inputs, outputs, rows := compactionFixture(t)
	// A registration lands between planning and committing the compaction.
	late := m.register(t, false, []partitionTestRow{{99, "eu", "2024-01-02T00:00:00Z"}})

	if _, err := CommitCompaction(context.Background(), m.load, m.reg, "run-c", inputs, outputs, int64(len(rows)), nil); err != nil {
		t.Fatal(err)
	}
	got, count := partitionsByPath(t, m.current(t))
	samePaths(t, got, append(append([]string{}, outputs...), late...))
	if count != int64(len(rows))+1 {
		t.Fatalf("rows=%d want %d", count, len(rows)+1)
	}
}

func TestCommitCompactionAbortsWithoutCommitting(t *testing.T) {
	ctx := context.Background()
	t.Run("inputs replaced by a full refresh", func(t *testing.T) {
		m, inputs, outputs, rows := compactionFixture(t)
		m.register(t, true, partitionTestRows)
		before := snapshotCount(m.current(t))
		_, err := CommitCompaction(ctx, m.load, m.reg, "run-c", inputs, outputs, int64(len(rows)), nil)
		var f *failure.Failure
		if !errors.As(err, &f) || f.Class != failure.FailureCatalogConflict || f.Retryable {
			t.Fatalf("err=%v", err)
		}
		if snapshotCount(m.current(t)) != before {
			t.Fatal("an aborted compaction must not commit")
		}
	})
	t.Run("outputs lose rows", func(t *testing.T) {
		m, inputs, outputs, rows := compactionFixture(t)
		before := snapshotCount(m.current(t))
		_, err := CommitCompaction(ctx, m.load, m.reg, "run-c", inputs, outputs, int64(len(rows))-1, nil)
		var f *failure.Failure
		if !errors.As(err, &f) || f.Class != failure.FailureDataIntegrity {
			t.Fatalf("err=%v", err)
		}
		if snapshotCount(m.current(t)) != before {
			t.Fatal("a compaction that loses rows must not commit")
		}
	})
}

func TestCompactionFilesRefusesTablesWithDeleteFiles(t *testing.T) {
	ctx := context.Background()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz},
	)
	tbl := newOptionTestTable(t, schema)
	path := writeWorkerFiles(t, nil, "UTC", partitionTestRows)[0]
	reader, err := newParquetRecordReader(ctx, iceio.LocalFS{}, []string{path})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	defer os.Remove(path)
	// Rows appended by iceberg-go itself get position deletes from a
	// merge-on-read delete.
	tx := tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{icetable.WriteDeleteModeKey: icetable.WriteModeMergeOnRead}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Append(ctx, reader, nil); err != nil {
		t.Fatal(err)
	}
	tbl, err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx = tbl.NewTransaction()
	if err := tx.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil); err != nil {
		t.Fatal(err)
	}
	if tbl, err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = CompactionFiles(ctx, tbl, localKeys)
	if err == nil || !strings.Contains(err.Error(), "delete files") {
		t.Fatalf("err=%v", err)
	}
}

func TestObjectKey(t *testing.T) {
	for path, want := range map[string]string{
		"s3://bucket/a/b.parquet":  "a/b.parquet",
		"s3a://bucket/a/b.parquet": "a/b.parquet",
		"s3://other/a/b.parquet":   "",
		"/local/a.parquet":         "",
	} {
		got, ok := ObjectKey(path, "bucket")
		if got != want || ok != (want != "") {
			t.Fatalf("%s: key=%q ok=%v", path, got, ok)
		}
	}
}
