package icebergreg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// iceTestCatalog is the in-memory catalog with a switch that makes every
// commit fail, as a catalog outage or a lost commit race would.
type iceTestCatalog struct {
	optionTestCatalog
	commitErr error
	commits   int
}

func (c *iceTestCatalog) CommitTable(ctx context.Context, ident icetable.Identifier, requirements []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	if c.commitErr != nil {
		return nil, "", c.commitErr
	}
	c.commits++
	return c.optionTestCatalog.CommitTable(ctx, ident, requirements, updates)
}

func (c *iceTestCatalog) table() *icetable.Table {
	return icetable.New(icetable.Identifier{"analytics", "orders"}, c.metadata, c.metadataLocation, func(context.Context) (iceio.IO, error) {
		return iceio.LocalFS{}, nil
	}, c)
}

func newIceTestCatalog(t *testing.T, fields []PartitionFieldConfig) *iceTestCatalog {
	t.Helper()
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
	return &iceTestCatalog{optionTestCatalog: optionTestCatalog{metadata: metadata, metadataLocation: location + "/metadata/v1.json"}}
}

// seedIceTable appends rows the way an earlier ice insert would have, so the
// table has a current snapshot to refresh.
func seedIceTable(t *testing.T, cat *iceTestCatalog, rows []partitionTestRow) []string {
	t.Helper()
	ctx := context.Background()
	paths := writeWorkerFiles(t, nil, "UTC", rows)
	tx := cat.table().NewTransaction()
	if err := tx.AddFiles(ctx, paths, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return paths
}

func iceFullRefreshRequest() RunRequest {
	return RunRequest{
		RunID:             "run-2",
		Incremental:       false,
		RegistrationID:    "reg-2",
		CommitID:          strings.Repeat("c", 64),
		ArtifactSetDigest: strings.Repeat("d", 64),
		ManifestKey:       "prefix/_manifests/run-2.json",
	}
}

func iceFullRefreshReg() RunConfig {
	return RunConfig{Enabled: true, Engine: "ice", Table: "analytics.orders"}.Normalize()
}

// managerWithoutIce is a manager whose ice binary fails the test if it runs.
func managerWithoutIce(t *testing.T) (*Manager, *int) {
	t.Helper()
	calls := 0
	m := NewManager(slog.New(slog.DiscardHandler))
	m.execCommand = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		calls++
		t.Errorf("ice must not run for this registration: %s %v", name, args)
		return exec.CommandContext(ctx, "false")
	}
	return m, &calls
}

func liveFilePaths(t *testing.T, tbl *icetable.Table) []string {
	t.Helper()
	files, err := currentDataFiles(context.Background(), tbl)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(files))
	for i, df := range files {
		out[i] = df.FilePath()
	}
	sort.Strings(out)
	return out
}

func scannedRows(t *testing.T, tbl *icetable.Table) int64 {
	t.Helper()
	scanned, err := tbl.Scan(icetable.WitMaxConcurrency(1)).ToArrowTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer scanned.Release()
	return scanned.NumRows()
}

// manifestRows counts the live rows from the manifests. iceberg-go v0.5
// cannot scan zoned timestamps outside UTC (other engines read them), so the
// non-UTC cases check the registration from metadata.
func manifestRows(t *testing.T, tbl *icetable.Table) int64 {
	t.Helper()
	files, err := currentDataFiles(context.Background(), tbl)
	if err != nil {
		t.Fatal(err)
	}
	var rows int64
	for _, df := range files {
		rows += df.Count()
	}
	return rows
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func TestIceFullRefreshReplacesLiveFilesInOneSnapshotWithoutIce(t *testing.T) {
	cat := newIceTestCatalog(t, nil)
	seedIceTable(t, cat, partitionTestRows[:2])
	before := cat.table()
	previous := before.CurrentSnapshot().SnapshotID
	newFiles := writeWorkerFiles(t, nil, "UTC", partitionTestRows[2:])
	m, iceCalls := managerWithoutIce(t)
	commitsBefore := cat.commits

	err := m.registerIceTable(context.Background(), before, iceFullRefreshRequest(), iceFullRefreshReg(), "analytics.orders", s3io.Config{}, newFiles)
	if err != nil {
		t.Fatal(err)
	}

	after := cat.table()
	if *iceCalls != 0 {
		t.Fatalf("ice ran %d times", *iceCalls)
	}
	if cat.commits-commitsBefore != 1 {
		t.Fatalf("catalog commits=%d want exactly one atomic commit", cat.commits-commitsBefore)
	}
	if got, want := liveFilePaths(t, after), sortedCopy(newFiles); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("live files=%v want only the refreshed files %v", got, want)
	}
	if got := scannedRows(t, after); got != int64(len(partitionTestRows[2:])) {
		t.Fatalf("rows=%d want %d", got, len(partitionTestRows[2:]))
	}
	if after.Metadata().SnapshotByID(previous) == nil {
		t.Fatal("the previous snapshot must stay in history until retention expires it")
	}
	if after.CurrentSnapshot().ParentSnapshotID == nil || *after.CurrentSnapshot().ParentSnapshotID != previous {
		t.Fatal("the refresh must commit on top of the previous snapshot, not into a recreated table")
	}
	props := after.CurrentSnapshot().Summary.Properties
	for key, want := range (OperationIdentity{RegistrationID: "reg-2", RunID: "run-2", CommitID: strings.Repeat("c", 64), ArtifactSetDigest: strings.Repeat("d", 64), ManifestKey: "prefix/_manifests/run-2.json"}).Properties() {
		if props[key] != want {
			t.Fatalf("snapshot summary %s=%q want %q", key, props[key], want)
		}
	}
}

func TestIceFullRefreshCommitFailureKeepsPreviousSnapshotReadable(t *testing.T) {
	cat := newIceTestCatalog(t, nil)
	old := seedIceTable(t, cat, partitionTestRows[:2])
	before := cat.table()
	previous := before.CurrentSnapshot().SnapshotID
	newFiles := writeWorkerFiles(t, nil, "UTC", partitionTestRows[2:])
	m, _ := managerWithoutIce(t)
	cat.commitErr = errors.New("catalog unavailable")

	err := m.registerIceTable(context.Background(), before, iceFullRefreshRequest(), iceFullRefreshReg(), "analytics.orders", s3io.Config{}, newFiles)
	if err == nil || !strings.Contains(err.Error(), "catalog unavailable") {
		t.Fatalf("err=%v want the commit failure", err)
	}

	cat.commitErr = nil
	after := cat.table()
	if after.CurrentSnapshot() == nil || after.CurrentSnapshot().SnapshotID != previous {
		t.Fatal("a failed refresh must leave the previous snapshot current")
	}
	if got, want := liveFilePaths(t, after), sortedCopy(old); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("live files=%v want the previous files %v", got, want)
	}
	if got := scannedRows(t, after); got != 2 {
		t.Fatalf("rows=%d want the previous 2 rows", got)
	}
}

func TestIceFullRefreshAfterCommitIsReconciledAsExactlyCommitted(t *testing.T) {
	// The catalog commit succeeded but the receipt was never persisted (master
	// crashed after the external commit boundary). The retry must find the
	// operation in the catalog instead of committing it twice.
	cat := newIceTestCatalog(t, nil)
	seedIceTable(t, cat, partitionTestRows[:2])
	newFiles := writeWorkerFiles(t, nil, "UTC", partitionTestRows[2:])
	m, _ := managerWithoutIce(t)
	req := iceFullRefreshRequest()
	if err := m.registerIceTable(context.Background(), cat.table(), req, iceFullRefreshReg(), "analytics.orders", s3io.Config{}, newFiles); err != nil {
		t.Fatal(err)
	}

	obs := observeTestTable(t, cat.table())
	expected := make([]ExpectedFile, len(newFiles))
	for i, p := range newFiles {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		// writeWorkerFiles without a spec writes every row into one file.
		expected[i] = ExpectedFile{Path: testObjectURI(p), Size: st.Size(), Records: int64(len(partitionTestRows[2:]))}
	}
	identity := OperationIdentity{RegistrationID: req.RegistrationID, RunID: req.RunID, CommitID: req.CommitID, ArtifactSetDigest: req.ArtifactSetDigest, ManifestKey: req.ManifestKey}
	decision, err := DecideReconciliation(identity, expected, obs)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Outcome != OutcomeExactlyCommitted {
		t.Fatalf("outcome=%s reason=%s want %s", decision.Outcome, decision.Reason, OutcomeExactlyCommitted)
	}
	if len(obs.Snapshots) != 2 {
		t.Fatalf("snapshots=%d want the seed and the refresh", len(obs.Snapshots))
	}
}

func TestIceFullRefreshOfPartitionedTableIsRejectedWithoutCommitting(t *testing.T) {
	fields := []PartitionFieldConfig{{Source: "region", Transform: "identity"}}
	for name, reg := range map[string]RunConfig{
		"run sets partition_spec":   RunConfig{Enabled: true, Engine: "ice", Table: "analytics.orders", PartitionSpec: fields}.Normalize(),
		"table is partitioned only": iceFullRefreshReg(),
	} {
		t.Run(name, func(t *testing.T) {
			cat := newIceTestCatalog(t, fields)
			seedPartitioned := writeWorkerFiles(t, nil, "UTC", partitionTestRows[:1])
			tx := cat.table().NewTransaction()
			if err := tx.AddFiles(context.Background(), seedPartitioned, nil, false); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			before := cat.table()
			previous := before.CurrentSnapshot().SnapshotID
			commitsBefore := cat.commits
			m, iceCalls := managerWithoutIce(t)

			err := m.registerIceTable(context.Background(), before, iceFullRefreshRequest(), reg, "analytics.orders", s3io.Config{}, writeWorkerFiles(t, nil, "UTC", partitionTestRows[1:2]))

			var f *failure.Failure
			if !errors.As(err, &f) || f.Class != failure.FailureConfigurationInvalid || f.Retryable || !f.DefiniteRejection {
				t.Fatalf("err=%v want a definite, non-retryable configuration failure", err)
			}
			if !strings.Contains(err.Error(), "engine=rest-go") {
				t.Fatalf("err=%v must name the supported engine", err)
			}
			// The check runs after the external commit boundary; it must stay a
			// definite rejection there, not become an ambiguous outcome.
			if got := ClassifyFailure(err, true); got.Class != failure.FailureConfigurationInvalid || got.Retryable {
				t.Fatalf("classified after the boundary as %s retryable=%t", got.Class, got.Retryable)
			}
			if *iceCalls != 0 || cat.commits != commitsBefore || cat.table().CurrentSnapshot().SnapshotID != previous {
				t.Fatal("a rejected refresh must not touch the table")
			}
		})
	}
}

func TestIceFullRefreshAcceptsNonUTCTimestampFiles(t *testing.T) {
	cat := newIceTestCatalog(t, nil)
	seedIceTable(t, cat, partitionTestRows[:2])
	newFiles := writeWorkerFiles(t, nil, "Asia/Yerevan", partitionTestRows[2:])
	m, _ := managerWithoutIce(t)

	err := m.registerIceTable(context.Background(), cat.table(), iceFullRefreshRequest(), iceFullRefreshReg(), "analytics.orders", s3io.Config{}, newFiles)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := liveFilePaths(t, cat.table()), sortedCopy(newFiles); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("live files=%v want %v", got, want)
	}
	if got := manifestRows(t, cat.table()); got != int64(len(partitionTestRows[2:])) {
		t.Fatalf("rows=%d want %d", got, len(partitionTestRows[2:]))
	}
}

func TestUnpartitionedAppendAcceptsNonUTCTimestampFiles(t *testing.T) {
	cat := newIceTestCatalog(t, nil)
	seedIceTable(t, cat, partitionTestRows[:2])
	newFiles := writeWorkerFiles(t, nil, "Asia/Yerevan", partitionTestRows[2:])
	reg := RunConfig{Enabled: true, Engine: "rest-go", Table: "analytics.orders"}.Normalize()
	req := RunRequest{RunID: "run-3", Incremental: true, WriteMode: "append"}

	if err := commitRESTGoFiles(context.Background(), slog.New(slog.DiscardHandler), cat.table(), req, reg, "analytics.orders", newFiles); err != nil {
		t.Fatal(err)
	}
	if got := manifestRows(t, cat.table()); got != int64(len(partitionTestRows)) {
		t.Fatalf("rows=%d want %d", got, len(partitionTestRows))
	}
}

func TestIceFirstLoadAndIncrementalStillUseIceInsert(t *testing.T) {
	for name, tc := range map[string]struct {
		seed bool
		req  RunRequest
	}{
		"full refresh into an empty table": {seed: false, req: iceFullRefreshRequest()},
		"incremental append":               {seed: true, req: RunRequest{RunID: "run-3", Incremental: true, WriteMode: "append"}},
	} {
		t.Run(name, func(t *testing.T) {
			cat := newIceTestCatalog(t, nil)
			if tc.seed {
				seedIceTable(t, cat, partitionTestRows[:1])
			}
			commitsBefore := cat.commits
			var iceArgs []string
			m := NewManager(slog.New(slog.DiscardHandler))
			m.execCommand = func(ctx context.Context, name string, args ...string) *exec.Cmd {
				iceArgs = args
				return exec.CommandContext(ctx, "sh", "-c", "cat >/dev/null")
			}

			err := m.registerIceTable(context.Background(), cat.table(), tc.req, iceTestRunConfig(), "analytics.orders", s3io.Config{}, []string{"s3://bucket/prefix/part-1.parquet"})
			if err != nil {
				t.Fatal(err)
			}
			if len(iceArgs) == 0 || !containsArg(iceArgs, "insert") {
				t.Fatalf("ice args=%v want an ice insert", iceArgs)
			}
			if cat.commits != commitsBefore {
				t.Fatal("the master must not commit to the catalog when ice inserts")
			}
		})
	}
}

func TestIsFullRefreshRequest(t *testing.T) {
	for _, tc := range []struct {
		incremental bool
		mode        string
		want        bool
	}{
		{false, "", true},
		{false, "append", true},
		{true, "append", false},
		{true, "", false},
		{true, "overwrite", true},
		{true, "OVERWRITE", true},
	} {
		if got := isFullRefreshRequest(RunRequest{Incremental: tc.incremental, WriteMode: tc.mode}); got != tc.want {
			t.Fatalf("incremental=%t mode=%q full=%t want %t", tc.incremental, tc.mode, got, tc.want)
		}
	}
}

// testObjectURI gives a local test file the bucket-style URI reconciliation
// compares, as an S3 key would have in production.
func testObjectURI(localPath string) string {
	return "s3://local-test" + localPath
}

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

// observeTestTable builds the stable observation InspectCatalog builds from a
// REST catalog, from the in-memory table.
func observeTestTable(t *testing.T, tbl *icetable.Table) CatalogObservation {
	t.Helper()
	fs, err := tbl.FS(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	obs := CatalogObservation{Backend: "rest-go", TableExists: true, TableIdentifier: "analytics.orders", MetadataStart: tbl.MetadataLocation(), SchemaCompatible: true, LocationCompatible: true}
	for _, snap := range tbl.Metadata().Snapshots() {
		se := SnapshotEvidence{ID: strconv.FormatInt(snap.SnapshotID, 10), Summary: map[string]string{}}
		if snap.Summary != nil {
			for k, v := range snap.Summary.Properties {
				se.Summary[k] = v
			}
		}
		manifests, err := snap.Manifests(fs)
		if err != nil {
			t.Fatal(err)
		}
		for _, manifest := range manifests {
			entries, err := manifest.FetchEntries(fs, false)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				df := entry.DataFile()
				size, records := df.FileSizeBytes(), df.Count()
				se.Files = append(se.Files, ObservedFile{Path: testObjectURI(df.FilePath()), Size: &size, Records: &records, SnapshotID: se.ID})
			}
		}
		obs.Snapshots = append(obs.Snapshots, se)
	}
	return finalizeCatalogObservation(obs, tbl.MetadataLocation())
}
