package icebergreg

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergpart"
	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"
)

type partitionTestRow struct {
	id     int64
	region string
	ts     string // RFC 3339
}

func partitionTestTableSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz},
	)
}

func newPartitionedTestTable(t *testing.T, fields []PartitionFieldConfig) *icetable.Table {
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
	catalog := &optionTestCatalog{metadata: metadata, metadataLocation: location + "/metadata/v1.json"}
	return icetable.New(icetable.Identifier{"analytics", "orders"}, metadata, catalog.metadataLocation, func(context.Context) (iceio.IO, error) {
		return iceio.LocalFS{}, nil
	}, catalog)
}

func partitionTestRecord(t *testing.T, tz string, rows []partitionTestRow) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: tz}, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, r := range rows {
		ts, err := time.Parse(time.RFC3339, r.ts)
		if err != nil {
			t.Fatal(err)
		}
		b.Field(0).(*array.Int64Builder).Append(r.id)
		b.Field(1).(*array.StringBuilder).Append(r.region)
		b.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(ts.UnixMicro()))
	}
	return b.NewRecordBatch()
}

// writeWorkerFiles writes rows the way a worker does: one file per partition
// tuple, each with its contract in the footer. With spec nil it writes one
// plain file, as a worker older than protocol 8 would.
func writeWorkerFiles(t *testing.T, spec icebergpart.Spec, tz string, rows []partitionTestRow) []string {
	t.Helper()
	rec := partitionTestRecord(t, tz, rows)
	defer rec.Release()
	write := func(r arrow.RecordBatch, footer string) string {
		w, path, err := parquetio.NewTempFileWriterInDir(r.Schema(), parquetio.Options{}, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if footer != "" {
			if err := w.AppendKeyValueMetadata(icebergpart.FooterKey, footer); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if spec == nil {
		return []string{write(rec, "")}
	}
	p, err := icebergpart.NewPartitioner(spec)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := p.Split(rec)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, g := range groups {
		part, err := icebergpart.SelectRows(context.Background(), rec, g.Rows)
		if err != nil {
			t.Fatal(err)
		}
		footer, err := g.Contract.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, write(part, footer))
		part.Release()
	}
	return paths
}

func partitionTestReg(fields []PartitionFieldConfig) RunConfig {
	return RunConfig{Enabled: true, Engine: "rest-go", Table: "analytics.orders", PartitionSpec: fields}.Normalize()
}

func registerTestFiles(t *testing.T, tbl *icetable.Table, reg RunConfig, fullRefresh bool, paths []string) (*icetable.Table, error) {
	t.Helper()
	ctx := context.Background()
	var existing []iceberg.DataFile
	if fullRefresh {
		var err error
		if existing, err = currentDataFiles(ctx, tbl); err != nil {
			t.Fatal(err)
		}
	}
	tx := tbl.NewTransaction()
	if err := registerPartitionedFiles(ctx, tx, reg, "analytics.orders", existing, fullRefresh, paths, iceberg.Properties{"orabbit.run-id": "run-1"}); err != nil {
		return nil, err
	}
	return tx.Commit(ctx)
}

// partitionsByPath returns each registered file's partition as
// "name=value" pairs, plus the table's total row count from a scan.
func partitionsByPath(t *testing.T, tbl *icetable.Table) (map[string]string, int64) {
	t.Helper()
	ctx := context.Background()
	files, err := currentDataFiles(ctx, tbl)
	if err != nil {
		t.Fatal(err)
	}
	spec := tbl.Spec()
	out := map[string]string{}
	var count int64
	for _, df := range files {
		var parts []string
		for field := range spec.Fields() {
			parts = append(parts, fmt.Sprintf("%s=%v", field.Name, df.Partition()[field.FieldID]))
		}
		out[df.FilePath()] = strings.Join(parts, ",")
		count += df.Count()
	}
	scanned, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer scanned.Release()
	if scanned.NumRows() != count {
		t.Fatalf("scanned rows=%d manifest rows=%d", scanned.NumRows(), count)
	}
	return out, count
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func samePaths(t *testing.T, got map[string]string, want []string) {
	t.Helper()
	want = append([]string(nil), want...)
	sort.Strings(want)
	if fmt.Sprint(sortedKeys(got)) != fmt.Sprint(want) {
		t.Fatalf("registered files=%v want the worker files %v", sortedKeys(got), want)
	}
}

var partitionTestRows = []partitionTestRow{
	{1, "eu", "2024-01-01T10:00:00Z"},
	{2, "us", "2024-01-01T11:00:00Z"},
	{3, "eu", "2024-01-02T01:00:00Z"},
	{4, "eu", "2024-01-01T23:59:59Z"},
}

func TestRegisterPartitionedFilesAppendsWorkerFilesAsIs(t *testing.T) {
	fields := []PartitionFieldConfig{{Source: "region", Transform: "identity"}, {Source: "ts", Name: "ts_day", Transform: "day"}}
	tbl := newPartitionedTestTable(t, fields)
	paths := writeWorkerFiles(t, icebergpart.Spec{{Source: "region"}, {Source: "ts", Transform: "day"}}, "UTC", partitionTestRows)
	if len(paths) != 3 {
		t.Fatalf("worker files=%d want 3 partitions", len(paths))
	}

	updated, err := registerTestFiles(t, tbl, partitionTestReg(fields), false, paths)
	if err != nil {
		t.Fatal(err)
	}
	got, rows := partitionsByPath(t, updated)
	samePaths(t, got, paths)
	if rows != int64(len(partitionTestRows)) {
		t.Fatalf("rows=%d want %d", rows, len(partitionTestRows))
	}
	// 2024-01-01 is day 19723 since the epoch.
	wantParts := []string{"region=eu,ts_day=19723", "region=eu,ts_day=19724", "region=us,ts_day=19723"}
	var parts []string
	for _, p := range got {
		parts = append(parts, p)
	}
	sort.Strings(parts)
	if fmt.Sprint(parts) != fmt.Sprint(wantParts) {
		t.Fatalf("partitions=%v want %v", parts, wantParts)
	}
	// Order-preserving transforms keep iceberg-go's full column metrics.
	files, err := currentDataFiles(context.Background(), updated)
	if err != nil {
		t.Fatal(err)
	}
	for _, df := range files {
		if len(df.LowerBoundValues()) == 0 {
			t.Fatalf("%s has no column bounds", df.FilePath())
		}
	}
}

func TestRegisterPartitionedFilesBucketAppendAndFullRefresh(t *testing.T) {
	fields := []PartitionFieldConfig{{Source: "id", Name: "id_bucket", Transform: "bucket[4]"}}
	spec := icebergpart.Spec{{Source: "id", Transform: "bucket[4]"}}
	tbl := newPartitionedTestTable(t, fields)
	first := writeWorkerFiles(t, spec, "UTC", partitionTestRows)

	appended, err := registerTestFiles(t, tbl, partitionTestReg(fields), false, first)
	if err != nil {
		t.Fatal(err)
	}
	got, rows := partitionsByPath(t, appended)
	samePaths(t, got, first)
	if rows != 4 {
		t.Fatalf("rows after append=%d want 4", rows)
	}
	// Each file's bucket must be the one Iceberg computes for its ids.
	bucketOf := func(id int64) string {
		v := iceberg.BucketTransform{NumBuckets: 4}.Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: iceberg.NewLiteral(id)})
		return fmt.Sprintf("id_bucket=%v", v.Val.Any())
	}
	wantBuckets := map[string]bool{}
	for _, r := range partitionTestRows {
		wantBuckets[bucketOf(r.id)] = true
	}
	for _, p := range got {
		if !wantBuckets[p] {
			t.Fatalf("partition %q is not the bucket of any id %v", p, wantBuckets)
		}
	}

	second := writeWorkerFiles(t, spec, "UTC", partitionTestRows[:2])
	refreshed, err := registerTestFiles(t, appended, partitionTestReg(fields), true, second)
	if err != nil {
		t.Fatal(err)
	}
	got, rows = partitionsByPath(t, refreshed)
	samePaths(t, got, second)
	if rows != 2 {
		t.Fatalf("rows after full refresh=%d want 2", rows)
	}
}

func TestRegisterPartitionedFilesZonedTimestampUsesUTCDay(t *testing.T) {
	fields := []PartitionFieldConfig{{Source: "ts", Name: "ts_day", Transform: "day"}}
	tbl := newPartitionedTestTable(t, fields)
	// 21:00 in New York on Jan 1 is Jan 2 in UTC.
	paths := writeWorkerFiles(t, icebergpart.Spec{{Source: "ts", Transform: "day"}}, "America/New_York",
		[]partitionTestRow{{1, "us", "2024-01-01T21:00:00-05:00"}})
	tx := tbl.NewTransaction()
	if err := registerPartitionedFiles(context.Background(), tx, partitionTestReg(fields), "analytics.orders", nil, false, paths, nil); err != nil {
		t.Fatal(err)
	}
	staged, err := tx.StagedTable()
	if err != nil {
		t.Fatal(err)
	}
	files, err := currentDataFiles(context.Background(), staged.Table)
	if err != nil {
		t.Fatal(err)
	}
	spec := staged.Spec()
	field := spec.Field(0)
	if len(files) != 1 || fmt.Sprint(files[0].Partition()[field.FieldID]) != "19724" {
		t.Fatalf("files=%v", files)
	}
}

func TestRegisterPartitionedFilesRejectsUnsafeInputsWithoutCommitting(t *testing.T) {
	fields := []PartitionFieldConfig{{Source: "region", Transform: "identity"}}
	contracted := writeWorkerFiles(t, icebergpart.Spec{{Source: "region"}}, "UTC", partitionTestRows)
	cases := []struct {
		name  string
		reg   RunConfig
		paths []string
		class failure.FailureClass
		text  string
	}{
		{"file from an older worker", partitionTestReg(fields),
			append(append([]string(nil), contracted...), writeWorkerFiles(t, nil, "UTC", partitionTestRows)...),
			failure.FailureDataIntegrity, "no partition contract"},
		{"spec changed after the export", partitionTestReg([]PartitionFieldConfig{{Source: "region", Transform: "truncate[1]"}}),
			contracted, failure.FailureConfigurationInvalid, "partition_spec cannot change"},
		{"partitioned table without a run spec", partitionTestReg(nil),
			contracted, failure.FailureConfigurationInvalid, "has no partition_spec"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl := newPartitionedTestTable(t, fields)
			_, err := registerTestFiles(t, tbl, tc.reg, false, tc.paths)
			var f *failure.Failure
			if !errors.As(err, &f) || f.Class != tc.class || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("error=%v want %s containing %q", err, tc.class, tc.text)
			}
			if f.Retryable {
				t.Fatal("the same files can never succeed, so the failure must not be retried")
			}
			if tbl.CurrentSnapshot() != nil {
				t.Fatal("a rejected registration must not commit a snapshot")
			}
		})
	}
}

func TestWorkerPartitionSpecOnlyForPartitionedRESTGoTables(t *testing.T) {
	fields := []PartitionFieldConfig{{Source: "id", Transform: "bucket[4]"}}
	for name, tc := range map[string]struct {
		reg  RunConfig
		want string
	}{
		"rest-go":                   {RunConfig{Enabled: true, Engine: "rest-go", PartitionSpec: fields}, `[{"source":"id","transform":"bucket[4]"}]`},
		"default engine":            {RunConfig{Enabled: true, PartitionSpec: fields}, `[{"source":"id","transform":"bucket[4]"}]`},
		"ice partitions on its own": {RunConfig{Enabled: true, Engine: "ice", PartitionSpec: fields}, ""},
		"registration disabled":     {RunConfig{PartitionSpec: fields}, ""},
		"unpartitioned":             {RunConfig{Enabled: true, Engine: "rest-go"}, ""},
	} {
		got, err := tc.reg.WorkerPartitionSpecJSON()
		if err != nil || got != tc.want {
			t.Fatalf("%s: spec=%q err=%v want %q", name, got, err, tc.want)
		}
	}
}
