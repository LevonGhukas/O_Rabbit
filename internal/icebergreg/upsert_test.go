package icebergreg

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strconv"
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

type upsertRow struct {
	id     int64
	region string
	v      string
}

func newUpsertTestTable(t *testing.T, keys []string, partition []PartitionFieldConfig) *icetable.Table {
	t.Helper()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "v", Type: iceberg.PrimitiveTypes.String},
	)
	schema, err := schemaWithIdentifierFields(schema, keys)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := buildPartitionSpec(schema, partition)
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

func upsertRecord(rows []upsertRow) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "region", Type: arrow.BinaryTypes.String},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, r := range rows {
		b.Field(0).(*array.Int64Builder).Append(r.id)
		b.Field(1).(*array.StringBuilder).Append(r.region)
		b.Field(2).(*array.StringBuilder).Append(r.v)
	}
	return b.NewRecordBatch()
}

// upsertFiles writes rows as worker files: one file, or one per partition
// with the partition contract when spec is set.
func upsertFiles(t *testing.T, spec icebergpart.Spec, rows []upsertRow) []string {
	t.Helper()
	rec := upsertRecord(rows)
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

func upsertReg(keys []string, partition []PartitionFieldConfig) RunConfig {
	return RunConfig{
		Enabled: true, Engine: "rest-go", Table: "analytics.orders", PartitionSpec: partition,
		Upsert: UpsertConfig{Enabled: true, Keys: keys},
	}.Normalize()
}

// registerUpsertRun mirrors runRESTGoRegister against the local catalog:
// table options, the upsert delete, then the zero-copy file mutation, all in
// one transaction.
func registerUpsertRun(t *testing.T, tbl *icetable.Table, reg RunConfig, fullRefresh bool, files []string) (*icetable.Table, error) {
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
	if err := tx.SetProperties(tableOptionProperties(reg)); err != nil {
		t.Fatal(err)
	}
	if err := applyUpsertDelete(ctx, tx, tbl, reg, fullRefresh, files, nil); err != nil {
		return nil, err
	}
	if len(reg.PartitionSpec) > 0 {
		if err := registerPartitionedFiles(ctx, tx, reg, "analytics.orders", existing, fullRefresh, files, nil); err != nil {
			return nil, err
		}
	} else {
		paths := make([]string, len(existing))
		for i, df := range existing {
			paths[i] = df.FilePath()
		}
		if err := applyRESTGoFileMutation(ctx, tx, fullRefresh, paths, files, nil); err != nil {
			return nil, err
		}
	}
	return tx.Commit(ctx)
}

// currentRows scans the table and fails on any key with more than one row.
func currentRows(t *testing.T, tbl *icetable.Table, keys []string) map[string]upsertRow {
	t.Helper()
	// One scan worker: iceberg-go v0.5 shares a partition evaluator between
	// concurrent manifest readers, which the race detector reports.
	scanned, err := tbl.Scan(icetable.WitMaxConcurrency(1)).ToArrowTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer scanned.Release()
	reader := array.NewTableReader(scanned, 1024)
	defer reader.Release()
	out := map[string]upsertRow{}
	for reader.Next() {
		rec := reader.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		regions := rec.Column(1).(*array.String)
		values := rec.Column(2).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			row := upsertRow{ids.Value(i), regions.Value(i), values.Value(i)}
			key := strconv.FormatInt(row.id, 10)
			if len(keys) > 1 {
				key += "/" + row.region
			}
			if prev, dup := out[key]; dup {
				t.Fatalf("key %s has two current rows: %+v and %+v", key, prev, row)
			}
			out[key] = row
		}
	}
	return out
}

func TestUpsertKeepsOneCurrentRowPerKey(t *testing.T) {
	for _, keys := range [][]string{{"id"}, {"id", "region"}} {
		t.Run(strings.Join(keys, "+"), func(t *testing.T) {
			reg := upsertReg(keys, nil)
			tbl := newUpsertTestTable(t, keys, nil)
			var err error
			runs := [][]upsertRow{
				{{1, "eu", "a"}, {2, "eu", "a"}, {3, "eu", "a"}, {4, "eu", "a"}, {5, "eu", "a"}},
				{{2, "eu", "b"}, {4, "eu", "b"}, {6, "eu", "b"}},
				{{4, "eu", "c"}, {7, "eu", "c"}},
			}
			for _, run := range runs {
				if tbl, err = registerUpsertRun(t, tbl, reg, false, upsertFiles(t, nil, run)); err != nil {
					t.Fatal(err)
				}
			}
			got := currentRows(t, tbl, keys)
			want := map[int64]string{1: "a", 2: "b", 3: "a", 4: "c", 5: "a", 6: "b", 7: "c"}
			if len(got) != len(want) {
				t.Fatalf("rows=%v", got)
			}
			for _, row := range got {
				if want[row.id] != row.v {
					t.Fatalf("id %d has v=%q want %q", row.id, row.v, want[row.id])
				}
			}
			// Other engines keep seeing the configured delete mode.
			if mode := tbl.Properties()[icetable.WriteDeleteModeKey]; mode != icetable.WriteModeMergeOnRead {
				t.Fatalf("write.delete.mode=%q want merge-on-read", mode)
			}
		})
	}
}

func TestUpsertPartitionedTableMovesKeyAcrossPartitions(t *testing.T) {
	partition := []PartitionFieldConfig{{Source: "region", Transform: "identity"}}
	spec := icebergpart.Spec{{Source: "region"}}
	reg := upsertReg([]string{"id"}, partition)
	tbl := newUpsertTestTable(t, []string{"id"}, partition)
	var err error
	for _, run := range [][]upsertRow{
		{{1, "eu", "a"}, {2, "us", "a"}, {3, "eu", "a"}},
		{{1, "us", "b"}},
	} {
		if tbl, err = registerUpsertRun(t, tbl, reg, false, upsertFiles(t, spec, run)); err != nil {
			t.Fatal(err)
		}
	}
	got := currentRows(t, tbl, []string{"id"})
	if len(got) != 3 || got["1"] != (upsertRow{1, "us", "b"}) || got["2"].v != "a" || got["3"].v != "a" {
		t.Fatalf("rows=%v", got)
	}
}

func TestUpsertFullRefreshAndEmptyInput(t *testing.T) {
	reg := upsertReg([]string{"id"}, nil)
	tbl := newUpsertTestTable(t, []string{"id"}, nil)
	tbl, err := registerUpsertRun(t, tbl, reg, false, upsertFiles(t, nil, []upsertRow{{1, "eu", "a"}, {2, "eu", "a"}}))
	if err != nil {
		t.Fatal(err)
	}
	// A full refresh replaces the table; upsert deletes nothing first.
	tbl, err = registerUpsertRun(t, tbl, reg, true, upsertFiles(t, nil, []upsertRow{{2, "eu", "b"}, {3, "eu", "b"}}))
	if err != nil {
		t.Fatal(err)
	}
	if got := currentRows(t, tbl, []string{"id"}); len(got) != 2 || got["2"].v != "b" || got["3"].v != "b" {
		t.Fatalf("rows after full refresh=%v", got)
	}

	// An empty run (no files) deletes nothing.
	ctx := context.Background()
	filter, err := buildUpsertDeleteFilter(ctx, tbl, nil, []string{"id"}, 0)
	if err != nil || !filter.Equals(iceberg.AlwaysFalse{}) {
		t.Fatalf("empty input filter=%v err=%v", filter, err)
	}
	tx := tbl.NewTransaction()
	if err := applyUpsertDelete(ctx, tx, tbl, reg, false, nil, nil); err != nil {
		t.Fatal(err)
	}
	staged, err := tx.StagedTable()
	if err != nil {
		t.Fatal(err)
	}
	if staged.CurrentSnapshot().SnapshotID != tbl.CurrentSnapshot().SnapshotID {
		t.Fatal("an empty upsert must not create a snapshot")
	}
}

func TestUpsertRejectsDuplicateKeysAcrossFiles(t *testing.T) {
	tbl := newUpsertTestTable(t, []string{"id"}, nil)
	tbl, err := registerUpsertRun(t, tbl, upsertReg([]string{"id"}, nil), false, upsertFiles(t, nil, []upsertRow{{1, "eu", "a"}}))
	if err != nil {
		t.Fatal(err)
	}
	files := append(upsertFiles(t, nil, []upsertRow{{1, "eu", "b"}}), upsertFiles(t, nil, []upsertRow{{1, "eu", "c"}})...)
	if _, err := registerUpsertRun(t, tbl, upsertReg([]string{"id"}, nil), false, files); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("error=%v", err)
	}
}

func TestUpsertMaxKeysRejectsOversizedRunsBeforeReadingRows(t *testing.T) {
	tbl := newUpsertTestTable(t, []string{"id"}, nil)
	tbl, err := registerUpsertRun(t, tbl, upsertReg([]string{"id"}, nil), false, upsertFiles(t, nil, []upsertRow{{1, "eu", "a"}}))
	if err != nil {
		t.Fatal(err)
	}
	reg := upsertReg([]string{"id"}, nil)
	reg.Upsert.MaxKeys = 2
	// Duplicate keys prove the rows were never read: the size check fails first.
	files := upsertFiles(t, nil, []upsertRow{{1, "eu", "b"}, {1, "eu", "b"}, {1, "eu", "b"}})
	_, err = registerUpsertRun(t, tbl, reg, false, files)
	var f *failure.Failure
	if !errors.As(err, &f) || f.Class != failure.FailureConfigurationInvalid || f.Retryable || !strings.Contains(err.Error(), "above upsert.max_keys=2") {
		t.Fatalf("error=%v", err)
	}
	if upsertMaxKeys(0, []string{"id"}) != DefaultUpsertMaxKeys || upsertMaxKeys(0, []string{"id", "region"}) != DefaultCompositeUpsertMaxKeys || upsertMaxKeys(7, []string{"id"}) != 7 {
		t.Fatal("unexpected default max keys")
	}
}

type exprShape struct{ nodes, sets, setValues int }

type shapeVisitor struct{}

func (shapeVisitor) VisitTrue() exprShape           { return exprShape{nodes: 1} }
func (shapeVisitor) VisitFalse() exprShape          { return exprShape{nodes: 1} }
func (shapeVisitor) VisitNot(c exprShape) exprShape { c.nodes++; return c }
func (shapeVisitor) VisitAnd(l, r exprShape) exprShape {
	return exprShape{l.nodes + r.nodes + 1, l.sets + r.sets, l.setValues + r.setValues}
}
func (shapeVisitor) VisitOr(l, r exprShape) exprShape {
	return exprShape{l.nodes + r.nodes + 1, l.sets + r.sets, l.setValues + r.setValues}
}
func (shapeVisitor) VisitUnbound(iceberg.UnboundPredicate) exprShape { return exprShape{nodes: 1} }
func (shapeVisitor) VisitBound(p iceberg.BoundPredicate) exprShape {
	if set, ok := p.(iceberg.BoundSetPredicate); ok {
		return exprShape{nodes: 1, sets: 1, setValues: set.Literals().Len()}
	}
	return exprShape{nodes: 1}
}

func TestUpsertDeleteExpressionSizeIsBoundedForSingleKey(t *testing.T) {
	for _, n := range []int{150, 10_000, 200_000} {
		tuples := make([][]iceberg.Literal, n)
		for i := range tuples {
			tuples[i] = []iceberg.Literal{iceberg.NewLiteral(int64(n - i))}
		}
		bound, err := iceberg.BindExpr(optionTestSchema(), upsertDeleteExpression([]string{"id"}, tuples), true)
		if err != nil {
			t.Fatal(err)
		}
		shape, err := iceberg.VisitExpr(bound, iceberg.BooleanExprVisitor[exprShape](shapeVisitor{}))
		if err != nil {
			t.Fatal(err)
		}
		// One IN set with every key, plus at most upsertKeyRanges ranges of
		// two comparisons joined by AND/OR nodes.
		if shape.sets != 1 || shape.setValues != n || shape.nodes > 1+upsertKeyRanges*3+upsertKeyRanges+1 {
			t.Fatalf("n=%d shape=%+v", n, shape)
		}
	}
}

// TestUpsertScale runs a high-cardinality upsert and logs its resource use.
// ORABBIT_UPSERT_SCALE_KEYS raises the key count (for example 1000000).
func TestUpsertScale(t *testing.T) {
	n := 50_000
	if v := os.Getenv("ORABBIT_UPSERT_SCALE_KEYS"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil {
			t.Fatal(err)
		}
	}
	for _, keys := range [][]string{{"id"}, {"id", "region"}} {
		keyCount := n
		if len(keys) > 1 {
			keyCount = min(n, 2_000)
		}
		t.Run(strings.Join(keys, "+"), func(t *testing.T) {
			reg := upsertReg(keys, nil)
			reg.Upsert.MaxKeys = int64(keyCount)
			tbl := newUpsertTestTable(t, keys, nil)
			base := make([]upsertRow, 2*keyCount)
			for i := range base {
				base[i] = upsertRow{int64(i), "eu", "a"}
			}
			tbl, err := registerUpsertRun(t, tbl, reg, false, upsertFiles(t, nil, base))
			if err != nil {
				t.Fatal(err)
			}
			incoming := make([]upsertRow, keyCount)
			for i := range incoming {
				// Every other existing key plus new keys past the end.
				incoming[i] = upsertRow{int64(i * 3), "eu", "b"}
			}
			files := upsertFiles(t, nil, incoming)

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			start := time.Now()
			tbl, err = registerUpsertRun(t, tbl, reg, false, files)
			if err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(start)
			runtime.ReadMemStats(&after)
			t.Logf("keys=%d table_rows=%d elapsed=%s allocated=%dMiB heap_in_use=%dMiB",
				keyCount, len(base), elapsed, (after.TotalAlloc-before.TotalAlloc)>>20, after.HeapInuse>>20)

			got := currentRows(t, tbl, keys)
			updated := 0
			for _, row := range got {
				if row.v == "b" {
					updated++
				}
			}
			wantRows := len(base) + keyCount - (2*keyCount+2)/3
			if updated != keyCount || len(got) != wantRows {
				t.Fatalf("rows=%d updated=%d want rows=%d updated=%d", len(got), updated, wantRows, keyCount)
			}
		})
	}
}

func TestUpsertMaxKeysOptionIsValidated(t *testing.T) {
	reg := upsertReg([]string{"id"}, nil)
	reg.Upsert.MaxKeys = -1
	if err := reg.validateTableOptions(); err == nil || !strings.Contains(err.Error(), "max_keys") {
		t.Fatalf("error=%v", err)
	}
	maxKeys := int64(42)
	cfg := ApplyRunOptions(upsertReg([]string{"id"}, nil), RunOptions{Upsert: RunUpsertOptions{MaxKeys: &maxKeys}})
	if cfg.Upsert.MaxKeys != 42 {
		t.Fatalf("max_keys=%d", cfg.Upsert.MaxKeys)
	}
}
