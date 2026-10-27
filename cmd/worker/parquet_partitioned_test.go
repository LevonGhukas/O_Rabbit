package main

import (
	"context"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergpart"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberg "github.com/apache/iceberg-go"
)

func TestNewTaskParquetWriterChoosesLayoutFromPartitionSpec(t *testing.T) {
	ctx := context.Background()
	plain, err := newTaskParquetWriter(ctx, &grpcpb.TaskAssignment{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plain.(*parquetRollingWriter); !ok {
		t.Fatalf("writer without a spec=%T want *parquetRollingWriter", plain)
	}
	partitioned, err := newTaskParquetWriter(ctx, &grpcpb.TaskAssignment{IcebergPartitionSpecJson: `[{"source":"payload"}]`})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := partitioned.(*partitionedParquetWriter); !ok {
		t.Fatalf("writer with a spec=%T want *partitionedParquetWriter", partitioned)
	}
	if _, err := newTaskParquetWriter(ctx, &grpcpb.TaskAssignment{IcebergPartitionSpecJson: `{`}); err == nil {
		t.Fatal("invalid spec must fail the task instead of writing unsplit files")
	}
}

// TestPartitionedParquetWriterWritesOnePartitionPerFile interleaves three
// partitions across batches with room for only two open files, so a
// partition's file is finished and reopened: rows must still never mix.
func TestPartitionedParquetWriterWritesOnePartitionPerFile(t *testing.T) {
	ctx := withWorkspaceDir(context.Background(), t.TempDir())
	w, err := newPartitionedParquetWriter(ctx, 0, icebergpart.Spec{{Source: "payload"}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	batches := [][]string{{"a", "b", "a"}, {"c", "a"}, {"b", "c", "b"}}
	var id int64
	for _, payloads := range batches {
		ids := make([]int64, len(payloads))
		for i := range ids {
			id++
			ids[i] = id
		}
		rec := newRollingTestRecord(t, ids, payloads)
		if err := w.Write(rec.Schema(), rec); err != nil {
			rec.Release()
			t.Fatal(err)
		}
		rec.Release()
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	files := w.Files()
	defer cleanupRollingTestFiles(files)

	if len(files) < 3 {
		t.Fatalf("files=%d want at least one per partition", len(files))
	}
	var total int64
	seen := map[string]int64{}
	for _, f := range files {
		rows, partition := readPartitionedTestFile(t, f.Path)
		if rows != f.Rows {
			t.Fatalf("%s rows=%d reported=%d", f.Path, rows, f.Rows)
		}
		total += rows
		seen[partition] += rows
	}
	if total != id {
		t.Fatalf("rows written=%d want %d", total, id)
	}
	if seen["a"] != 3 || seen["b"] != 3 || seen["c"] != 2 {
		t.Fatalf("rows per partition=%v", seen)
	}
}

// readPartitionedTestFile checks that every row of the file has the payload
// its footer contract declares and returns the row count and that payload.
func readPartitionedTestFile(t *testing.T, path string) (int64, string) {
	t.Helper()
	pr, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer pr.Close()
	raw := pr.MetaData().KeyValueMetadata().FindValue(icebergpart.FooterKey)
	if raw == nil {
		t.Fatalf("%s has no partition contract", path)
	}
	contract, err := icebergpart.ParseContract(*raw)
	if err != nil {
		t.Fatal(err)
	}
	lit, err := contract.Values[0].Literal(iceberg.PrimitiveTypes.String)
	if err != nil {
		t.Fatal(err)
	}
	want := lit.Any().(string)

	fr, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()
	col := tbl.Column(1)
	for _, chunk := range col.Data().Chunks() {
		values := chunk.(*array.String)
		for i := 0; i < values.Len(); i++ {
			if values.Value(i) != want {
				t.Fatalf("%s holds payload %q but its contract says %q", path, values.Value(i), want)
			}
		}
	}
	return tbl.NumRows(), want
}
