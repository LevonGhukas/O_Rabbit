package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestParquetRollingWriterUsesManagedWorkspace(t *testing.T) {
	workspace := t.TempDir()
	ctx := withWorkspaceDir(context.Background(), workspace)
	w := newParquetRollingWriterWithContext(ctx, 0)
	rec := newRollingTestRecord(t, []int64{1}, []string{"owned"})
	defer rec.Release()
	if err := w.Write(rec.Schema(), rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	defer cleanupRollingTestFiles(w.Files())
	if len(w.Files()) != 1 || filepath.Dir(w.Files()[0].Path) != workspace {
		t.Fatalf("files=%+v workspace=%s", w.Files(), workspace)
	}
}

func TestParquetRollingDecisionUsesEncodedBytesAndAllowsBoundedOvershoot(t *testing.T) {
	w := &parquetRollingWriter{
		targetFileBytes:     100,
		current:             &parquetio.Writer{},
		currentRows:         1,
		currentLogicalBytes: 1_000,
		currentEncodedBytes: 100,
	}
	if w.shouldRollBefore(100) {
		t.Fatal("expected a small next batch to be absorbed within the overshoot allowance")
	}
	if !w.shouldRollBefore(300) {
		t.Fatal("expected rollover when predicted physical size exceeds the overshoot allowance")
	}
}

func TestBuildTaskParquetObjectKeys(t *testing.T) {
	got := buildTaskParquetObjectKeys("exports/orders/_runs/run-1", 123, 3)
	want := []string{
		"exports/orders/_runs/run-1/part-000123.parquet",
		"exports/orders/_runs/run-1/part-000123-001.parquet",
		"exports/orders/_runs/run-1/part-000123-002.parquet",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys=%v want %v", got, want)
	}
}

func TestParquetRollingWriterCloseWithNoRowsProducesNoFiles(t *testing.T) {
	w := newParquetRollingWriterWithContext(context.Background(), 1)
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := len(w.Files()); got != 0 {
		t.Fatalf("len(Files())=%d want 0", got)
	}
	if got := w.TotalBytes(); got != 0 {
		t.Fatalf("TotalBytes()=%d want 0", got)
	}
}

func TestParquetRollingWriterDisabledKeepsSingleFile(t *testing.T) {
	w := newParquetRollingWriterWithContext(context.Background(), 0)
	rec1 := newRollingTestRecord(t, []int64{1, 2}, []string{"alpha", "beta"})
	defer rec1.Release()
	rec2 := newRollingTestRecord(t, []int64{3, 4}, []string{"gamma", "delta"})
	defer rec2.Release()

	if err := w.Write(rec1.Schema(), rec1); err != nil {
		t.Fatalf("Write(rec1) error = %v", err)
	}
	if err := w.Write(rec2.Schema(), rec2); err != nil {
		t.Fatalf("Write(rec2) error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	defer cleanupRollingTestFiles(w.Files())

	files := w.Files()
	if got := len(files); got != 1 {
		t.Fatalf("len(Files())=%d want 1", got)
	}
	if files[0].Rows != 4 {
		t.Fatalf("files[0].Rows=%d want 4", files[0].Rows)
	}
	if files[0].Bytes <= 0 {
		t.Fatalf("files[0].Bytes=%d want > 0", files[0].Bytes)
	}
	if got := w.TotalBytes(); got != files[0].Bytes {
		t.Fatalf("TotalBytes()=%d want %d", got, files[0].Bytes)
	}
}

func TestParquetRollingWriterTargetBytesCreatesMultipleFiles(t *testing.T) {
	w := newParquetRollingWriterWithContext(context.Background(), 1)
	recs := []arrow.RecordBatch{
		newRollingTestRecord(t, []int64{1}, []string{"alpha"}),
		newRollingTestRecord(t, []int64{2}, []string{"beta"}),
		newRollingTestRecord(t, []int64{3}, []string{"gamma"}),
	}
	for _, rec := range recs {
		defer rec.Release()
		if err := w.Write(rec.Schema(), rec); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	defer cleanupRollingTestFiles(w.Files())

	files := w.Files()
	if got := len(files); got != 3 {
		t.Fatalf("len(Files())=%d want 3", got)
	}
	for i, f := range files {
		if f.Rows != 1 {
			t.Fatalf("files[%d].Rows=%d want 1", i, f.Rows)
		}
		if f.Bytes <= 0 {
			t.Fatalf("files[%d].Bytes=%d want > 0", i, f.Bytes)
		}
	}
}

func newRollingTestRecord(t *testing.T, ids []int64, payloads []string) arrow.RecordBatch {
	t.Helper()
	if len(ids) != len(payloads) {
		t.Fatalf("ids=%d payloads=%d", len(ids), len(payloads))
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "payload", Type: arrow.BinaryTypes.String},
	}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	payloadBuilder := builder.Field(1).(*array.StringBuilder)
	for i := range ids {
		idBuilder.Append(ids[i])
		payloadBuilder.Append(payloads[i])
	}
	return builder.NewRecordBatch()
}

func cleanupRollingTestFiles(files []parquetOutputFile) {
	for _, f := range files {
		if f.Path != "" {
			_ = os.Remove(f.Path)
		}
	}
}
