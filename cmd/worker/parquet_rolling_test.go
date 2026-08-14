package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

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

func TestShouldRollParquetFile(t *testing.T) {
	tests := []struct {
		name            string
		rows            int64
		bytes           int64
		targetFileBytes int64
		want            bool
	}{
		{name: "empty file never rolls", rows: 0, bytes: 1024, targetFileBytes: 1, want: false},
		{name: "target bytes threshold", rows: 10, bytes: 1024, targetFileBytes: 1024, want: true},
		{name: "disabled thresholds", rows: 5, bytes: 10, want: false},
		{name: "below thresholds", rows: 4, bytes: 1023, targetFileBytes: 2048, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRollParquetFile(tt.rows, tt.bytes, tt.targetFileBytes); got != tt.want {
				t.Fatalf("shouldRollParquetFile()=%v want %v", got, tt.want)
			}
		})
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
