package parquetio

import (
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{
			Name: "id",
			Type: arrow.PrimitiveTypes.Int64,
		},
	}, nil)
}

func TestParquetTemporalAnnotationsRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  *arrow.TimestampType
	}{
		{"local", &arrow.TimestampType{Unit: arrow.Microsecond}},
		{"instant", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "created_at", Type: tc.typ, Nullable: false}}, nil)
			w, path, err := NewTempFileWriter(schema, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(path)
			b := array.NewTimestampBuilder(memory.DefaultAllocator, tc.typ)
			b.Append(1)
			a := b.NewArray()
			b.Release()
			defer a.Release()
			rec := array.NewRecordBatch(schema, []arrow.Array{a}, 1)
			defer rec.Release()
			if err := w.Write(rec); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			pr, err := file.OpenParquetFile(path, false)
			if err != nil {
				t.Fatal(err)
			}
			defer pr.Close()
			actual, err := pqarrow.FromParquet(pr.MetaData().Schema, &pqarrow.ArrowReadProperties{}, pr.MetaData().KeyValueMetadata())
			if err != nil {
				t.Fatal(err)
			}
			got := actual.Field(0).Type.(*arrow.TimestampType)
			if got.Unit != arrow.Microsecond || got.TimeZone != tc.typ.TimeZone {
				t.Fatalf("timestamp annotation=%s, want %s", got, tc.typ)
			}
		})
	}
}

func TestNewTempFileWriter(t *testing.T) {
	schema := testSchema()

	w, path, err := NewTempFileWriter(schema, Options{})
	if err != nil {
		t.Fatalf("NewTempFileWriter failed: %v", err)
	}

	defer os.Remove(path)

	if path == "" {
		t.Fatal("expected parquet path")
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected parquet file to exist: %v", err)
	}

	if got := w.Schema(); got != schema {
		t.Fatal("schema mismatch")
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestNewTempFileWriterNilSchema(t *testing.T) {
	_, _, err := NewTempFileWriter(nil, Options{})
	if err == nil {
		t.Fatal("expected error for nil schema")
	}

	if !strings.Contains(err.Error(), "nil schema") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriterSchemaNilReceiver(t *testing.T) {
	var w *Writer

	if got := w.Schema(); got != nil {
		t.Fatal("expected nil schema")
	}
}

func TestWriterWriteNilReceiver(t *testing.T) {
	var w *Writer

	err := w.Write(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestComputeFileMeta(t *testing.T) {
	f, err := os.CreateTemp("", "meta_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	content := []byte("hello parquet metadata")

	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	meta, err := ComputeFileMeta(f.Name())
	if err != nil {
		t.Fatalf("ComputeFileMeta failed: %v", err)
	}

	if meta.Bytes != int64(len(content)) {
		t.Fatalf("expected %d bytes, got %d", len(content), meta.Bytes)
	}

	if meta.SHA256 == "" {
		t.Fatal("expected sha256")
	}
}

func TestComputeFileMetaMissingFile(t *testing.T) {
	_, err := ComputeFileMeta("/does/not/exist/file.parquet")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWriterWrite(t *testing.T) {
	schema := testSchema()

	w, path, err := NewTempFileWriter(schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)

	pool := memory.DefaultAllocator

	builder := array.NewInt64Builder(pool)
	defer builder.Release()

	builder.Append(1)

	arr := builder.NewArray()
	defer arr.Release()

	rec := array.NewRecordBatch(
		schema,
		[]arrow.Array{arr},
		1,
	)
	defer rec.Release()

	if err := w.Write(rec); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}
