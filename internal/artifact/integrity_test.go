package artifact_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func writeTestParquet(t *testing.T, schema *arrow.Schema, values []int64) string {
	t.Helper()
	w, path, err := parquetio.NewTempFileWriter(schema, parquetio.Options{})
	if err != nil {
		t.Fatal(err)
	}
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues(values, nil)
	a := b.NewArray()
	b.Release()
	defer a.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{a}, int64(len(values)))
	defer rec.Release()
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

func TestValidateLocalParquetStableAndRejectsCorruption(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}, nil)
	path := writeTestParquet(t, schema, []int64{1, 2, 3})
	one, err := artifact.ValidateLocalParquet(context.Background(), path, 3, schema)
	if err != nil {
		t.Fatal(err)
	}
	two, err := artifact.ValidateLocalParquet(context.Background(), path, 3, schema)
	if err != nil {
		t.Fatal(err)
	}
	if one != two || one.ByteSize <= 0 || len(one.SHA256) != 64 || len(one.SchemaFingerprint) != 64 {
		t.Fatalf("unstable integrity: %+v %+v", one, two)
	}
	if _, err := artifact.ValidateLocalParquet(context.Background(), path, 4, schema); err == nil || !strings.Contains(err.Error(), "row count mismatch") {
		t.Fatalf("row mismatch err=%v", err)
	} else if failure, ok := artifact.AsFailure(err); !ok || failure.Classification != artifact.FailureLocalRowCountMismatch {
		t.Fatalf("row classification=%v", err)
	}
	other := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	if _, err := artifact.ValidateLocalParquet(context.Background(), path, 3, other); err == nil || !strings.Contains(err.Error(), "schema fingerprint mismatch") {
		t.Fatalf("schema mismatch err=%v", err)
	} else if failure, ok := artifact.AsFailure(err); !ok || failure.Classification != artifact.FailureLocalSchemaMismatch {
		t.Fatalf("schema classification=%v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	truncated := path + ".truncated"
	if err := os.WriteFile(truncated, b[:len(b)/2], 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(truncated) })
	if _, err := artifact.ValidateLocalParquet(context.Background(), truncated, 3, schema); err == nil {
		t.Fatal("truncated parquet accepted")
	} else if failure, ok := artifact.AsFailure(err); !ok || failure.Classification != artifact.FailureLocalParquetInvalid {
		t.Fatalf("footer classification=%v", err)
	}
}

func TestSchemaFingerprintGoldenProperties(t *testing.T) {
	nested := arrow.StructOf(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	base := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false}, {Name: "nested", Type: nested, Nullable: true}}, nil)
	same := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false}, {Name: "nested", Type: arrow.StructOf(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true}), Nullable: true}}, nil)
	reordered := arrow.NewSchema([]arrow.Field{{Name: "nested", Type: nested, Nullable: true}, {Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}, nil)
	typeChanged := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false}, {Name: "nested", Type: nested, Nullable: true}}, nil)
	nullChanged := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "nested", Type: nested, Nullable: true}}, nil)
	nestedChanged := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false}, {Name: "nested", Type: arrow.StructOf(arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true}}, nil)
	fp, err := artifact.SchemaFingerprint(base)
	if err != nil {
		t.Fatal(err)
	}
	if fp != "2cf21faec7e3fedab5db314024e8aae3b19ec57e6c36936a0ac301abee18c60a" {
		t.Fatalf("golden fingerprint changed: %s", fp)
	}
	for name, schema := range map[string]*arrow.Schema{"reordered": reordered, "type": typeChanged, "nullable": nullChanged, "nested": nestedChanged} {
		got, _ := artifact.SchemaFingerprint(schema)
		if got == fp {
			t.Fatalf("%s change did not alter fingerprint", name)
		}
	}
	got, _ := artifact.SchemaFingerprint(same)
	if got != fp {
		t.Fatalf("identical schema fingerprint=%s want=%s", got, fp)
	}
}

type boundedReader struct {
	remaining int
	max       int
}

func (r *boundedReader) Read(p []byte) (int, error) {
	if len(p) > r.max {
		panic("unbounded read buffer")
	}
	if r.remaining == 0 {
		return 0, os.ErrClosed
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	r.remaining -= n
	if r.remaining == 0 {
		return n, context.Canceled
	}
	return n, nil
}

func TestStreamHashIsBoundedAndCancelable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := artifact.StreamSHA256(ctx, strings.NewReader(strings.Repeat("x", 1024))); err == nil {
		t.Fatal("canceled validation continued")
	}
	r := &boundedReader{remaining: 128 * 1024, max: 64 * 1024}
	if _, _, err := artifact.StreamSHA256(context.Background(), r); err == nil {
		t.Fatal("expected sentinel reader error")
	}
}

func TestPortableRemoteVerificationRejectsSameSizeChangedBytes(t *testing.T) {
	expected := []byte("exact parquet bytes")
	sum := sha256.Sum256(expected)
	digest := hex.EncodeToString(sum[:])
	if err := artifact.VerifyStream(context.Background(), strings.NewReader(string(expected)), int64(len(expected)), digest); err != nil {
		t.Fatal(err)
	}
	changed := []byte("wrong parquet bytes")
	if len(changed) != len(expected) {
		t.Fatal("fixture must preserve size")
	}
	if err := artifact.VerifyStream(context.Background(), strings.NewReader(string(changed)), int64(len(expected)), digest); err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
		t.Fatalf("changed bytes err=%v", err)
	}
	if err := artifact.VerifyStream(context.Background(), strings.NewReader(string(expected)), int64(len(expected)+1), digest); err == nil || !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("size mismatch err=%v", err)
	}
}
