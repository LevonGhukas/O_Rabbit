package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// localObjects serves object keys from local files.
type localObjects map[string]string

func (l localObjects) OpenObject(_ context.Context, key string) (io.ReadCloser, bool, error) {
	path, ok := l[key]
	if !ok {
		return nil, false, nil
	}
	f, err := os.Open(path)
	return f, err == nil, err
}

// writeCompactionInput writes one small input file and returns it as a
// compaction input.
func writeCompactionInput(t *testing.T, ctx context.Context, objects localObjects, key string, rec arrow.RecordBatch) compactionInput {
	t.Helper()
	w := newParquetRollingWriterWithContext(ctx, 0)
	if err := w.Write(rec.Schema(), rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f := w.Files()[0]
	objects[key] = f.Path
	t.Cleanup(func() { _ = os.Remove(f.Path) })
	return compactionInput{Key: key, Size: f.Bytes, Records: f.Rows}
}

func TestCompactInputsRewritesFilesKeepingRowsAndPartitions(t *testing.T) {
	ctx := withWorkspaceDir(context.Background(), t.TempDir())
	objects := localObjects{}
	var inputs []compactionInput
	for i, payloads := range [][]string{{"a", "a"}, {"a"}, {"a", "a", "a"}} {
		ids := make([]int64, len(payloads))
		for j := range ids {
			ids[j] = int64(10*i + j)
		}
		rec := newRollingTestRecord(t, ids, payloads)
		inputs = append(inputs, writeCompactionInput(t, ctx, objects, "in-"+string(rune('0'+i)), rec))
		rec.Release()
	}
	pw, err := newParquetOutputWriter(ctx, `[{"source":"payload"}]`, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := compactInputs(ctx, objects, inputs, pw)
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	files := pw.Files()
	defer cleanupRollingTestFiles(files)
	if rows != 6 || len(files) != 1 || files[0].Rows != 6 {
		t.Fatalf("rows=%d files=%+v", rows, files)
	}
	// The output carries the partition contract, so the master registers it
	// like any worker file.
	if n, partition := readPartitionedTestFile(t, files[0].Path); n != 6 || partition != "a" {
		t.Fatalf("output rows=%d partition=%q", n, partition)
	}
}

func TestCompactInputsRefusesInputsThatChanged(t *testing.T) {
	ctx := withWorkspaceDir(context.Background(), t.TempDir())
	objects := localObjects{}
	rec := newRollingTestRecord(t, []int64{1, 2}, []string{"a", "a"})
	defer rec.Release()
	good := writeCompactionInput(t, ctx, objects, "good", rec)

	other := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), other)
	b.Field(0).(*array.StringBuilder).Append("x")
	otherRec := b.NewRecordBatch()
	b.Release()
	defer otherRec.Release()
	differentSchema := writeCompactionInput(t, ctx, objects, "other-schema", otherRec)

	cases := map[string]struct {
		inputs []compactionInput
		want   string
	}{
		"size changed":     {[]compactionInput{{Key: good.Key, Size: good.Size + 1, Records: good.Records}}, "bytes but the table records"},
		"rows changed":     {[]compactionInput{{Key: good.Key, Size: good.Size, Records: good.Records + 1}}, "rows but the table records"},
		"missing":          {[]compactionInput{{Key: "gone", Size: 1, Records: 1}}, "no longer exists"},
		"different schema": {[]compactionInput{good, differentSchema}, "schema differs"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pw, err := newParquetOutputWriter(ctx, "", 1<<30)
			if err != nil {
				t.Fatal(err)
			}
			defer pw.Abort()
			if _, err := compactInputs(ctx, objects, tc.inputs, pw); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %q", err, tc.want)
			}
		})
	}
}
