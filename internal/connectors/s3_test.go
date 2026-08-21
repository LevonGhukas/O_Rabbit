package connectors

import (
	"bytes"
	"context"
	"encoding/xml"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestOpenS3_InvalidDSN(t *testing.T) {
	ctx := context.Background()

	// Should fail on invalid prefix
	_, err := OpenS3(ctx, "postgres://user:pass@host/db")
	if err == nil {
		t.Fatal("expected error for non-s3 dsn")
	}

	// Should fail on invalid structure
	_, err = OpenS3(ctx, "s3://bucket_without_key")
	if err == nil {
		t.Fatal("expected error for missing key in s3 dsn")
	}

	// Should fail if no credentials in env
	os.Unsetenv("ORABBIT_DEFAULT_S3_ACCESS_KEY_ID")
	os.Unsetenv("ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY")
	_, err = OpenS3(ctx, "s3://bucket/key.csv")
	if err == nil {
		t.Fatal("expected error for missing credentials")
	}

	// Set mock credentials
	os.Setenv("ORABBIT_DEFAULT_S3_ACCESS_KEY_ID", "mock")
	os.Setenv("ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY", "mock")

	reader, err := OpenS3(ctx, "s3://bucket/key.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s3Reader, ok := reader.(*S3Reader)
	if !ok {
		t.Fatal("expected S3Reader")
	}
	if s3Reader.bucket != "bucket" {
		t.Errorf("expected bucket 'bucket', got %q", s3Reader.bucket)
	}
	if s3Reader.key != "key.csv" {
		t.Errorf("expected key 'key.csv', got %q", s3Reader.key)
	}
	if s3Reader.format != "csv" {
		t.Errorf("expected format 'csv', got %q", s3Reader.format)
	}
}

func TestOpenS3_Formats(t *testing.T) {
	ctx := context.Background()
	os.Setenv("ORABBIT_DEFAULT_S3_ACCESS_KEY_ID", "mock")
	os.Setenv("ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY", "mock")

	cases := []struct {
		dsn    string
		format string
	}{
		{"s3://my-bucket/path/to/file.json", "json"},
		{"s3://my-bucket/path/to/file.xml", "xml"},
		{"s3://my-bucket/path/to/file.parquet", "parquet"},
		{"s3://my-bucket/path/to/file.xlsx", "excel"},
		{"s3://my-bucket/path/to/file.xls", "excel"},
		{"s3://my-bucket/path/to/file.csv", "csv"},
	}

	for _, tc := range cases {
		reader, err := OpenS3(ctx, tc.dsn)
		if err != nil {
			t.Fatalf("unexpected error for %s: %v", tc.dsn, err)
		}
		s3Reader, ok := reader.(*S3Reader)
		if !ok {
			t.Fatalf("expected S3Reader for %s", tc.dsn)
		}
		if s3Reader.format != tc.format {
			t.Errorf("expected format %q, got %q for %s", tc.format, s3Reader.format, tc.dsn)
		}
	}
}

func TestS3XMLIterator(t *testing.T) {
	xmlData := `<?xml version="1.0" encoding="UTF-8"?>
<catalog>
    <book id="101" available="true">
        <author>Alice Smith</author>
        <price>29.95</price>
        <qty>5</qty>
    </book>
    <book id="102">
        <author>Bob Jones</author>
        <price>19.50</price>
        <qty>0</qty>
    </book>
</catalog>`

	decoder := xml.NewDecoder(strings.NewReader(xmlData))
	it := &s3XMLIterator{decoder: decoder}
	ctx := context.Background()

	var docs []map[string]any
	for it.Next(ctx) {
		doc, err := it.Decode()
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		docs = append(docs, doc)
	}

	if it.Err() != nil {
		t.Fatalf("unexpected iterator error: %v", it.Err())
	}

	if len(docs) != 2 {
		t.Fatalf("expected 2 docs, got %d: %+v", len(docs), docs)
	}

	doc1 := docs[0]
	if doc1["id"] != int64(101) {
		t.Errorf("expected id 101, got %v", doc1["id"])
	}
	if doc1["author"] != "Alice Smith" {
		t.Errorf("expected author Alice Smith, got %v", doc1["author"])
	}
	if doc1["price"] != float64(29.95) {
		t.Errorf("expected price 29.95, got %v", doc1["price"])
	}
	if doc1["qty"] != int64(5) {
		t.Errorf("expected qty 5, got %v", doc1["qty"])
	}

	doc2 := docs[1]
	if doc2["id"] != int64(102) {
		t.Errorf("expected id 102, got %v", doc2["id"])
	}
	if doc2["author"] != "Bob Jones" {
		t.Errorf("expected author Bob Jones, got %v", doc2["author"])
	}
	if doc2["price"] != float64(19.50) {
		t.Errorf("expected price 19.50, got %v", doc2["price"])
	}
}

func TestS3ParquetIterator(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob"}, nil)
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{95.5, 88.0}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &buf, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	if err != nil {
		t.Fatalf("failed to create parquet writer: %v", err)
	}
	if err := writer.Write(rec); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Now read it back using s3ParquetIterator
	parquetReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("failed to open parquet reader: %v", err)
	}
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 2048}, mem)
	if err != nil {
		t.Fatalf("failed to open pqarrow reader: %v", err)
	}
	ctx := context.Background()
	recReader, err := arrowReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		t.Fatalf("failed to get record reader: %v", err)
	}

	it := &s3ParquetIterator{recReader: recReader}
	defer it.Close()

	var rows []map[string]any
	for it.Next(ctx) {
		doc, err := it.Decode()
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		rows = append(rows, doc)
	}

	if it.Err() != nil {
		t.Fatalf("unexpected iterator error: %v", it.Err())
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %+v", len(rows), rows)
	}

	if rows[0]["id"] != int64(1) || rows[0]["name"] != "Alice" || rows[0]["score"] != float64(95.5) {
		t.Errorf("unexpected row 0: %+v", rows[0])
	}
	if rows[1]["id"] != int64(2) || rows[1]["name"] != "Bob" || rows[1]["score"] != float64(88.0) {
		t.Errorf("unexpected row 1: %+v", rows[1])
	}
}
