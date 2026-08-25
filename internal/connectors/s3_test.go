package connectors

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/xuri/excelize/v2"
)

func TestS3CSVIteratorFieldOrder(t *testing.T) {
	reader := csv.NewReader(strings.NewReader("id,date,age\n1,2026-01-01,30\n"))
	headers, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	it := &s3CSVIterator{reader: reader, headers: headers}
	if got := it.FieldOrder(); !equalStrings(got, []string{"id", "date", "age"}) {
		t.Fatalf("field order=%v", got)
	}
}

func TestS3ExcelIteratorFieldOrder(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	if err := f.SetSheetRow("Sheet1", "A1", &[]any{"id", "date", "age"}); err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	if err := f.Write(&data); err != nil {
		t.Fatal(err)
	}
	parsed, err := excelize.OpenReader(bytes.NewReader(data.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = parsed.Close() }()
	rows, err := parsed.Rows("Sheet1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("missing header row")
	}
	headers, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	it := &s3ExcelIterator{rows: rows, headers: headers}
	if got := it.FieldOrder(); !equalStrings(got, []string{"id", "date", "age"}) {
		t.Fatalf("field order=%v", got)
	}
}

func TestS3JSONIteratorRecordPath(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		recordPath string
		wantIDs    []float64
		wantErr    string
	}{
		{"root array remains compatible", `[{"id":1},{"id":2}]`, "", []float64{1, 2}, ""},
		{"object wrapped array", `{"airports":[{"id":1},{"id":2}]}`, "/airports", []float64{1, 2}, ""},
		{"nested object path", `{"data":{"items":[{"id":1}]}}`, "/data/items", []float64{1}, ""},
		{"escaped pointer key", `{"a/b":[{"id":1}]}`, "/a~1b", []float64{1}, ""},
		{"object root requires path", `{"airports":[{"id":1}],"countries":[{"id":2}]}`, "", nil, "record_path is required"},
		{"malformed path", `{"airports":[{"id":1}]}`, "airports", nil, "must start with /"},
		{"missing path", `{"airports":[{"id":1}]}`, "/missing", nil, `record_path "/missing" was not found`},
		{"scalar path", `{"airports":42}`, "/airports", nil, "must resolve to an array"},
		{"object path", `{"airports":{"id":1}}`, "/airports", nil, "must resolve to an array"},
		{"array of scalars", `{"airports":[1,2]}`, "/airports", nil, "elements must be objects"},
		{"empty selected array", `{"airports":[]}`, "/airports", []float64{}, ""},
		{"malformed JSON", `{"airports":[`, "/airports", nil, "EOF"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			it := &s3JSONIterator{decoder: json.NewDecoder(strings.NewReader(tc.input)), recordPath: tc.recordPath}
			var gotIDs []float64
			for it.Next(context.Background()) {
				doc, err := it.Decode()
				if err != nil {
					t.Fatal(err)
				}
				gotIDs = append(gotIDs, doc["id"].(float64))
			}
			if tc.wantErr != "" {
				if it.Err() == nil || !strings.Contains(it.Err().Error(), tc.wantErr) {
					t.Fatalf("error=%v, want %q", it.Err(), tc.wantErr)
				}
				return
			}
			if it.Err() != nil {
				t.Fatal(it.Err())
			}
			if !equalFloat64s(gotIDs, tc.wantIDs) {
				t.Fatalf("ids=%v want %v", gotIDs, tc.wantIDs)
			}
		})
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func equalFloat64s(got, want []float64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

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
