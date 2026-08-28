package parquetio

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// parquetCompatibilityCase is deliberately test-only: it documents the
// canonical physical forms emitted by source planners and exercises the real
// Writer rather than introducing another production type registry.
type parquetCompatibilityCase struct {
	name     string
	type_    arrow.DataType
	json     string
	semantic string
}

func parquetCompatibilityMatrix() []parquetCompatibilityCase {
	return []parquetCompatibilityCase{
		{"boolean", arrow.FixedWidthTypes.Boolean, `[true,false]`, "Boolean"},
		{"int8-extrema", arrow.PrimitiveTypes.Int8, `[-128,127]`, "Int8"},
		{"int16-extrema", arrow.PrimitiveTypes.Int16, `[-32768,32767]`, "Int16"},
		{"int32-extrema", arrow.PrimitiveTypes.Int32, `[-2147483648,2147483647]`, "Int32"},
		{"int64-extrema", arrow.PrimitiveTypes.Int64, `[-9223372036854775808,9223372036854775807]`, "Int64"},
		{"uint8-extrema", arrow.PrimitiveTypes.Uint8, `[0,255]`, "UInt8"},
		{"uint16-extrema", arrow.PrimitiveTypes.Uint16, `[0,65535]`, "UInt16"},
		{"uint32-extrema", arrow.PrimitiveTypes.Uint32, `[0,4294967295]`, "UInt32"},
		{"uint64-max", arrow.PrimitiveTypes.Uint64, `[0,18446744073709551615]`, "UInt64; full max"},
		{"float32", arrow.PrimitiveTypes.Float32, `[-1.5,3.25]`, "Float32"},
		{"float64", arrow.PrimitiveTypes.Float64, `[-1.5,3.25]`, "Float64"},
		{"string-fallback-codecs", arrow.BinaryTypes.String, `["1234567890.01","550e8400-e29b-41d4-a716-446655440000","{1,2,NULL}","00101","[1,2)","-838:59:59.999999","AQI=","507f1f77bcf86cd799439011","{\"$numberDecimal\":\"-12.30\"}","-12.30"]`, "canonical-decimal-text; postgres UUID/array/bit/range; mysql time/bit; BSON ObjectId/Extended JSON/Decimal128"},
		{"binary-arbitrary-bytes", arrow.BinaryTypes.Binary, `["AAH/gP8="]`, "Binary with NUL, invalid UTF-8, arbitrary bytes"},
		{"decimal128-10-2", &arrow.Decimal128Type{Precision: 10, Scale: 2}, `["-99999999.99","99999999.99"]`, "Decimal(10,2), negative and precision boundary"},
		{"decimal128-38-0", &arrow.Decimal128Type{Precision: 38, Scale: 0}, `["-99999999999999999999999999999999999999","99999999999999999999999999999999999999"]`, "Decimal(38,0), negative and precision boundary"},
		{"date32-range", arrow.PrimitiveTypes.Date32, `["1800-01-02","2300-12-30"]`, "Date32 before 1900 and after 2299"},
		{"timestamp-us", &arrow.TimestampType{Unit: arrow.Microsecond}, `["1800-01-02T03:04:05.123456","2300-12-30T23:59:59.654321"]`, "Timestamp(us), exact microseconds"},
		{"timestamp-ms-mongodb-date", &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, `["1970-01-01T00:00:00.123Z","2300-12-30T23:59:59.999Z"]`, "MongoDB Date/Timestamp(ms), UTC instant"},
		{"time64-us", arrow.FixedWidthTypes.Time64us, `["00:00:00.000001","23:59:59.999999"]`, "Time64(us) local time of day"},
		{"list-int32-null-elements", arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), `[[1,null,3],[],null]`, "PostgreSQL primitive array; empty and null elements"},
	}
}

func TestCompatibilityMatrixArrowToParquetRoundTrip(t *testing.T) {
	for _, tc := range parquetCompatibilityMatrix() {
		t.Run(tc.name, func(t *testing.T) {
			arr, _, err := array.FromJSON(memory.DefaultAllocator, tc.type_, bytes.NewBufferString(tc.json))
			if err != nil {
				t.Fatalf("build %s Arrow value: %v", tc.semantic, err)
			}
			defer arr.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: tc.type_, Nullable: true}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(arr.Len()))
			defer record.Release()

			writer, path, err := NewTempFileWriterInDir(schema, Options{}, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(path)
			if err := writer.Write(record); err != nil {
				t.Fatalf("write %s: %v", tc.semantic, err)
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("close %s: %v", tc.semantic, err)
			}

			input, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			pr, err := file.NewParquetReader(input)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
			if err != nil {
				t.Fatal(err)
			}
			got, err := reader.ReadTable(context.Background())
			if err != nil {
				t.Fatalf("read %s: %v", tc.semantic, err)
			}
			defer got.Release()
			gotField := got.Schema().Field(0)
			if gotField.Name != "value" || gotField.Nullable != true || !arrow.TypeEqual(tc.type_, gotField.Type) {
				t.Fatalf("schema changed for %s: got %s want %s", tc.semantic, got.Schema(), schema)
			}
			if got.NumRows() != record.NumRows() {
				t.Fatalf("row count changed for %s: got %d want %d", tc.semantic, got.NumRows(), record.NumRows())
			}
			gotCol := got.Column(0).Data().Chunk(0)
			if !array.Equal(arr, gotCol) {
				t.Fatalf("value changed for %s: got %s want %s", tc.semantic, gotCol, arr)
			}
		})
	}
}
