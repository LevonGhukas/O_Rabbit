package icebergreg

import (
	"fmt"
	"testing"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/apache/arrow-go/v18/arrow"
)

// This table mirrors the physical Arrow representations used by current
// planners. It tests the same conversion function used for auto-created and
// durable Iceberg schemas, rather than a helper or hand-written mapping.
func TestCompatibilityMatrixArrowToIcebergSchema(t *testing.T) {
	cases := []struct {
		name string
		dt   arrow.DataType
		want string
	}{
		{"boolean", arrow.FixedWidthTypes.Boolean, "boolean"},
		{"int8", arrow.PrimitiveTypes.Int8, "int"},
		{"int16", arrow.PrimitiveTypes.Int16, "int"},
		{"int32", arrow.PrimitiveTypes.Int32, "int"},
		{"int64", arrow.PrimitiveTypes.Int64, "long"},
		// Iceberg v0.5.0 has no unsigned primitives. These are schema-level
		// conversion results, not assertions of safe value semantics.
		{"uint8", arrow.PrimitiveTypes.Uint8, "int"},
		{"uint16", arrow.PrimitiveTypes.Uint16, "int"},
		{"uint32", arrow.PrimitiveTypes.Uint32, "int"},
		{"uint64", arrow.PrimitiveTypes.Uint64, "long"},
		{"float32", arrow.PrimitiveTypes.Float32, "float"},
		{"float64", arrow.PrimitiveTypes.Float64, "double"},
		{"string-and-fallback-codecs", arrow.BinaryTypes.String, "string"},
		{"binary", arrow.BinaryTypes.Binary, "binary"},
		{"decimal10-2", &arrow.Decimal128Type{Precision: 10, Scale: 2}, "decimal(10, 2)"},
		{"decimal38-0", &arrow.Decimal128Type{Precision: 38, Scale: 0}, "decimal(38, 0)"},
		{"date32", arrow.PrimitiveTypes.Date32, "date"},
		{"timestamp-us", &arrow.TimestampType{Unit: arrow.Microsecond}, "timestamp"},
		{"timestamp-ms-utc-mongodb-date", &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, "timestamptz"},
		{"time64-us", arrow.FixedWidthTypes.Time64us, "time"},
		{"list", arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), "list<int>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: tc.dt, Nullable: true}}, nil)
			got, err := icetable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
			if err != nil {
				t.Fatalf("real Arrow->Iceberg conversion failed: %v", err)
			}
			field, ok := got.FindFieldByName("value")
			if !ok {
				t.Fatal("registered schema lost value field")
			}
			if field.Required {
				t.Fatal("nullable Arrow field became required")
			}
			if actual := fmt.Sprint(field.Type); actual != tc.want {
				t.Fatalf("Iceberg type=%q want %q", actual, tc.want)
			}
		})
	}
}
