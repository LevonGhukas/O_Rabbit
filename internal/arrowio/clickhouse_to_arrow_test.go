package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestClickHouseTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"UInt64", 0, 0, false, &arrow.Decimal128Type{Precision: 20, Scale: 0}},
		{"UInt32", 0, 0, false, arrow.PrimitiveTypes.Uint32},
		{"UInt16", 0, 0, false, arrow.PrimitiveTypes.Uint16},
		{"UInt8", 0, 0, false, arrow.PrimitiveTypes.Uint8},
		{"Int64", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"Int32", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"Int16", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"Int8", 0, 0, false, arrow.PrimitiveTypes.Int8},
		{"Float32", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"Float64", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"Bool", 0, 0, false, arrow.FixedWidthTypes.Boolean},
		{"Decimal(38, 10)", 38, 10, true, &arrow.Decimal128Type{Precision: 38, Scale: 10}},
		{"Decimal32(2)", 0, 2, false, &arrow.Decimal128Type{Precision: 9, Scale: 2}},
		{"Decimal64(4)", 0, 4, false, &arrow.Decimal128Type{Precision: 18, Scale: 4}},
		{"Decimal128(6)", 0, 6, false, &arrow.Decimal128Type{Precision: 38, Scale: 6}},
		{"Date", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"Date32", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"DateTime64(6)", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"DateTime64(6, 'UTC')", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"UUID", 0, 0, false, arrow.BinaryTypes.String},
		{"Nullable(UInt64)", 0, 0, false, &arrow.Decimal128Type{Precision: 20, Scale: 0}},
		{"LowCardinality(String)", 0, 0, false, arrow.BinaryTypes.String},
		{"Array(Int32)", 0, 0, false, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("clickhouse", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}

func TestClickHousePointerDereferencing(t *testing.T) {
	plan := PlanForSQLColumn("clickhouse", "col", "LowCardinality(String)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	val := "hello_world"
	var ptr *string = &val

	err := plan.Append(builder, ptr)
	require.NoError(t, err)

	var nilPtr *string = nil
	err = plan.Append(builder, nilPtr)
	require.NoError(t, err)

	arr := builder.NewArray().(*array.String)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, "hello_world", arr.Value(0))
	require.True(t, arr.IsNull(1))
}
