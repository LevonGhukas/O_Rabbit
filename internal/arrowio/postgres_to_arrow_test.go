package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestPostgresTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"INT2", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"SMALLINT", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"INT4", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"INTEGER", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"INT8", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"BIGINT", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"FLOAT4", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"REAL", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"FLOAT8", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"DOUBLE PRECISION", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"NUMERIC(38,18)", 38, 18, true, &arrow.Decimal128Type{Precision: 38, Scale: 18}},
		{"MONEY", 0, 0, false, &arrow.Decimal128Type{Precision: 19, Scale: 2}},
		{"BOOL", 0, 0, false, arrow.FixedWidthTypes.Boolean},
		{"BOOLEAN", 0, 0, false, arrow.FixedWidthTypes.Boolean},
		{"DATE", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"TIMESTAMP", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"TIMESTAMPTZ", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"TIME", 0, 0, false, arrow.FixedWidthTypes.Time64us},
		{"UUID", 0, 0, false, arrow.BinaryTypes.String},
		{"JSONB", 0, 0, false, arrow.BinaryTypes.String},
		{"BYTEA", 0, 0, false, arrow.BinaryTypes.Binary},
		{"INTEGER[]", 0, 0, false, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{"BIGINT[]", 0, 0, false, arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{"NUMERIC[]", 0, 0, false, arrow.ListOf(&arrow.Decimal128Type{Precision: 38, Scale: 10})},
		{"TEXT[]", 0, 0, false, arrow.ListOf(arrow.BinaryTypes.String)},
		{"BOOLEAN[]", 0, 0, false, arrow.ListOf(arrow.FixedWidthTypes.Boolean)},
		{"UUID[]", 0, 0, false, arrow.ListOf(arrow.BinaryTypes.String)},
		{"_INT4", 0, 0, false, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{"_TEXT", 0, 0, false, arrow.ListOf(arrow.BinaryTypes.String)},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("postgres", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}

func TestPostgresArrayParsing(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "col", "INTEGER[]", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	// PostgreSQL array string format: {-2147483648,NULL,0,2147483647}
	err := plan.Append(builder, "{-2147483648,NULL,0,2147483647}")
	require.NoError(t, err)

	arr := builder.NewArray().(*array.List)
	defer arr.Release()

	require.Equal(t, 1, arr.Len())
	require.False(t, arr.IsNull(0))

	values := arr.ListValues().(*array.Int32)
	require.Equal(t, 4, values.Len())
	require.Equal(t, int32(-2147483648), values.Value(0))
	require.True(t, values.IsNull(1))
	require.Equal(t, int32(0), values.Value(2))
	require.Equal(t, int32(2147483647), values.Value(3))
}

func TestPostgresMoneyParsing(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "col", "MONEY", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	err := plan.Append(builder, "$12,345.67")
	require.NoError(t, err)

	err = plan.Append(builder, "-$92,233,720,368,547,758.08")
	require.NoError(t, err)

	arr := builder.NewArray().(*array.Decimal128)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, "12345.67", arr.Value(0).ToString(2))
	require.Equal(t, "-92233720368547758.08", arr.Value(1).ToString(2))
}

func TestPostgresMicrosecondPrecision(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "col", "TIMESTAMP(6)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	t1, err := time.Parse("2006-01-02 15:04:05.999999", "2026-08-20 12:34:56.123456")
	require.NoError(t, err)

	err = plan.Append(builder, t1)
	require.NoError(t, err)

	arr := builder.NewArray().(*array.Timestamp)
	defer arr.Release()

	require.Equal(t, 1, arr.Len())
	require.Equal(t, arrow.Timestamp(t1.UnixMicro()), arr.Value(0))
}
