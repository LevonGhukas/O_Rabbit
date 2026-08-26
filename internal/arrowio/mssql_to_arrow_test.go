package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestMSSQLTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"TINYINT", 0, 0, false, arrow.PrimitiveTypes.Uint8},
		{"SMALLINT", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"INT", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"BIGINT", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"BIT", 0, 0, false, arrow.FixedWidthTypes.Boolean},
		{"FLOAT", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"REAL", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"DECIMAL(38,18)", 38, 18, true, &arrow.Decimal128Type{Precision: 38, Scale: 18}},
		{"MONEY", 0, 0, false, &arrow.Decimal128Type{Precision: 19, Scale: 4}},
		{"SMALLMONEY", 0, 0, false, &arrow.Decimal128Type{Precision: 10, Scale: 4}},
		{"DATE", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"DATETIME", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"DATETIME2(7)", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"DATETIMEOFFSET(7)", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"TIME(7)", 0, 0, false, arrow.FixedWidthTypes.Time64us},
		{"UNIQUEIDENTIFIER", 0, 0, false, arrow.BinaryTypes.String},
		{"BINARY(16)", 0, 0, false, arrow.BinaryTypes.Binary},
		{"VARBINARY(MAX)", 0, 0, false, arrow.BinaryTypes.Binary},
		{"SQL_VARIANT", 0, 0, false, arrow.BinaryTypes.String},
		{"HIERARCHYID", 0, 0, false, arrow.BinaryTypes.String},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("mssql", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}

func TestMSSQLUniqueidentifierFormat(t *testing.T) {
	plan := PlanForSQLColumn("mssql", "col", "UNIQUEIDENTIFIER", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	// 16 bytes raw UUID
	rawUUID := []byte{
		0x12, 0x3e, 0x45, 0x67,
		0xe8, 0x9b,
		0x12, 0xd3,
		0xa4, 0x56,
		0x42, 0x66, 0x14, 0x17, 0x40, 0x00,
	}

	err := plan.Append(builder, rawUUID)
	require.NoError(t, err)

	err = plan.Append(builder, "11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)

	arr := builder.NewArray().(*array.String)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, "123e4567-e89b-12d3-a456-426614174000", arr.Value(0))
	require.Equal(t, "11111111-1111-1111-1111-111111111111", arr.Value(1))
}

func TestMSSQLPre1970Dates(t *testing.T) {
	plan := PlanForSQLColumn("mssql", "col", "DATE", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	err := plan.Append(builder, "1969-12-31")
	require.NoError(t, err)

	d, err := time.Parse("2006-01-02", "1969-12-31")
	require.NoError(t, err)
	err = plan.Append(builder, d)
	require.NoError(t, err)

	arr := builder.NewArray().(*array.Date32)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, "1969-12-31", arr.Value(0).FormattedString())
	require.Equal(t, "1969-12-31", arr.Value(1).FormattedString())

	// Source dates remain exact; downstream ClickHouse limits are not applied here.
	builder2 := plan.Builder(memory.DefaultAllocator)
	defer builder2.Release()

	err = plan.Append(builder2, "0001-01-01")
	require.NoError(t, err)

	arr2 := builder2.NewArray().(*array.Date32)
	defer arr2.Release()
	require.Equal(t, "0001-01-01", arr2.Value(0).FormattedString())
}
