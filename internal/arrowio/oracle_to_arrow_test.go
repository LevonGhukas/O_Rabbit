package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestOracleTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"NUMBER(4,0)", 4, 0, true, arrow.PrimitiveTypes.Int16},
		{"NUMBER(9,0)", 9, 0, true, arrow.PrimitiveTypes.Int32},
		{"NUMBER(18,0)", 18, 0, true, arrow.PrimitiveTypes.Int64},
		{"NUMBER(38,0)", 38, 0, true, &arrow.Decimal128Type{Precision: 38, Scale: 0}},
		{"NUMBER(38,10)", 38, 10, true, &arrow.Decimal128Type{Precision: 38, Scale: 10}},
		{"NUMBER", 0, 0, false, &arrow.Decimal128Type{Precision: 38, Scale: 10}},
		{"FLOAT", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"BINARY_FLOAT", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"BINARY_DOUBLE", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"DATE", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"TIMESTAMP", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"TIMESTAMP WITH TIME ZONE", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"VARCHAR2(255)", 0, 0, false, arrow.BinaryTypes.String},
		{"RAW(16)", 0, 0, false, arrow.BinaryTypes.Binary},
		{"BLOB", 0, 0, false, arrow.BinaryTypes.Binary},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("oracle", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}

func TestOracleDateIncludesTime(t *testing.T) {
	plan := PlanForSQLColumn("oracle", "col", "DATE", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	t1, err := time.Parse("2006-01-02 15:04:05", "2026-08-20 14:30:45")
	require.NoError(t, err)

	err = plan.Append(builder, t1)
	require.NoError(t, err)

	arr := builder.NewArray().(*array.Timestamp)
	defer arr.Release()

	require.Equal(t, 1, arr.Len())
	require.Equal(t, arrow.Timestamp(t1.UnixMicro()), arr.Value(0))
}
