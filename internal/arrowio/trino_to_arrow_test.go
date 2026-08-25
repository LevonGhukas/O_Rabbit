package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestTrinoTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"boolean", 0, 0, false, arrow.FixedWidthTypes.Boolean},
		{"tinyint", 0, 0, false, arrow.PrimitiveTypes.Int8},
		{"smallint", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"integer", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"bigint", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"real", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"double", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"decimal(38,10)", 38, 10, true, &arrow.Decimal128Type{Precision: 38, Scale: 10}},
		{"date", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"time", 0, 0, false, arrow.FixedWidthTypes.Time64us},
		{"time(6)", 0, 0, false, arrow.FixedWidthTypes.Time64us},
		{"timestamp", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"timestamp(6)", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"timestamp with time zone", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"uuid", 0, 0, false, arrow.BinaryTypes.String},
		{"varchar(255)", 0, 0, false, arrow.BinaryTypes.String},
		{"array(integer)", 0, 0, false, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("trino", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}
