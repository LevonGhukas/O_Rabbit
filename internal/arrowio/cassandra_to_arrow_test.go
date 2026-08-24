package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestCassandraTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"tinyint", 0, 0, false, arrow.PrimitiveTypes.Int8},
		{"smallint", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"int", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"bigint", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"varint", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"counter", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"float", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"double", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"decimal", 0, 0, false, &arrow.Decimal128Type{Precision: 38, Scale: 10}},
		{"boolean", 0, 0, false, arrow.FixedWidthTypes.Boolean},
		{"date", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"time", 0, 0, false, arrow.FixedWidthTypes.Time64us},
		{"timestamp", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"uuid", 0, 0, false, arrow.BinaryTypes.String},
		{"timeuuid", 0, 0, false, arrow.BinaryTypes.String},
		{"text", 0, 0, false, arrow.BinaryTypes.String},
		{"blob", 0, 0, false, arrow.BinaryTypes.Binary},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("cassandra", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}
