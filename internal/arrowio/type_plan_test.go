package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestPlanForTargetType(t *testing.T) {
	tests := []struct {
		target   string
		wantType arrow.DataType
	}{
		{"UInt64", &arrow.Decimal128Type{Precision: 20, Scale: 0}},
		{"Nullable(UInt64)", &arrow.Decimal128Type{Precision: 20, Scale: 0}},
		{"LowCardinality(String)", arrow.BinaryTypes.String},
		{"Decimal(38, 10)", &arrow.Decimal128Type{Precision: 38, Scale: 10}},
		{"Decimal(10, 2)", &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{"NUMERIC(18, 4)", &arrow.Decimal128Type{Precision: 18, Scale: 4}},
		{"NUMBER(12, 2)", &arrow.Decimal128Type{Precision: 12, Scale: 2}},
		{"MONEY", &arrow.Decimal128Type{Precision: 19, Scale: 4}},
		{"SMALLMONEY", &arrow.Decimal128Type{Precision: 10, Scale: 4}},
		{"DateTime64(6)", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"DateTime64(6, 'UTC')", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"Array(Int64)", arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{"Array(Nullable(Float64))", arrow.ListOf(arrow.PrimitiveTypes.Float64)},
	}

	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			plan := PlanForTargetType("col", tt.target)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}

func TestPlansFromSQLEngineWithOverrides(t *testing.T) {
	cols := []string{"id", "amount", "created_at", "optional_desc"}
	targetTypes := map[string]string{
		"id":            "UInt64",
		"amount":        "Decimal(18, 4)",
		"created_at":    "DateTime64(6, 'UTC')",
		"optional_desc": "Nullable(String)",
	}

	plans, schema, err := PlansFromSQLEngineWithOverrides("mssql", cols, nil, targetTypes)
	require.NoError(t, err)
	require.Equal(t, 4, len(plans))
	require.Equal(t, &arrow.Decimal128Type{Precision: 20, Scale: 0}, schema.Field(0).Type)
	require.True(t, schema.Field(0).Nullable)
	require.Equal(t, &arrow.Decimal128Type{Precision: 18, Scale: 4}, schema.Field(1).Type)
	require.True(t, schema.Field(1).Nullable)
	require.Equal(t, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, schema.Field(2).Type)
	require.True(t, schema.Field(2).Nullable)
	require.Equal(t, arrow.BinaryTypes.String, schema.Field(3).Type)
	require.True(t, schema.Field(3).Nullable)
}
