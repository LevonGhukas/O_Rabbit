package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
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
		{"varint", 0, 0, false, &arrow.Decimal128Type{Precision: 38, Scale: 0}},
		{"counter", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"float", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"double", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"decimal", 0, 0, false, arrow.BinaryTypes.String},
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

func TestCassandraCompositeTypeMapping(t *testing.T) {
	tests := []struct {
		dbType   string
		wantType arrow.DataType
		fallback bool
	}{
		{"list<text>", arrow.ListOf(arrow.BinaryTypes.String), false},
		{"set<int>", arrow.ListOf(arrow.PrimitiveTypes.Int32), false},
		{"frozen<list<bigint>>", arrow.ListOf(arrow.PrimitiveTypes.Int64), false},
		{"vector<float, 3>", arrow.ListOf(arrow.PrimitiveTypes.Float32), false},
		{"inet", arrow.BinaryTypes.String, false},
		{"duration", arrow.BinaryTypes.String, false},
		{"map<text, int>", mapWithItemNullable(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, true), false},
		{"frozen<map<text, frozen<list<int>>>>", mapWithItemNullable(arrow.BinaryTypes.String, arrow.ListOf(arrow.PrimitiveTypes.Int32), true), false},
		{"tuple<double, double>", arrow.StructOf(arrow.Field{Name: "f1", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, arrow.Field{Name: "f2", Type: arrow.PrimitiveTypes.Float64, Nullable: true}), false},
		{"frozen<tuple<int, text>>", arrow.StructOf(arrow.Field{Name: "f1", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, arrow.Field{Name: "f2", Type: arrow.BinaryTypes.String, Nullable: true}), false},
		{"map<int, varint>", mapWithItemNullable(arrow.PrimitiveTypes.Int32, &arrow.Decimal128Type{Precision: 38, Scale: 0}, true), false},
		// A nested UUID stays canonical text; only top-level UUID columns
		// are stored natively (see the planner's automatic column types).
		{"map<text, uuid>", mapWithItemNullable(arrow.BinaryTypes.String, arrow.BinaryTypes.String, true), true},
		// User-defined types are not part of TID-945 and stay JSON text.
		{"frozen<address>", arrow.BinaryTypes.String, true},
		{"varint", &arrow.Decimal128Type{Precision: 38, Scale: 0}, false},
		// Without precision and scale from the data, decimal stays exact text.
		{"decimal", arrow.BinaryTypes.String, true},
	}
	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("cassandra", "col", tt.dbType, 0, 0, false)
			require.Equal(t, tt.wantType, plan.DataType)

			logical, err := LogicalTypeForCassandraColumn(tt.dbType, 0, 0, false)
			require.NoError(t, err)
			_, mapping, err := PlanForLogicalType("col", logical)
			require.NoError(t, err)
			require.Equal(t, tt.fallback, mapping.Fallback, "mapping %+v", mapping)
		})
	}
}

func TestCassandraTypeArgs(t *testing.T) {
	args, ok := cassandraTypeArgs("map<text, frozen<list<int>>>")
	require.True(t, ok)
	require.Equal(t, []string{"text", "frozen<list<int>>"}, args)
	_, ok = cassandraTypeArgs("list<int")
	require.False(t, ok)
}

func TestCassandraPlansAcceptConnectorValues(t *testing.T) {
	cases := []struct {
		dbType string
		value  any
	}{
		{"time", 90 * time.Second},
		{"list<text>", []any{"a", nil}},
		{"set<uuid>", []any{"123e4567-e89b-12d3-a456-426614174000"}},
		{"vector<float, 2>", []any{float32(1.25), float32(-2)}},
		{"map<text, int>", typesystem.MapValue{{Key: "a", Value: 1}}},
		{"map<text, int>", typesystem.MapValue{{Key: "a", Value: nil}}},
		{"frozen<address>", typesystem.MapValue{{Key: "city", Value: "x"}}},
		{"tuple<int, text>", typesystem.TupleValue{1, "x"}},
		{"tuple<int, text>", typesystem.TupleValue{nil, "x"}},
		{"duration", "P1M2DT3S"},
		{"inet", "10.0.0.1"},
		{"decimal", "123.45"},
		{"varint", "-42"},
	}
	for _, tc := range cases {
		t.Run(tc.dbType, func(t *testing.T) {
			p := PlanForSQLColumn("cassandra", "v", tc.dbType, 0, 0, false)
			b := p.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, p.Append(b, tc.value))
		})
	}
}

func TestTypeWarningsReportCanonicalTargetType(t *testing.T) {
	for dbType, want := range map[string]string{
		"decimal":          "string",
		"map<text, int>":   "map<string,int32>",
		"tuple<int, text>": "struct<f1:int32,f2:string>",
		"varint":           "decimal(38,0)",
		"uuid":             "string",
		"vector<float, 2>": "array<float32>",
	} {
		logical, err := LogicalTypeForCassandraColumn(dbType, 0, 0, false)
		require.NoError(t, err)
		_, mapping, err := PlanForLogicalType("c", logical)
		require.NoError(t, err)
		require.Equal(t, want, mapping.Target, dbType)
	}
	_, mapping, err := PlanForLogicalType("c", typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: "decimal"})
	require.NoError(t, err)
	w, ok := typesystem.WarningForMapping("c", mapping)
	require.True(t, ok)
	require.Equal(t, "string", w.TargetType)
	require.Equal(t, "utf8", w.StorageType)
}

// Columns that stay JSON text (user-defined types, and map or tuple columns
// of existing tables) must keep exactly the text earlier runs wrote.
func TestCassandraNestedValuesKeepTheirJSONText(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  string
	}{
		{typesystem.MapValue{{Key: "city", Value: "x"}, {Key: "zip", Value: int64(5)}}, `{"city":"x","zip":5}`},
		{typesystem.MapValue{{Key: int32(2), Value: "b"}, {Key: int32(10), Value: nil}}, `{"10":null,"2":"b"}`},
		{typesystem.TupleValue{int32(1), "x", nil}, `[1,"x",null]`},
		{typesystem.MapValue{{Key: "nested", Value: typesystem.TupleValue{"123.45", typesystem.MapValue{{Key: "k", Value: []any{"a"}}}}}}, `{"nested":["123.45",{"k":["a"]}]}`},
	} {
		got, err := typesystem.Convert(tc.value, typesystem.LogicalType{Kind: typesystem.KindJSON})
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}
