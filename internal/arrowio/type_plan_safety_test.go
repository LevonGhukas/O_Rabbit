package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestCurrentPlannerMappingsAndFallbacks(t *testing.T) {
	tests := []struct {
		name   string
		engine string
		dbType string
		prec   int64
		scale  int64
		dec    bool
		want   arrow.DataType
	}{
		{"postgres_array_alias", "postgres", "_INT4", 0, 0, false, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{"mysql_boolean_spelling", "mysql", "TINYINT(1)", 0, 0, false, arrow.FixedWidthTypes.Boolean},
		{"mariadb_unsigned", "mariadb", "BIGINT UNSIGNED", 0, 0, false, arrow.PrimitiveTypes.Uint64},
		{"mssql_rowversion", "mssql", "ROWVERSION", 0, 0, false, arrow.BinaryTypes.Binary},
		{"oracle_number_precision", "oracle", "NUMBER", 4, 0, true, arrow.PrimitiveTypes.Int16},
		{"clickhouse_structural_fallback", "clickhouse", "Tuple(String, UInt64)", 0, 0, false, arrow.BinaryTypes.String},
		{"trino_structural_fallback", "trino", "ROW(x BIGINT)", 0, 0, false, arrow.BinaryTypes.String},
		{"cassandra_varint", "cassandra", "varint", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"sqlite_unknown_type", "sqlite", "CUSTOM_AFFINITY", 0, 0, false, arrow.BinaryTypes.String},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PlanForSQLColumn(tt.engine, "col", tt.dbType, tt.prec, tt.scale, tt.dec)
			require.True(t, arrow.TypeEqual(tt.want, got.DataType), "got %s, want %s", got.DataType, tt.want)
		})
	}
}

func TestCurrentTargetTypeOverrideUnknownTypeFallsBackToString(t *testing.T) {
	plan := PlanForTargetType("status", "Enum8('queued' = 1)")
	require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
}

func TestCurrentStringFallbacks(t *testing.T) {
	require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff", asSafeString([]byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
		0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
	}))
	require.Equal(t, `{"a":1,"b":2}`, asSafeString(map[string]any{"b": 2, "a": 1}))

	plan := planBinary("binary")
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, 123))
	got := b.NewArray().(*array.Binary)
	defer got.Release()
	require.Equal(t, []byte("123"), got.Value(0))
}

func TestCurrentImplicitNullFallbacks(t *testing.T) {
	tests := []struct {
		name  string
		plan  ColumnPlan
		value any
	}{
		{"int8_invalid_text", planInt8("c"), "not-an-int"},
		{"uint8_negative", planUint8("c"), int64(-1)},
		{"float32_boolean", planFloat32("c"), true},
		{"date_invalid_text", planDate32("c"), "not-a-date"},
		{"time_invalid_shape", planTime64("c"), "not-a-time"},
		{"timestamp_invalid_text", planTimestampUs("c", ""), "not-a-timestamp"},
		{"decimal_invalid_text", planDecimal128("c", 10, 2), "not-a-decimal"},
		{"list_scalar", planList("c", planInt64("item")), 123},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, tt.plan.Append(b, tt.value))
			got := b.NewArray()
			defer got.Release()
			require.True(t, got.IsNull(0))
		})
	}
}

func TestCurrentDefaultValueConversions(t *testing.T) {
	boolPlan := planBool("b")
	boolBuilder := boolPlan.Builder(memory.DefaultAllocator)
	defer boolBuilder.Release()
	require.NoError(t, boolPlan.Append(boolBuilder, "not-a-boolean"))
	bools := boolBuilder.NewArray().(*array.Boolean)
	defer bools.Release()
	require.False(t, bools.IsNull(0))
	require.False(t, bools.Value(0))

	timePlan := planTime64("t")
	timeBuilder := timePlan.Builder(memory.DefaultAllocator)
	defer timeBuilder.Release()
	require.NoError(t, timePlan.Append(timeBuilder, "x:y"))
	times := timeBuilder.NewArray().(*array.Time64)
	defer times.Release()
	require.False(t, times.IsNull(0))
	require.Equal(t, arrow.Time64(0), times.Value(0))
}

func TestCurrentUncheckedNarrowIntegerCastsWrap(t *testing.T) {
	tests := []struct {
		name  string
		plan  ColumnPlan
		value any
		want  any
	}{
		{"int8", planInt8("c"), int64(128), int8(-128)},
		{"int16", planInt16("c"), int64(32768), int16(-32768)},
		{"uint8", planUint8("c"), uint64(256), uint8(0)},
		{"uint32", planUint32("c"), uint64(1 << 32), uint32(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, tt.plan.Append(b, tt.value))
			got := b.NewArray()
			defer got.Release()
			switch values := got.(type) {
			case *array.Int8:
				require.Equal(t, tt.want, values.Value(0))
			case *array.Int16:
				require.Equal(t, tt.want, values.Value(0))
			case *array.Uint8:
				require.Equal(t, tt.want, values.Value(0))
			case *array.Uint32:
				require.Equal(t, tt.want, values.Value(0))
			default:
				t.Fatalf("unexpected array type %T", got)
			}
		})
	}
}
