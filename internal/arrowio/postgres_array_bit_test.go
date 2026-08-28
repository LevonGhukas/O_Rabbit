package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestPostgresNativeArrayTextSemantics(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "tags", "TEXT[]", 0, 0, false)
	require.Equal(t, arrow.ListOf(arrow.BinaryTypes.String), plan.DataType)
	require.Equal(t, MappingNative, plan.Policy.MappingKind)
	require.Equal(t, "true", plan.Policy.Metadata.Properties["postgres.array"])

	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, `{"a,b","NULL",NULL,"a\\b","say \"hi\""}`))
	require.NoError(t, plan.Append(b, "{}"))
	require.NoError(t, plan.Append(b, nil))

	result := b.NewArray().(*array.List)
	defer result.Release()
	values := result.ListValues().(*array.String)
	require.Equal(t, 3, result.Len())
	require.False(t, result.IsNull(0))
	require.False(t, result.IsNull(1))
	require.True(t, result.IsNull(2))
	require.Equal(t, "a,b", values.Value(0))
	require.Equal(t, "NULL", values.Value(1))
	require.True(t, values.IsNull(2))
	require.Equal(t, `a\b`, values.Value(3))
	require.Equal(t, `say "hi"`, values.Value(4))
	require.Equal(t, 5, values.Len())
}

func TestPostgresArrayFallbacksPreserveText(t *testing.T) {
	for _, tt := range []struct{ source, value string }{
		{"INTEGER[][]", "{{1,2},{3,4}}"},
		{"INTEGER[][]", "[0:1][0:1]={{10,20},{30,40}}"},
		{"JSONB[]", `{"{""a"": [1,2]}"}`},
		{"UUID[]", "{550e8400-e29b-41d4-a716-446655440000}"},
	} {
		t.Run(tt.source, func(t *testing.T) {
			plan := PlanForSQLColumn("postgres", "col", tt.source, 0, 0, false)
			require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
			require.Equal(t, MappingFallback, plan.Policy.MappingKind)
			require.Equal(t, postgresArrayTextCodec, plan.Policy.Fallback.Name)
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, plan.Append(b, tt.value))
			result := b.NewArray().(*array.String)
			defer result.Release()
			require.Equal(t, tt.value, result.Value(0))
		})
	}
}

func TestPostgresBitAndVarbitUseExactText(t *testing.T) {
	for _, tt := range []struct {
		source, value string
		width         int64
		known         bool
	}{
		{"BIT(1)", "0", 1, true},
		{"BIT(8)", "00000001", 8, true},
		{"BIT(64)", "0000000000000000000000000000000000000000000000000000000000000001", 64, true},
		{"BIT(65)", "00000000000000000000000000000000000000000000000000000000000000001", 65, true},
		{"VARBIT", "101010", 0, false},
		{"VARBIT(65)", "00000000000000000000000000000000000000000000000000000000000000001", 65, true},
	} {
		t.Run(tt.source, func(t *testing.T) {
			plan := PlanForSQLColumn("postgres", "bits", tt.source, 0, 0, false)
			require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
			require.Equal(t, MappingFallback, plan.Policy.MappingKind)
			require.Equal(t, postgresBitTextCodec, plan.Policy.Fallback.Name)
			require.Equal(t, tt.known, plan.Policy.Metadata.BitWidthKnown)
			require.Equal(t, tt.width, plan.Policy.Metadata.BitWidth)
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, plan.Append(b, tt.value))
			require.NoError(t, plan.Append(b, nil))
			result := b.NewArray().(*array.String)
			defer result.Release()
			require.Equal(t, tt.value, result.Value(0))
			require.True(t, result.IsNull(1))
		})
	}

	plan := PlanForSQLColumn("postgres", "bits", "BIT(8)", 0, 0, false)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.Error(t, plan.Append(b, "0000000x"))
	require.Error(t, plan.Append(b, "1"))
}

func TestPostgresArrayAndBitDiagnostics(t *testing.T) {
	plans := []ColumnPlan{
		PlanForSQLColumn("postgres", "ids", "INTEGER[]", 0, 0, false),
		PlanForSQLColumn("postgres", "matrix", "INTEGER[][]", 0, 0, false),
		PlanForSQLColumn("postgres", "flags", "VARBIT(8)", 0, 0, false),
	}
	diagnostics := MappingDiagnostics(plans)
	require.Equal(t, MappingNative, diagnostics[0].MappingKind)
	require.Empty(t, diagnostics[0].FallbackCodecName)
	require.Equal(t, MappingFallback, diagnostics[1].MappingKind)
	require.Equal(t, postgresArrayTextCodec, diagnostics[1].FallbackCodecName)
	require.Equal(t, MappingFallback, diagnostics[2].MappingKind)
	require.Equal(t, postgresBitTextCodec, diagnostics[2].FallbackCodecName)
}
