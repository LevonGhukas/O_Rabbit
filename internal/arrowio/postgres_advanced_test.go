package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

func TestPostgresAdvancedTextFallbacksPreserveExactText(t *testing.T) {
	for _, tt := range []struct{ typ, codec, kind, value string }{
		{"INT4RANGE", postgresRangeTextCodec, "range", `[1,5)`},
		{"NUMRANGE", postgresRangeTextCodec, "range", `(,"a,b"]`},
		{"DATERANGE", postgresRangeTextCodec, "range", `empty`},
		{"INT4MULTIRANGE", postgresMultirangeTextCodec, "multirange", `{[1,2),[5,9]}`},
		{"POINT", postgresGeometryTextCodec, "geometry", `(1.000,2.50)`},
		{"POLYGON", postgresGeometryTextCodec, "geometry", `((0,0),(1,0),(1,1))`},
		{"CIRCLE", postgresGeometryTextCodec, "geometry", `<(1,2),3>`},
		{"HSTORE", postgresHStoreTextCodec, "hstore", `"a"=>"x\\\"y", "missing"=>NULL, "ü"=>"z"`},
		{"widget_ext", postgresExtensionTextCodec, "extension", `verbatim, (extension)`},
	} {
		t.Run(tt.typ, func(t *testing.T) {
			plan := PlanForSQLColumn("postgres", "value", tt.typ, 0, 0, false)
			require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
			require.Equal(t, MappingFallback, plan.Policy.MappingKind)
			require.Equal(t, tt.codec, plan.Policy.Fallback.Name)
			require.Equal(t, tt.kind, plan.Policy.Metadata.Properties["postgres.type_kind"])
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, plan.Append(b, tt.value))
			require.NoError(t, plan.Append(b, nil))
			require.Error(t, plan.Append(b, struct{}{}))
			values := b.NewArray().(*array.String)
			defer values.Release()
			require.Equal(t, tt.value, values.Value(0))
			require.True(t, values.IsNull(1))
		})
	}
}

func TestPostgresPostGISUsesExactTextOrAlreadyBinaryValues(t *testing.T) {
	textPlan := PlanForSQLColumn("postgres", "shape", "GEOGRAPHY", 0, 0, false)
	require.Equal(t, postgresPostGISTextCodec, textPlan.Policy.Fallback.Name)
	b := textPlan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, textPlan.Append(b, `SRID=4326;POINT(1 2)`))

	metadata := connectors.PostgresTypeMetadata{TypeName: "geometry", Schema: "public", Kind: "postgis", PostGISBinary: true}
	binaryPlan := PlanForPostgresColumnWithMetadata("shape", "geometry", 0, 0, false, &metadata)
	require.Equal(t, arrow.BinaryTypes.Binary, binaryPlan.DataType)
	require.Equal(t, postgresPostGISBinaryCodec, binaryPlan.Policy.Fallback.Name)
	bb := binaryPlan.Builder(memory.DefaultAllocator)
	defer bb.Release()
	data := []byte{1, 2, 0, 0, 0, 9}
	require.NoError(t, binaryPlan.Append(bb, data))
	require.Error(t, binaryPlan.Append(bb, "POINT(1 2)"))
	values := bb.NewArray().(*array.Binary)
	defer values.Release()
	require.Equal(t, data, values.Value(0))
}

func TestPostgresAdvancedDiagnostics(t *testing.T) {
	plans := []ColumnPlan{PlanForSQLColumn("postgres", "r", "INT8RANGE", 0, 0, false), PlanForSQLColumn("postgres", "x", "HSTORE", 0, 0, false), PlanForSQLColumn("postgres", "e", "custom_extension", 0, 0, false)}
	diagnostics := MappingDiagnostics(plans)
	require.Equal(t, postgresRangeTextCodec, diagnostics[0].FallbackCodecName)
	require.Equal(t, postgresHStoreTextCodec, diagnostics[1].FallbackCodecName)
	require.Equal(t, postgresExtensionTextCodec, diagnostics[2].FallbackCodecName)
}
