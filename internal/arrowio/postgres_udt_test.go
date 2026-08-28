package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

func TestPostgresEnumPlanPreservesLabelsAndIdentity(t *testing.T) {
	metadata := connectors.PostgresTypeMetadata{TypeName: "mood", Schema: "public", Kind: "enum", EnumLabels: []string{"Ready", "needs review", "!blocked!"}}
	plan := PlanForPostgresColumnWithMetadata("state", "mood", 0, 0, false, &metadata)
	require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
	require.Equal(t, MappingFallback, plan.Policy.MappingKind)
	require.Equal(t, postgresEnumTextCodec, plan.Policy.Fallback.Name)
	require.Equal(t, "enum", plan.Policy.Metadata.Properties["postgres.type_kind"])
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, "needs review"))
	require.NoError(t, plan.Append(b, nil))
	require.Error(t, plan.Append(b, "NEEDS REVIEW"))
	values := b.NewArray().(*array.String)
	defer values.Release()
	require.Equal(t, "needs review", values.Value(0))
	require.True(t, values.IsNull(1))
}

func TestPostgresDomainUsesSafeBasePlanAndRetainsIdentity(t *testing.T) {
	for _, tt := range []struct {
		name, base string
		want       arrow.DataType
		value      any
	}{
		{"small_id", "INT4", arrow.PrimitiveTypes.Int32, "42"},
		{"customer_id", "INT8", arrow.PrimitiveTypes.Int64, int64(42)},
		{"code", "TEXT", arrow.BinaryTypes.String, "A-1"},
		{"external_id", "UUID", arrow.BinaryTypes.String, "550e8400-e29b-41d4-a716-446655440000"},
		{"unsupported", "my_extension", arrow.BinaryTypes.String, "raw"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			metadata := connectors.PostgresTypeMetadata{TypeName: tt.name, Schema: "app", Kind: "domain", BaseType: tt.base, DomainNotNull: true}
			plan := PlanForPostgresColumnWithMetadata("value", tt.name, 0, 0, false, &metadata)
			require.Equal(t, tt.want, plan.DataType)
			require.Equal(t, "domain", plan.Policy.Metadata.Properties["postgres.type_kind"])
			require.Equal(t, tt.base, plan.Policy.Metadata.Properties["postgres.domain_base_type"])
			if tt.base == "my_extension" {
				require.Equal(t, postgresDomainTextCodec, plan.Policy.Fallback.Name)
			}
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, plan.Append(b, tt.value))
		})
	}
}

func TestPostgresCompositeUsesExactTextFallback(t *testing.T) {
	metadata := connectors.PostgresTypeMetadata{TypeName: "address", Schema: "app", Kind: "composite", CompositeFields: []string{"street:text", "zip:integer"}}
	plan := PlanForPostgresColumnWithMetadata("address", "address", 0, 0, false, &metadata)
	require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
	require.Equal(t, postgresCompositeTextCodec, plan.Policy.Fallback.Name)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, `("1, Main",,"")`))
	require.NoError(t, plan.Append(b, nil))
	require.Error(t, plan.Append(b, struct{}{}))
	values := b.NewArray().(*array.String)
	defer values.Release()
	require.Equal(t, `("1, Main",,"")`, values.Value(0))
	require.True(t, values.IsNull(1))
}
