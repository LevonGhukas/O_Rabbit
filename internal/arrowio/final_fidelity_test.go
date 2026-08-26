package arrowio

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestUInt64FullDomainMapsToIcebergDecimal20(t *testing.T) {
	plan := planUint64("id")
	require.Equal(t, &arrow.Decimal128Type{Precision: 20, Scale: 0}, plan.DataType)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, uint64(math.MaxUint64)))
	a := b.NewArray().(*array.Decimal128)
	defer a.Release()
	require.Equal(t, "18446744073709551615", a.Value(0).ToString(0))

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: plan.DataType, Nullable: false}}, nil)
	iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	require.NoError(t, err)
	field, ok := iceSchema.FindFieldByName("id")
	require.True(t, ok)
	require.Equal(t, iceberg.DecimalTypeOf(20, 0), field.Type)
}

func TestFixedBinaryPreservesPaddingAndIcebergFixedSchema(t *testing.T) {
	plan := planFixedBinary("payload", 4)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	raw := []byte{0x61, 0x00, 0x00, 0xff}
	require.NoError(t, plan.Append(b, raw))
	require.Error(t, plan.Append(b, raw[:3]))
	a := b.NewArray().(*array.FixedSizeBinary)
	defer a.Release()
	require.Equal(t, raw, a.Value(0))

	schema := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: plan.DataType, Nullable: false}}, nil)
	iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	require.NoError(t, err)
	field, ok := iceSchema.FindFieldByName("payload")
	require.True(t, ok)
	require.Equal(t, iceberg.FixedTypeOf(4), field.Type)
}

func TestUUIDUsesExplicitCanonicalTextFallback(t *testing.T) {
	d := SourceFieldDescriptor{Engine: "postgres", SourceType: "UUID"}
	classifySourceRepresentation(&d)
	require.Equal(t, LogicalUUID, d.LogicalFamily)
	require.Equal(t, "canonical_uuid_text_v1", d.FallbackEncoding)

	plan := planUUID("id")
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, "123E4567-E89B-12D3-A456-426614174000"))
	require.Error(t, plan.Append(b, "not-a-uuid"))
	a := b.NewArray().(*array.String)
	defer a.Release()
	require.Equal(t, "123e4567-e89b-12d3-a456-426614174000", a.Value(0))
}

func TestJSONTextFallbackPreservesValidSourceText(t *testing.T) {
	plan := planJSONText("document")
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	raw := `{"n":1.2300,"null":null,"word":"\u2603"}`
	require.NoError(t, plan.Append(b, raw))
	require.Error(t, plan.Append(b, `{"broken":`))
	a := b.NewArray().(*array.String)
	defer a.Release()
	require.Equal(t, raw, a.Value(0))
}
