package icebergreg

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func TestIcebergMappingForLogicalType(t *testing.T) {
	decimal := typesystem.Decimal(18, 2)
	tests := []struct {
		name     string
		logical  typesystem.LogicalType
		typeName string
		class    typesystem.MappingClass
		fallback bool
	}{
		{"bool", typesystem.LogicalType{Kind: typesystem.KindBool}, "boolean", typesystem.MappingExact, false},
		{"int8", typesystem.LogicalType{Kind: typesystem.KindInt8}, "int", typesystem.MappingSafePromotion, false},
		{"int16", typesystem.LogicalType{Kind: typesystem.KindInt16}, "int", typesystem.MappingSafePromotion, false},
		{"int32", typesystem.LogicalType{Kind: typesystem.KindInt32}, "int", typesystem.MappingExact, false},
		{"int64", typesystem.LogicalType{Kind: typesystem.KindInt64}, "long", typesystem.MappingExact, false},
		{"uint8", typesystem.LogicalType{Kind: typesystem.KindUInt8}, "int", typesystem.MappingSafePromotion, false},
		{"uint16", typesystem.LogicalType{Kind: typesystem.KindUInt16}, "int", typesystem.MappingSafePromotion, false},
		{"uint32", typesystem.LogicalType{Kind: typesystem.KindUInt32}, "long", typesystem.MappingSafePromotion, false},
		{"uint64", typesystem.LogicalType{Kind: typesystem.KindUInt64}, "string", typesystem.MappingSemanticFallback, true},
		{"float32", typesystem.LogicalType{Kind: typesystem.KindFloat32}, "float", typesystem.MappingExact, false},
		{"float64", typesystem.LogicalType{Kind: typesystem.KindFloat64}, "double", typesystem.MappingExact, false},
		{"decimal", decimal, "decimal(18, 2)", typesystem.MappingExact, false},
		{"date", typesystem.LogicalType{Kind: typesystem.KindDate}, "date", typesystem.MappingExact, false},
		{"time", typesystem.LogicalType{Kind: typesystem.KindTime}, "time", typesystem.MappingExact, false},
		{"timestamp", typesystem.LogicalType{Kind: typesystem.KindTimestamp}, "timestamp", typesystem.MappingExact, false},
		{"timestamp tz", typesystem.LogicalType{Kind: typesystem.KindTimestampTZ}, "timestamptz", typesystem.MappingExact, false},
		{"uuid", typesystem.LogicalType{Kind: typesystem.KindUUID}, "string", typesystem.MappingSemanticFallback, true},
		{"string", typesystem.LogicalType{Kind: typesystem.KindString}, "string", typesystem.MappingExact, false},
		{"binary", typesystem.LogicalType{Kind: typesystem.KindBinary}, "binary", typesystem.MappingExact, false},
		{"json", typesystem.LogicalType{Kind: typesystem.KindJSON}, "string", typesystem.MappingSemanticFallback, true},
		{"struct", typesystem.LogicalType{Kind: typesystem.KindStruct}, "string", typesystem.MappingSemanticFallback, true},
		{"map", typesystem.MapOf(typesystem.LogicalType{Kind: typesystem.KindString}, typesystem.LogicalType{Kind: typesystem.KindInt64}), "string", typesystem.MappingSemanticFallback, true},
		{"unknown", typesystem.LogicalType{Kind: typesystem.KindUnknown}, "string", typesystem.MappingUnsupportedFallback, true},
		{"array", typesystem.ArrayOf(typesystem.LogicalType{Kind: typesystem.KindInt64}), "list<long>", typesystem.MappingExact, false},
		{"array json", typesystem.ArrayOf(typesystem.LogicalType{Kind: typesystem.KindJSON}), "list<string>", typesystem.MappingSemanticFallback, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapping, err := IcebergMappingForLogicalType(test.logical)
			require.NoError(t, err)
			require.Equal(t, test.typeName, mapping.TypeName)
			require.Equal(t, test.class, mapping.Class)
			require.Equal(t, test.fallback, mapping.Fallback)
		})
	}

	tooWide := typesystem.Decimal(39, 2)
	mapping, err := IcebergMappingForLogicalType(tooWide)
	require.NoError(t, err)
	require.Equal(t, "string", mapping.TypeName)
	require.True(t, mapping.Fallback)

	mapping, err = IcebergMappingForLogicalType(typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "America/New_York"})
	require.NoError(t, err)
	require.Equal(t, "string", mapping.TypeName)
	require.True(t, mapping.Fallback)

	_, err = IcebergMappingForLogicalType(typesystem.LogicalType{Kind: typesystem.KindArray})
	require.Error(t, err)
}

func TestResolveStorageMappingKeepsDestinationsCompatible(t *testing.T) {
	logical := typesystem.LogicalType{Kind: typesystem.KindUInt64}
	arrowType, arrowMapping, err := arrowio.ArrowTypeForLogicalType(logical)
	require.NoError(t, err)
	require.Equal(t, arrow.PrimitiveTypes.Uint64, arrowType)
	require.Equal(t, typesystem.MappingExact, arrowMapping.Class)

	expected, err := IcebergMappingForLogicalType(logical)
	require.NoError(t, err)
	compatible, err := ArrowTypeCompatibleWithIceberg(arrowType, expected)
	require.NoError(t, err)
	require.False(t, compatible)

	resolved, err := ResolveStorageMapping(logical)
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.String, resolved.ArrowType)
	require.Equal(t, "string", resolved.ExpectedIceberg.TypeName)
	require.Equal(t, typesystem.MappingSemanticFallback, resolved.Class)
	require.True(t, resolved.Fallback)
	compatible, err = ArrowTypeCompatibleWithIceberg(resolved.ArrowType, resolved.ExpectedIceberg)
	require.NoError(t, err)
	require.True(t, compatible)

	for _, logical := range []typesystem.LogicalType{
		typesystem.Decimal(39, 2),
		{Kind: typesystem.KindUUID},
		{Kind: typesystem.KindJSON},
		{Kind: typesystem.KindStruct},
		typesystem.MapOf(typesystem.LogicalType{Kind: typesystem.KindString}, typesystem.LogicalType{Kind: typesystem.KindString}),
		{Kind: typesystem.KindTimestampTZ, Timezone: "America/New_York"},
	} {
		resolved, err := ResolveStorageMapping(logical)
		require.NoError(t, err)
		require.Equal(t, arrow.BinaryTypes.String, resolved.ArrowType)
		require.Equal(t, "string", resolved.ExpectedIceberg.TypeName)
		require.True(t, resolved.Fallback)
	}

	array, err := ResolveStorageMapping(typesystem.ArrayOf(typesystem.LogicalType{Kind: typesystem.KindUInt64}))
	require.NoError(t, err)
	require.Equal(t, arrow.ListOf(arrow.BinaryTypes.String), array.ArrowType)
	require.Equal(t, "list<string>", array.ExpectedIceberg.TypeName)
	require.True(t, array.Fallback)
	compatible, err = ArrowTypeCompatibleWithIceberg(array.ArrowType, array.ExpectedIceberg)
	require.NoError(t, err)
	require.True(t, compatible)
}
