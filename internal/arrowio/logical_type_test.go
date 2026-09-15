package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func TestArrowTypeForLogicalTypeScalars(t *testing.T) {
	decimal18 := typesystem.Decimal(18, 2)
	decimal38 := typesystem.Decimal(38, 10)
	tests := []struct {
		name     string
		logical  typesystem.LogicalType
		wantType arrow.DataType
		class    typesystem.MappingClass
		fallback bool
	}{
		{"bool", typesystem.LogicalType{Kind: typesystem.KindBool}, arrow.FixedWidthTypes.Boolean, typesystem.MappingExact, false},
		{"int8", typesystem.LogicalType{Kind: typesystem.KindInt8}, arrow.PrimitiveTypes.Int8, typesystem.MappingExact, false},
		{"int16", typesystem.LogicalType{Kind: typesystem.KindInt16}, arrow.PrimitiveTypes.Int16, typesystem.MappingExact, false},
		{"int32", typesystem.LogicalType{Kind: typesystem.KindInt32}, arrow.PrimitiveTypes.Int32, typesystem.MappingExact, false},
		{"int64", typesystem.LogicalType{Kind: typesystem.KindInt64}, arrow.PrimitiveTypes.Int64, typesystem.MappingExact, false},
		{"uint8", typesystem.LogicalType{Kind: typesystem.KindUInt8}, arrow.PrimitiveTypes.Uint8, typesystem.MappingExact, false},
		{"uint16", typesystem.LogicalType{Kind: typesystem.KindUInt16}, arrow.PrimitiveTypes.Uint16, typesystem.MappingExact, false},
		{"uint32", typesystem.LogicalType{Kind: typesystem.KindUInt32}, arrow.PrimitiveTypes.Uint32, typesystem.MappingExact, false},
		{"uint64", typesystem.LogicalType{Kind: typesystem.KindUInt64}, arrow.PrimitiveTypes.Uint64, typesystem.MappingExact, false},
		{"float32", typesystem.LogicalType{Kind: typesystem.KindFloat32}, arrow.PrimitiveTypes.Float32, typesystem.MappingExact, false},
		{"float64", typesystem.LogicalType{Kind: typesystem.KindFloat64}, arrow.PrimitiveTypes.Float64, typesystem.MappingExact, false},
		{"string", typesystem.LogicalType{Kind: typesystem.KindString}, arrow.BinaryTypes.String, typesystem.MappingExact, false},
		{"binary", typesystem.LogicalType{Kind: typesystem.KindBinary}, arrow.BinaryTypes.Binary, typesystem.MappingExact, false},
		{"decimal 18", decimal18, &arrow.Decimal128Type{Precision: 18, Scale: 2}, typesystem.MappingExact, false},
		{"decimal 38", decimal38, &arrow.Decimal128Type{Precision: 38, Scale: 10}, typesystem.MappingExact, false},
		{"date", typesystem.LogicalType{Kind: typesystem.KindDate}, arrow.FixedWidthTypes.Date32, typesystem.MappingExact, false},
		{"time", typesystem.LogicalType{Kind: typesystem.KindTime}, arrow.FixedWidthTypes.Time64us, typesystem.MappingExact, false},
		{"timestamp", typesystem.LogicalType{Kind: typesystem.KindTimestamp}, &arrow.TimestampType{Unit: arrow.Microsecond}, typesystem.MappingExact, false},
		{"timestamp timezone defaults UTC", typesystem.LogicalType{Kind: typesystem.KindTimestampTZ}, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, typesystem.MappingExact, false},
		{"timestamp timezone preserved", typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "America/New_York"}, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/New_York"}, typesystem.MappingExact, false},
		{"uuid", typesystem.LogicalType{Kind: typesystem.KindUUID}, arrow.BinaryTypes.String, typesystem.MappingSemanticFallback, true},
		{"json", typesystem.LogicalType{Kind: typesystem.KindJSON}, arrow.BinaryTypes.String, typesystem.MappingSemanticFallback, true},
		{"struct", typesystem.LogicalType{Kind: typesystem.KindStruct}, arrow.BinaryTypes.String, typesystem.MappingSemanticFallback, true},
		{"map", typesystem.MapOf(typesystem.LogicalType{Kind: typesystem.KindString}, typesystem.LogicalType{Kind: typesystem.KindInt64}), arrow.BinaryTypes.String, typesystem.MappingSemanticFallback, true},
		{"unknown", typesystem.LogicalType{Kind: typesystem.KindUnknown}, arrow.BinaryTypes.String, typesystem.MappingUnsupportedFallback, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataType, mapping, err := ArrowTypeForLogicalType(test.logical)
			require.NoError(t, err)
			require.Equal(t, test.wantType, dataType)
			require.Equal(t, test.class, mapping.Class)
			require.Equal(t, test.fallback, mapping.Fallback)
		})
	}
}

func TestArrowTypeForLogicalTypeDecimalArrayAndInvalid(t *testing.T) {
	tooWide := typesystem.Decimal(39, 10)
	dataType, mapping, err := ArrowTypeForLogicalType(tooWide)
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.String, dataType)
	require.Equal(t, typesystem.MappingSemanticFallback, mapping.Class)
	require.Contains(t, mapping.Reason, "precision <= 38")

	for _, test := range []struct {
		name     string
		logical  typesystem.LogicalType
		wantType arrow.DataType
		class    typesystem.MappingClass
	}{
		{"int64", typesystem.ArrayOf(typesystem.LogicalType{Kind: typesystem.KindInt64}), arrow.ListOf(arrow.PrimitiveTypes.Int64), typesystem.MappingExact},
		{"nullable string", typesystem.ArrayOf(typesystem.Nullable(typesystem.LogicalType{Kind: typesystem.KindString})), arrow.ListOf(arrow.BinaryTypes.String), typesystem.MappingExact},
		{"json", typesystem.ArrayOf(typesystem.LogicalType{Kind: typesystem.KindJSON}), arrow.ListOf(arrow.BinaryTypes.String), typesystem.MappingSemanticFallback},
		{"nested", typesystem.ArrayOf(typesystem.ArrayOf(typesystem.LogicalType{Kind: typesystem.KindInt64})), arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)), typesystem.MappingExact},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataType, mapping, err := ArrowTypeForLogicalType(test.logical)
			require.NoError(t, err)
			require.Equal(t, test.wantType, dataType)
			require.Equal(t, test.class, mapping.Class)
		})
	}

	for _, invalid := range []typesystem.LogicalType{
		{Kind: typesystem.KindDecimal},
		{Kind: typesystem.KindArray},
		{Kind: typesystem.KindMap},
	} {
		_, _, err := ArrowTypeForLogicalType(invalid)
		require.Error(t, err)
	}
}
