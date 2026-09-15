package typesystem

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKindString(t *testing.T) {
	tests := []struct {
		kind Kind
		want string
	}{
		{KindUnknown, "unknown"},
		{KindString, "string"},
		{KindBool, "bool"},
		{KindInt8, "int8"},
		{KindInt16, "int16"},
		{KindInt32, "int32"},
		{KindInt64, "int64"},
		{KindUInt8, "uint8"},
		{KindUInt16, "uint16"},
		{KindUInt32, "uint32"},
		{KindUInt64, "uint64"},
		{KindFloat32, "float32"},
		{KindFloat64, "float64"},
		{KindDecimal, "decimal"},
		{KindDate, "date"},
		{KindTime, "time"},
		{KindTimestamp, "timestamp"},
		{KindTimestampTZ, "timestamp_tz"},
		{KindUUID, "uuid"},
		{KindBinary, "binary"},
		{KindJSON, "json"},
		{KindArray, "array"},
		{KindStruct, "struct"},
		{KindMap, "map"},
		{Kind(255), "unknown"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.kind.String())
	}
}

func TestLogicalTypePrimitiveEqualityAndSourceName(t *testing.T) {
	int64Type := LogicalType{Kind: KindInt64}
	require.True(t, int64Type.Equal(LogicalType{Kind: KindInt64}))
	require.False(t, int64Type.Equal(LogicalType{Kind: KindUInt64}))

	uuid := LogicalType{Kind: KindUUID, SourceTypeName: "UUID"}
	uniqueIdentifier := LogicalType{Kind: KindUUID, SourceTypeName: "UNIQUEIDENTIFIER"}
	require.True(t, uuid.Equal(uniqueIdentifier))
	require.NoError(t, uuid.Validate())
}

func TestLogicalTypeDecimal(t *testing.T) {
	decimal18 := Decimal(18, 2)
	decimal38 := Decimal(38, 10)
	require.Equal(t, "decimal(18,2)", decimal18.String())
	require.Equal(t, "decimal(38,10)", decimal38.String())
	require.NoError(t, decimal18.Validate())
	require.NoError(t, decimal38.Validate())
	require.False(t, decimal18.Equal(decimal38))

	for _, tt := range []LogicalType{
		Decimal(0, 0),
		Decimal(10, -1),
		Decimal(10, 11),
		{Kind: KindDecimal},
	} {
		require.Error(t, tt.Validate())
	}
}

func TestLogicalTypeNullableAndArray(t *testing.T) {
	nullableInt := Nullable(LogicalType{Kind: KindInt64})
	require.Equal(t, "nullable<int64>", nullableInt.String())
	require.False(t, nullableInt.Equal(LogicalType{Kind: KindInt64}))

	array := ArrayOf(nullableInt)
	require.Equal(t, "array<nullable<int64>>", array.String())
	require.NoError(t, array.Validate())
	require.Error(t, (LogicalType{Kind: KindArray}).Validate())
}

func TestLogicalTypeMap(t *testing.T) {
	logicalMap := MapOf(LogicalType{Kind: KindString}, LogicalType{Kind: KindInt64})
	require.Equal(t, "map<string,int64>", logicalMap.String())
	require.True(t, logicalMap.Equal(MapOf(LogicalType{Kind: KindString}, LogicalType{Kind: KindInt64})))
	require.NoError(t, logicalMap.Validate())
	require.Error(t, (LogicalType{Kind: KindMap}).Validate())
	require.Error(t, (LogicalType{Kind: KindMap, Key: &LogicalType{Kind: KindString}}).Validate())
}

func TestLogicalTypeStruct(t *testing.T) {
	structType := LogicalType{
		Kind: KindStruct,
		Fields: []LogicalField{
			{Name: "id", Type: LogicalType{Kind: KindInt64}},
			{Name: "name", Type: LogicalType{Kind: KindString}},
		},
	}
	require.Equal(t, "struct<id:int64,name:string>", structType.String())
	require.NoError(t, structType.Validate())
	require.True(t, structType.Equal(structType))
	require.False(t, structType.Equal(LogicalType{
		Kind: KindStruct,
		Fields: []LogicalField{
			{Name: "name", Type: LogicalType{Kind: KindString}},
			{Name: "id", Type: LogicalType{Kind: KindInt64}},
		},
	}))

	require.NoError(t, (LogicalType{Kind: KindStruct}).Validate())
	require.Error(t, (LogicalType{Kind: KindStruct, Fields: []LogicalField{{Name: ""}}}).Validate())
	require.Error(t, (LogicalType{Kind: KindStruct, Fields: []LogicalField{
		{Name: "id", Type: LogicalType{Kind: KindInt64}},
		{Name: "id", Type: LogicalType{Kind: KindString}},
	}}).Validate())
	require.Error(t, (LogicalType{Kind: KindStruct, Fields: []LogicalField{
		{Name: "bad", Type: LogicalType{Kind: KindArray}},
	}}).Validate())
}

func TestLogicalTypeTimestampTZAndNestedTypes(t *testing.T) {
	require.Equal(t, "timestamp_tz", (LogicalType{Kind: KindTimestampTZ}).String())
	require.Equal(t, "timestamp_tz[UTC]", (LogicalType{Kind: KindTimestampTZ, Timezone: "UTC"}).String())

	nested := ArrayOf(LogicalType{
		Kind: KindStruct,
		Fields: []LogicalField{
			{Name: "id", Type: LogicalType{Kind: KindInt64}},
			{Name: "tags", Type: ArrayOf(LogicalType{Kind: KindString})},
		},
	})
	require.Equal(t, "array<struct<id:int64,tags:array<string>>>", nested.String())
	require.True(t, nested.Equal(nested))
	require.NoError(t, nested.Validate())
}
