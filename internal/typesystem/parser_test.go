package typesystem

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTypeCanonicalGrammar(t *testing.T) {
	decimal := Decimal(18, 2)
	tests := []struct {
		input string
		want  LogicalType
	}{
		{"string", LogicalType{Kind: KindString}},
		{"BOOLEAN", LogicalType{Kind: KindBool}},
		{"int8", LogicalType{Kind: KindInt8}},
		{"int16", LogicalType{Kind: KindInt16}},
		{"int32", LogicalType{Kind: KindInt32}},
		{"int64", LogicalType{Kind: KindInt64}},
		{"uint8", LogicalType{Kind: KindUInt8}},
		{"uint16", LogicalType{Kind: KindUInt16}},
		{"uint32", LogicalType{Kind: KindUInt32}},
		{"uint64", LogicalType{Kind: KindUInt64}},
		{"float32", LogicalType{Kind: KindFloat32}},
		{"float64", LogicalType{Kind: KindFloat64}},
		{"decimal(18, 2)", decimal},
		{"date", LogicalType{Kind: KindDate}},
		{"time", LogicalType{Kind: KindTime}},
		{"timestamp", LogicalType{Kind: KindTimestamp}},
		{"timestamp_tz", LogicalType{Kind: KindTimestampTZ}},
		{"timestamp_tz[UTC]", LogicalType{Kind: KindTimestampTZ, Timezone: "UTC"}},
		{"timestamp_tz[Europe/Yerevan]", LogicalType{Kind: KindTimestampTZ, Timezone: "Europe/Yerevan"}},
		{"uuid", LogicalType{Kind: KindUUID}},
		{"binary", LogicalType{Kind: KindBinary}},
		{"json", LogicalType{Kind: KindJSON}},
		{"array < int64 >", ArrayOf(LogicalType{Kind: KindInt64})},
		{"array<nullable<string>>", ArrayOf(Nullable(LogicalType{Kind: KindString}))},
		{"nullable < array < string > >", Nullable(ArrayOf(LogicalType{Kind: KindString}))},
		{"array<array<uint32>>", ArrayOf(ArrayOf(LogicalType{Kind: KindUInt32}))},
		{"nullable<nullable<int64>>", Nullable(LogicalType{Kind: KindInt64})},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := ParseType(test.input)
			require.NoError(t, err)
			require.True(t, got.Equal(test.want), "got %s, want %s", got.String(), test.want.String())
		})
	}
}

func TestParseTypeLegacyAliasesNormalizeSemantics(t *testing.T) {
	decimal := Decimal(18, 2)
	tests := []struct {
		input string
		want  LogicalType
	}{
		{"UInt64", LogicalType{Kind: KindUInt64}},
		{"INT8", LogicalType{Kind: KindInt8}},
		{"Float32", LogicalType{Kind: KindFloat32}},
		{"Bool", LogicalType{Kind: KindBool}},
		{"Numeric(18,2)", decimal},
		{"Number(18,2)", decimal},
		{"Money", Decimal(19, 4)},
		{"SmallMoney", Decimal(10, 4)},
		{"Date32", LogicalType{Kind: KindDate}},
		{"Time64(6)", LogicalType{Kind: KindTime}},
		{"DateTime64(6)", LogicalType{Kind: KindTimestamp}},
		{"DateTime64(6, 'UTC')", LogicalType{Kind: KindTimestampTZ, Timezone: "UTC"}},
		{"DateTime('Europe/Yerevan')", LogicalType{Kind: KindTimestampTZ, Timezone: "Europe/Yerevan"}},
		{"Timestamp(6)", LogicalType{Kind: KindTimestamp}},
		{"Array(Nullable(UInt64))", ArrayOf(Nullable(LogicalType{Kind: KindUInt64}))},
		{"LowCardinality(String)", LogicalType{Kind: KindString}},
		{"Nullable(LowCardinality(String))", Nullable(LogicalType{Kind: KindString})},
		{"VARCHAR", LogicalType{Kind: KindString}},
		{"UniqueIdentifier", LogicalType{Kind: KindUUID}},
		{"XML", LogicalType{Kind: KindString}},
		{"BYTEA", LogicalType{Kind: KindBinary}},
		{"RowVersion", LogicalType{Kind: KindBinary}},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := ParseType(test.input)
			require.NoError(t, err)
			require.True(t, got.Equal(test.want), "got %s, want %s", got.String(), test.want.String())
		})
	}

	for _, input := range []string{"UInt64", "uint64", "UINT64"} {
		got, err := ParseType(input)
		require.NoError(t, err)
		require.True(t, got.Equal(LogicalType{Kind: KindUInt64}))
	}
}

func TestParseTypeRejectsInvalidExplicitTargets(t *testing.T) {
	for _, input := range []string{
		"decimal",
		"decimal()",
		"decimal(18)",
		"decimal(0,0)",
		"decimal(10,-1)",
		"decimal(5,6)",
		"decimal(foo,2)",
		"array<>",
		"array<int64",
		"nullable<>",
		"Enum8('a'=1)",
		"FooBar",
		"timestamp_tz[]",
		"DateTime64(foo)",
	} {
		t.Run(input, func(t *testing.T) {
			_, err := ParseType(input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid type")
		})
	}
}

func TestParseTypeDoesNotClampDecimalPrecision(t *testing.T) {
	got, err := ParseType("decimal(50,10)")
	require.NoError(t, err)
	require.True(t, got.Equal(Decimal(50, 10)))
}
