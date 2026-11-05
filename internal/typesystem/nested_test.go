package typesystem

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMapAndStructRoundTrip(t *testing.T) {
	for _, spelling := range []string{
		"map<string,int32>",
		"map<int32,nullable<decimal(38,0)>>",
		"struct<f1:nullable<int32>,f2:nullable<string>>",
		"nullable<map<string,array<nullable<int32>>>>",
		"struct<point:struct<x:float64,y:float64>,tags:map<string,string>>",
	} {
		parsed, err := ParseType(spelling)
		require.NoError(t, err, spelling)
		require.Equal(t, spelling, parsed.String())
		again, err := ParseType(parsed.String())
		require.NoError(t, err)
		require.True(t, again.Equal(parsed), spelling)
	}
	for _, invalid := range []string{"map<string>", "map<string,int32", "struct<>", "struct<a int32>", "struct<a:int32,a:string>"} {
		_, err := ParseType(invalid)
		require.Error(t, err, invalid)
	}
}

func TestConvertMap(t *testing.T) {
	target := MapOf(LogicalType{Kind: KindString}, Nullable(LogicalType{Kind: KindInt32}))
	got, err := Convert(MapValue{{Key: "b", Value: int64(2)}, {Key: "a", Value: nil}}, target)
	require.NoError(t, err)
	require.Equal(t, []MapEntry{{Key: "b", Value: int32(2)}, {Key: "a", Value: nil}}, got)

	// Plain Go maps are accepted and ordered by key.
	got, err = Convert(map[string]int{"z": 1, "a": 2}, target)
	require.NoError(t, err)
	require.Equal(t, []MapEntry{{Key: "a", Value: int32(2)}, {Key: "z", Value: int32(1)}}, got)

	_, err = Convert(MapValue{{Key: nil, Value: int32(1)}}, target)
	require.ErrorContains(t, err, "map key 0 is null")
	_, err = Convert(MapValue{{Key: "a", Value: nil}}, MapOf(LogicalType{Kind: KindString}, LogicalType{Kind: KindInt32}))
	require.ErrorContains(t, err, "not nullable")
	_, err = Convert(MapValue{{Key: "a", Value: int64(1 << 40)}}, target)
	require.Error(t, err, "a value out of the value type's range is an error, not truncated")
	_, err = Convert("not a map", target)
	require.ErrorContains(t, err, "map source must be a map")
}

func TestConvertStruct(t *testing.T) {
	target := LogicalType{Kind: KindStruct, Fields: []LogicalField{
		{Name: "f1", Type: Nullable(LogicalType{Kind: KindInt32})},
		{Name: "f2", Type: LogicalType{Kind: KindString}},
	}}
	got, err := Convert(TupleValue{int64(7), "x"}, target)
	require.NoError(t, err)
	require.Equal(t, []any{int32(7), "x"}, got)

	got, err = Convert(TupleValue{nil, "x"}, target)
	require.NoError(t, err)
	require.Equal(t, []any{nil, "x"}, got)

	got, err = Convert(MapValue{{Key: "f2", Value: "y"}, {Key: "f1", Value: int64(1)}}, target)
	require.NoError(t, err)
	require.Equal(t, []any{int32(1), "y"}, got, "struct fields are matched by name from a map")

	_, err = Convert(TupleValue{int64(1)}, target)
	require.ErrorContains(t, err, "struct has 2 fields but the value has 1")
	_, err = Convert(TupleValue{int64(1), nil}, target)
	require.ErrorContains(t, err, `struct field "f2" is null`)
}

func TestNestedDecimalAndVarintStayExact(t *testing.T) {
	target := MapOf(LogicalType{Kind: KindInt32}, Nullable(Decimal(38, 0)))
	got, err := Convert(MapValue{{Key: int64(1), Value: "123456789012345678901234567890"}}, target)
	require.NoError(t, err)
	entries := got.([]MapEntry)
	require.Equal(t, "123456789012345678901234567890", entries[0].Value.(DecimalValue).String())

	_, err = Convert(MapValue{{Key: int64(1), Value: "1234567890123456789012345678901234567890"}}, target)
	require.Error(t, err, "a varint wider than 38 digits must fail, not be rounded")
}

func TestDecimalDigits(t *testing.T) {
	for _, tc := range []struct {
		text          string
		integer, frac int
	}{
		{"123.45", 3, 2},
		{"-0.005", 0, 3},
		{"0", 0, 0},
		{"1000", 4, 0},
		{"0.10", 0, 2},
		{"99999999999999999999999999999999999999", 38, 0},
	} {
		i, s, ok := DecimalDigits(tc.text)
		require.True(t, ok, tc.text)
		require.Equal(t, tc.integer, i, tc.text)
		require.Equal(t, tc.frac, s, tc.text)
	}
	_, _, ok := DecimalDigits("1e5")
	require.False(t, ok)
}
