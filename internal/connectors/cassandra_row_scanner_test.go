package connectors

import (
	"database/sql"
	"encoding/binary"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	inf "gopkg.in/inf.v0"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func TestCassandraDestFactoryFallsBackToRawBytesForUnsupportedTypes(t *testing.T) {
	custom := gocql.NewNativeType(4, gocql.TypeCustom, "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType,3)")
	_, err := custom.NewWithError()
	require.Error(t, err, "precondition: gocql cannot build a Go value for custom types")

	dest := cassandraDestFactory(custom)()
	raw, ok := dest.(*cassandraRawValue)
	require.True(t, ok)
	require.NoError(t, gocql.Unmarshal(custom, []byte{1, 2, 3}, raw))
	require.Equal(t, []byte{1, 2, 3}, cassandraDereference(raw))
	require.NoError(t, gocql.Unmarshal(custom, nil, raw))
	require.Nil(t, cassandraDereference(raw))
}

func TestCassandraRowScannerExpandsTuplesAndKeepsNativeTypes(t *testing.T) {
	text := gocql.NewNativeType(4, gocql.TypeText, "")
	bigint := gocql.NewNativeType(4, gocql.TypeBigInt, "")
	custom := gocql.NewNativeType(4, gocql.TypeCustom, "x.Custom")
	tuple := gocql.TupleTypeInfo{NativeType: gocql.NewNativeType(4, gocql.TypeTuple, ""), Elems: []gocql.TypeInfo{text, bigint}}

	s := newCassandraRowScanner([]gocql.ColumnInfo{
		{Name: "id", TypeInfo: bigint},
		{Name: "pair", TypeInfo: tuple},
		{Name: "embedding", TypeInfo: custom},
	})
	dests := s.newDests()
	require.Len(t, dests, 4, "tuple elements occupy one scan slot each")
	require.IsType(t, new(*int64), dests[0], "scalars scan into **T so NULL stays nil")
	require.IsType(t, new(*string), dests[1])
	require.IsType(t, new(*int64), dests[2])
	require.IsType(t, &cassandraRawValue{}, dests[3])
}

// gocql writes NULL as nil only into a pointer-to-pointer; into *T it stores
// T's zero value. Every scalar CQL type must keep NULL as NULL and keep its
// value otherwise, also inside tuples.
func TestCassandraScanDestinationsKeepNulls(t *testing.T) {
	uuid := gocql.UUID{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}
	cases := []struct {
		typ   gocql.Type
		value any
		want  any
	}{
		{gocql.TypeInt, 7, int64(7)},
		{gocql.TypeBigInt, int64(-9), int64(-9)},
		{gocql.TypeText, "x", "x"},
		{gocql.TypeBoolean, false, false},
		{gocql.TypeDouble, 1.5, 1.5},
		{gocql.TypeUUID, uuid, "123e4567-e89b-12d3-a456-426614174000"},
		{gocql.TypeTimeUUID, uuid, "123e4567-e89b-12d3-a456-426614174000"},
		{gocql.TypeTimestamp, time.UnixMilli(1700000000000).UTC(), time.UnixMilli(1700000000000).UTC()},
		{gocql.TypeDecimal, inf.NewDec(12345, 2), "123.45"},
		{gocql.TypeVarint, big.NewInt(-42), "-42"},
	}
	for _, tc := range cases {
		info := gocql.NewNativeType(4, tc.typ, "")
		factory := cassandraDestFactory(info)

		null := factory()
		require.NoError(t, gocql.Unmarshal(info, nil, null), tc.typ.String())
		v, err := cassandraToDriverValue(cassandraDereference(null))
		require.NoError(t, err)
		require.Nil(t, v, "NULL %s must stay NULL", tc.typ)

		data, err := gocql.Marshal(info, tc.value)
		require.NoError(t, err, tc.typ.String())
		dest := factory()
		require.NoError(t, gocql.Unmarshal(info, data, dest), tc.typ.String())
		v, err = cassandraToDriverValue(cassandraDereference(dest))
		require.NoError(t, err)
		if ts, ok := v.(time.Time); ok {
			v = ts.UTC()
		}
		require.Equal(t, tc.want, v, tc.typ.String())
	}

	// A tuple element that is NULL stays NULL.
	tuple := cassandraTuple{cassandraDereference(func() any {
		d := cassandraDestFactory(gocql.NewNativeType(4, gocql.TypeInt, ""))()
		require.NoError(t, gocql.Unmarshal(gocql.NewNativeType(4, gocql.TypeInt, ""), nil, d))
		return d
	}()), "x"}
	v, err := cassandraToDriverValue(tuple)
	require.NoError(t, err)
	require.Equal(t, typesystem.TupleValue{nil, "x"}, v)
}

func TestCassandraToDriverValueHandlesDecimalVarintDurationAndNested(t *testing.T) {
	dec := inf.NewDec(12345, 2)
	v, err := cassandraToDriverValue(dec)
	require.NoError(t, err)
	require.Equal(t, "123.45", v)

	v, err = cassandraToDriverValue(big.NewInt(-42))
	require.NoError(t, err)
	require.Equal(t, "-42", v)

	v, err = cassandraToDriverValue(gocql.Duration{Months: 1, Days: 2, Nanoseconds: 3})
	require.NoError(t, err)
	require.Equal(t, "P1M2DT0.000000003S", v)

	v, err = cassandraToDriverValue(map[string]any{"amount": dec, "tags": []string{"a"}})
	require.NoError(t, err)
	require.Equal(t, `{"amount":"123.45","tags":["a"]}`, cassandraJSONText(t, v))

	var nilDec *inf.Dec
	v, err = cassandraToDriverValue(nilDec)
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestCassandraToDriverValueKeepsTimeAndEncodesTuplesOnce(t *testing.T) {
	v, err := cassandraToDriverValue(90 * time.Second)
	require.NoError(t, err)
	require.Equal(t, 90*time.Second, v, "CQL time must stay a time.Duration for the time converter")

	v, err = cassandraToDriverValue(cassandraTuple{1.5, map[string]any{"k": inf.NewDec(1, 0)}})
	require.NoError(t, err)
	require.Equal(t, typesystem.TupleValue{1.5, typesystem.MapValue{{Key: "k", Value: "1"}}}, v)
	require.Equal(t, `[1.5,{"k":"1"}]`, cassandraJSONText(t, v))

	v, err = cassandraToDriverValue([]map[string]int{{"a": 1}})
	require.NoError(t, err)
	require.Equal(t, []any{typesystem.MapValue{{Key: "a", Value: 1}}}, v)
	// A list<map> column of an existing table keeps one JSON string per element.
	text, err := typesystem.Convert(v, typesystem.ArrayOf(typesystem.LogicalType{Kind: typesystem.KindJSON}))
	require.NoError(t, err)
	require.Equal(t, []any{`{"a":1}`}, text)

	v, err = cassandraToDriverValue([]string{"x", "y"})
	require.NoError(t, err)
	require.Equal(t, []any{"x", "y"}, v)
}

func TestDecodeCassandraVector(t *testing.T) {
	custom := "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType,2)"
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[0:4], math.Float32bits(1.25))
	binary.BigEndian.PutUint32(data[4:8], math.Float32bits(-2))
	raw := &cassandraRawValue{custom: custom, data: data}
	require.Equal(t, []any{float32(1.25), float32(-2)}, raw.value())

	other := &cassandraRawValue{custom: "x.Custom", data: []byte{9}}
	require.Equal(t, []byte{9}, other.value())
}

func TestSelectCassandraColumns(t *testing.T) {
	cts := []*sql.ColumnType{nil, nil, nil}
	cols, gotTypes, list, err := selectCassandraColumns([]string{"id", "name", "age"}, cts, []string{"AGE", "id"})
	require.NoError(t, err)
	require.Equal(t, []string{"age", "id"}, cols)
	require.Len(t, gotTypes, 2)
	require.Equal(t, `"age", "id"`, list)

	_, _, _, err = selectCassandraColumns([]string{"id"}, cts[:1], []string{"missing"})
	require.Error(t, err)
}

func TestCassandraWherePredicate(t *testing.T) {
	got, err := cassandraWherePredicate(`(age > 30 AND name = 'or else')`)
	require.NoError(t, err)
	require.Equal(t, `age > 30 AND name = 'or else'`, got)

	got, err = cassandraWherePredicate("  ")
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = cassandraWherePredicate("age > 30 OR age < 5")
	require.Error(t, err)
	_, err = cassandraWherePredicate("age > 1; DROP TABLE x")
	require.Error(t, err)
}
