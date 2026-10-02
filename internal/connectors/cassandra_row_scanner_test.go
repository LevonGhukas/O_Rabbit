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
	require.IsType(t, new(int64), dests[0])
	require.IsType(t, new(string), dests[1])
	require.IsType(t, new(int64), dests[2])
	require.IsType(t, &cassandraRawValue{}, dests[3])
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
	require.Equal(t, `{"amount":"123.45","tags":["a"]}`, v)

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
	require.Equal(t, `[1.5,{"k":"1"}]`, v)

	v, err = cassandraToDriverValue([]map[string]int{{"a": 1}})
	require.NoError(t, err)
	require.Equal(t, []any{`{"a":1}`}, v, "list<map> keeps one JSON string per element")

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
