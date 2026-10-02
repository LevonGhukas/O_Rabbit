package connectors

import (
	"math/big"
	"testing"

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
	require.Equal(t, "1mo2d3ns", v)

	v, err = cassandraToDriverValue(map[string]any{"amount": dec, "tags": []string{"a"}})
	require.NoError(t, err)
	require.Equal(t, `json:{"amount":"123.45","tags":["a"]}`, v)

	var nilDec *inf.Dec
	v, err = cassandraToDriverValue(nilDec)
	require.NoError(t, err)
	require.Nil(t, v)
}
