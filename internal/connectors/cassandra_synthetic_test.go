package connectors

import (
	"context"
	"database/sql/driver"
	"math/big"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

// Regression: cassandraColumnType registered a global sql driver per call,
// named after a heap address. A reused address after GC produced a duplicate
// name and sql.Register panicked inside the master's run planning.
func TestCassandraColumnTypeIsSafeToCallRepeatedly(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				ct, err := cassandraColumnType("id", "int", false)
				if err != nil {
					t.Error(err)
					return
				}
				if ct.Name() != "id" || ct.DatabaseTypeName() != "INT" {
					t.Errorf("column type = %s/%s", ct.Name(), ct.DatabaseTypeName())
					return
				}
				if i%200 == 0 {
					runtime.GC()
				}
			}
		}()
	}
	wg.Wait()
}

func TestSyntheticRowsReadableAfterDBClosed(t *testing.T) {
	db := openSyntheticDB(&cassandraRowsDriver{cols: []string{"id"}, data: [][]driver.Value{{int64(1)}, {int64(2)}}})
	rows, err := db.QueryContext(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil || len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("rows=%v err=%v", got, err)
	}
}

// Regression: decimal/varint/inet values (and collections containing them,
// UUIDs or timestamps) failed conversion, failing every task of the table.
func TestCassandraToDriverValueConvertsAllCQLTypesExactly(t *testing.T) {
	u, _ := gocql.ParseUUID("6f1b6c1e-0b8e-4d1a-9a8c-1d2e3f4a5b6c")
	huge, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
	cases := []struct {
		name string
		in   any
		want driver.Value
	}{
		{"decimal", inf.NewDec(12345, 2), "123.45"},
		{"decimal high precision", inf.NewDecBig(huge, 10), "12345678901234567890.1234567890"},
		{"varint beyond int64", huge, "123456789012345678901234567890"},
		{"inet", net.ParseIP("10.0.0.1"), "10.0.0.1"},
		{"list<decimal>", []*inf.Dec{inf.NewDec(1, 0), inf.NewDec(25, 1)}, `json:["1","2.5"]`},
		{"map<text,decimal>", map[string]*inf.Dec{"k": inf.NewDec(5, 1)}, `json:{"k":"0.5"}`},
		{"map<uuid,varint>", map[gocql.UUID]*big.Int{u: big.NewInt(7)}, `json:{"6f1b6c1e-0b8e-4d1a-9a8c-1d2e3f4a5b6c":"7"}`},
		{"list<uuid>", []gocql.UUID{u}, `json:["6f1b6c1e-0b8e-4d1a-9a8c-1d2e3f4a5b6c"]`},
		{"list<timestamp>", []time.Time{time.Unix(0, 0).UTC()}, `json:["1970-01-01T00:00:00Z"]`},
		{"list<text>", []string{"a", "b"}, `json:["a","b"]`},
		{"udt", map[string]interface{}{"lat": 1.5}, `json:{"lat":1.5}`},
		{"null decimal", (*inf.Dec)(nil), nil},
	}
	for _, tc := range cases {
		got, err := cassandraToDriverValue(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("%s: got %#v err=%v, want %#v", tc.name, got, err, tc.want)
		}
	}
}

func TestCassandraRawValueDecodesVectorsAndKeepsOtherCustomTypesLossless(t *testing.T) {
	floats := []byte{0x3f, 0x80, 0, 0, 0x40, 0, 0, 0, 0x40, 0x40, 0, 0} // 1, 2, 3 as float32
	cases := []struct {
		name string
		raw  cassandraRawValue
		want driver.Value
	}{
		{"vector<float,3>", cassandraRawValue{custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 3)", data: floats}, `json:[1,2,3]`},
		{"vector<int,2>", cassandraRawValue{custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.Int32Type, 2)", data: []byte{0, 0, 0, 7, 0xff, 0xff, 0xff, 0xff}}, `json:[7,-1]`},
		{"wrong length stays lossless", cassandraRawValue{custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 3)", data: []byte{1, 2}}, "base64:AQI="},
		{"unknown custom type", cassandraRawValue{custom: "com.example.MyType", data: []byte("hi")}, "base64:aGk="},
		{"null", cassandraRawValue{custom: "com.example.MyType", null: true}, nil},
	}
	for _, tc := range cases {
		got, err := cassandraToDriverValue(tc.raw)
		if err != nil || got != tc.want {
			t.Errorf("%s: got %#v err=%v, want %#v", tc.name, got, err, tc.want)
		}
	}
}

func TestCassandraTimeOfDayBecomesClockText(t *testing.T) {
	// 08:30:00.123456789, the value gocql returns for a CQL time column.
	got, err := cassandraToDriverValue(time.Duration(30600123456789))
	if err != nil {
		t.Fatal(err)
	}
	if got != "08:30:00.123456789" {
		t.Fatalf("time = %#v", got)
	}
	got, err = cassandraToDriverValue([]time.Duration{0})
	if err != nil {
		t.Fatal(err)
	}
	if got != `json:["00:00:00.000000000"]` {
		t.Fatalf("list<time> = %#v", got)
	}
}
