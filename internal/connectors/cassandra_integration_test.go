package connectors

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

// Reads one row containing every CQL type through newCassandraRows against a
// real Cassandra 5. Run with, for example:
//
//	ORABBIT_IT_CASSANDRA_HOSTS=localhost:9042 go test -run CassandraIntegration ./internal/connectors/
func TestCassandraIntegrationReadsAllCQLTypes(t *testing.T) {
	hosts := os.Getenv("ORABBIT_IT_CASSANDRA_HOSTS")
	if hosts == "" {
		t.Skip("set ORABBIT_IT_CASSANDRA_HOSTS to run against a real Cassandra")
	}
	cluster := gocql.NewCluster(strings.Split(hosts, ",")...)
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	for _, stmt := range []string{
		`CREATE KEYSPACE IF NOT EXISTS orabbit_it WITH replication = {'class':'SimpleStrategy','replication_factor':1}`,
		`CREATE TYPE IF NOT EXISTS orabbit_it.point (lat double, lon double)`,
		`DROP TABLE IF EXISTS orabbit_it.all_types`,
		`CREATE TABLE orabbit_it.all_types (
			id uuid PRIMARY KEY, t text, i int, b bigint, f float, d double, flag boolean,
			dec decimal, vi varint, ip inet, ts timestamp, dt date, tm time, dur duration,
			bl blob, tid timeuuid, tags list<text>, nums set<int>, attrs map<text,decimal>,
			pair tuple<text,int>, pos frozen<point>, embedding vector<float, 3>, empty_dec decimal)`,
		`INSERT INTO orabbit_it.all_types (id,t,i,b,f,d,flag,dec,vi,ip,ts,dt,tm,dur,bl,tid,tags,nums,attrs,pair,pos,embedding)
		 VALUES (6f1b6c1e-0b8e-4d1a-9a8c-1d2e3f4a5b6c,'hello',42,9000000000,1.5,2.25,true,
		 123.45,123456789012345678901234567890,'10.0.0.1','2026-01-02T03:04:05Z','2026-01-02','03:04:05',1mo2d,
		 0xcafe,now(),['a','b'],{1,2},{'k':0.5},('x',7),{lat:1.5,lon:2.5},[1.0,2.0,3.0])`,
	} {
		if err := session.Query(stmt).Exec(); err != nil {
			t.Fatalf("%s: %v", strings.Fields(stmt)[0], err)
		}
	}

	cols := []string{"id", "t", "dec", "vi", "ip", "attrs", "pair", "pos", "embedding", "empty_dec", "tags"}
	iter := session.Query(`SELECT * FROM orabbit_it.all_types`).Iter()
	rows, err := newCassandraRows(iter, cols)
	if err != nil {
		t.Fatalf("newCassandraRows: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("no row: %v", rows.Err())
	}
	got := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range got {
		ptrs[i] = &got[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"id":        "6f1b6c1e-0b8e-4d1a-9a8c-1d2e3f4a5b6c",
		"t":         "hello",
		"dec":       "123.45",
		"vi":        "123456789012345678901234567890",
		"ip":        "10.0.0.1",
		"attrs":     `json:{"k":"0.5"}`,
		"pair":      `json:["x",7]`,
		"pos":       `json:{"lat":1.5,"lon":2.5}`,
		"embedding": `json:[1,2,3]`,
		"empty_dec": nil,
		"tags":      `json:["a","b"]`,
	}
	for i, col := range cols {
		g := got[i]
		if b, ok := g.([]byte); ok {
			g = string(b)
		}
		if g != want[col] {
			t.Errorf("%s = %#v, want %#v", col, g, want[col])
		}
	}
}
