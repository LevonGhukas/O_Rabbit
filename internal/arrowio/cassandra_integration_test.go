package arrowio

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gocql/gocql"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

// Runs the worker's read path (Cassandra connector -> Arrow record batches)
// over a table holding every CQL type, plus a row of NULLs, against a real
// Cassandra 5. Every column must convert; a connector change that hands the
// typesystem a value it cannot parse fails here. Run with, for example:
//
//	ORABBIT_IT_CASSANDRA_HOSTS=localhost:9042 go test -run CassandraIntegration ./internal/arrowio/
func TestCassandraIntegrationConvertsAllCQLTypesToArrow(t *testing.T) {
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
		`DROP TABLE IF EXISTS orabbit_it.arrow_types`,
		`CREATE TABLE orabbit_it.arrow_types (
			id uuid PRIMARY KEY, t text, a ascii, ti tinyint, si smallint, i int, b bigint,
			f float, d double, flag boolean, dec decimal, vi varint, ip inet,
			ts timestamp, dt date, tm time, dur duration, bl blob, tid timeuuid,
			tags list<text>, nums set<int>, attrs map<text,decimal>, times list<time>,
			pair tuple<text,int>, pos frozen<point>, embedding vector<float, 3>)`,
		`INSERT INTO orabbit_it.arrow_types (id,t,a,ti,si,i,b,f,d,flag,dec,vi,ip,ts,dt,tm,dur,bl,tid,tags,nums,attrs,times,pair,pos,embedding)
		 VALUES (6f1b6c1e-0b8e-4d1a-9a8c-1d2e3f4a5b6c,'hello','abc',1,2,42,9000000000,1.5,2.25,true,
		 123.45,123456789012345678901234567890,'10.0.0.1','2026-01-02T03:04:05Z','2026-01-02',
		 '08:30:00.123456789',1mo2d,0xcafe,now(),['a','b'],{1,2},{'k':0.5},['23:59:59.999999999'],
		 ('x',7),{lat:1.5,lon:2.5},[1.0,2.0,3.0])`,
		`INSERT INTO orabbit_it.arrow_types (id) VALUES (00000000-0000-0000-0000-000000000001)`,
	} {
		if err := session.Query(stmt).Exec(); err != nil {
			t.Fatalf("%s: %v", strings.Fields(stmt)[0], err)
		}
	}

	ctx := context.Background()
	src, err := connectors.OpenCassandra(ctx, "cassandra://"+strings.Split(hosts, ",")[0]+"/orabbit_it")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	rows, cols, colTypes, cursorIdx, err := src.QueryCursor(ctx, connectors.CursorQuery{
		Table:        "arrow_types",
		CursorColumn: "id",
		CursorDomain: connectors.CursorDomainInt64,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var tm []string
	nonNull := map[string]int{}
	total, _, err := RowsToRecordBatchesEngineWithOverrides("cassandra", rows, cols, colTypes, nil, 100, memory.NewGoAllocator(), cursorIdx, connectors.CursorDomainInt64, func(schema *arrow.Schema, rec arrow.RecordBatch) error {
		for c, field := range schema.Fields() {
			col := rec.Column(c)
			nonNull[field.Name] += col.Len() - col.NullN()
			if field.Name != "tm" {
				continue
			}
			for r := 0; r < col.Len(); r++ {
				if col.IsNull(r) {
					tm = append(tm, "NULL")
					continue
				}
				// Iceberg time has microsecond precision.
				tm = append(tm, col.(*array.Time64).Value(r).ToTime(arrow.Microsecond).Format("15:04:05.000000"))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("convert to arrow: %v", err)
	}
	if total != 2 {
		t.Fatalf("rows = %d, want 2", total)
	}
	got := strings.Join(tm, ",")
	if got != "08:30:00.123456,NULL" && got != "NULL,08:30:00.123456" {
		t.Fatalf("tm = %s", got)
	}
	// The second row sets only id: every other column must stay NULL rather
	// than become a zero value (0, false, 00:00:00, empty string).
	for _, col := range cols {
		want := 1
		if col == "id" {
			want = 2
		}
		if nonNull[col] != want {
			t.Errorf("column %s: %d non-NULL values, want %d", col, nonNull[col], want)
		}
	}
}
