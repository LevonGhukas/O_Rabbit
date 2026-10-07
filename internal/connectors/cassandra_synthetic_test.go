package connectors

import (
	"context"
	"database/sql/driver"
	"runtime"
	"sync"
	"testing"
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
