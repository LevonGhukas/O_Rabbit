package connectors

import (
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// Regression for TID-898: the master synthesizes a *sql.ColumnType for every
// Cassandra column on each validate/plan request. Concurrent ETL submissions
// (and plain GC address reuse between sequential ones) must never make the
// synthetic driver registration collide and panic the HTTP handler.
func TestCassandraColumnTypeDoesNotRegisterGlobalDrivers(t *testing.T) {
	before := len(sql.Drivers())
	for i := 0; i < 10; i++ {
		if _, err := cassandraColumnType("c", "text", true); err != nil {
			t.Fatal(err)
		}
	}
	if after := len(sql.Drivers()); after != before {
		t.Fatalf("registered drivers grew from %d to %d", before, after)
	}
}

func TestCassandraColumnTypeSurvivesRepeatedCallsAcrossGC(t *testing.T) {
	const calls = 2000
	for i := 0; i < calls; i++ {
		ct, err := cassandraColumnType(fmt.Sprintf("col_%d", i), "int", true)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if ct.Name() != fmt.Sprintf("col_%d", i) {
			t.Fatalf("call %d: column name = %q", i, ct.Name())
		}
		if i%50 == 0 {
			runtime.GC()
		}
	}
}

func TestCassandraColumnTypeConcurrentRequestsKeepTheirOwnColumns(t *testing.T) {
	const requests = 8
	const columnsPerRequest = 200
	var wg sync.WaitGroup
	errs := make(chan error, requests)
	for r := 0; r < requests; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					errs <- fmt.Errorf("request %d panicked: %v", r, rec)
				}
			}()
			for c := 0; c < columnsPerRequest; c++ {
				name := fmt.Sprintf("r%d_c%d", r, c)
				ct, err := cassandraColumnType(name, "bigint", c%2 == 0)
				if err != nil {
					errs <- fmt.Errorf("request %d column %d: %w", r, c, err)
					return
				}
				if ct.Name() != name || ct.DatabaseTypeName() != "BIGINT" {
					errs <- fmt.Errorf("request %d column %d: got %q/%q", r, c, ct.Name(), ct.DatabaseTypeName())
					return
				}
				if c%25 == 0 {
					runtime.GC()
				}
			}
		}(r)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
