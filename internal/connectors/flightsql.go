package connectors

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	adbcflightsql "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FlightSQL wraps an ADBC Flight SQL database + connection pair.
type FlightSQL struct {
	db   adbc.Database
	conn adbc.Connection
}

// OpenFlightSQL opens a Flight SQL ADBC connection from a DSN.
//
// DSN accepted formats:
//   - URI only: "grpc+tcp://host:32010"
//   - key/value: "uri=grpc+tcp://host:32010;username=u;password=p;authorization_header=Bearer <token>"
func OpenFlightSQL(ctx context.Context, dsn string) (*FlightSQL, error) {
	opts, err := parseFlightSQLDSN(dsn)
	if err != nil {
		return nil, err
	}

	drv := adbcflightsql.NewDriver(memory.NewGoAllocator())
	db, err := drv.NewDatabase(opts)
	if err != nil {
		return nil, err
	}

	conn, err := db.Open(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &FlightSQL{db: db, conn: conn}, nil
}

// Close releases the underlying connection resources.
func (f *FlightSQL) Close() error {
	if f == nil {
		return nil
	}
	var first error
	if f.conn != nil {
		if err := f.conn.Close(); err != nil {
			first = err
		}
		f.conn = nil
	}
	if f.db != nil {
		if err := f.db.Close(); err != nil && first == nil {
			first = err
		}
		f.db = nil
	}
	return first
}

// StreamQuery executes SQL and streams Arrow records to onRecord.
func (f *FlightSQL) StreamQuery(ctx context.Context, query string, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, error) {
	if f == nil || f.conn == nil {
		return 0, fmt.Errorf("flightsql connection is nil")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return 0, fmt.Errorf("empty source_sql")
	}
	if onRecord == nil {
		return 0, fmt.Errorf("onRecord callback is nil")
	}

	stmt, err := f.conn.NewStatement()
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return 0, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return 0, err
	}
	defer reader.Release()

	schema := reader.Schema()
	var rowsTotal int64
	for reader.Next() {
		rec := reader.RecordBatch()
		if rec == nil {
			continue
		}
		rowsTotal += rec.NumRows()
		if err := onRecord(schema, rec); err != nil {
			return rowsTotal, err
		}
	}
	if err := reader.Err(); err != nil {
		return rowsTotal, err
	}
	return rowsTotal, nil
}
