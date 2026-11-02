package connectors

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// captureDriver records every statement and returns one single-column row.
type captureDriver struct {
	mu      sync.Mutex
	queries []capturedQuery
}

type capturedQuery struct {
	query string
	args  []driver.NamedValue
}

func (d *captureDriver) Open(string) (driver.Conn, error) { return &captureConn{d: d}, nil }

type captureConn struct{ d *captureDriver }

func (c *captureConn) Prepare(string) (driver.Stmt, error) { return nil, io.ErrUnexpectedEOF }
func (c *captureConn) Close() error                        { return nil }
func (c *captureConn) Begin() (driver.Tx, error)           { return nil, io.ErrUnexpectedEOF }

func (c *captureConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.d.mu.Lock()
	defer c.d.mu.Unlock()
	c.d.queries = append(c.d.queries, capturedQuery{query: query, args: args})
	return &captureRows{}, nil
}

type captureRows struct{ done bool }

func (r *captureRows) Columns() []string { return []string{"id"} }
func (r *captureRows) Close() error      { return nil }
func (r *captureRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = int64(7)
	return nil
}

var captureSeq int

func newCaptureDB(t *testing.T) (*sql.DB, *captureDriver) {
	t.Helper()
	captureSeq++
	name := "capture-" + strings.Repeat("x", captureSeq)
	d := &captureDriver{}
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, d
}

type parityReader interface {
	QueryCursor(context.Context, CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error)
	RangeCounter
}

func whereOf(query string) string {
	i := strings.Index(query, " WHERE ")
	if i < 0 {
		return ""
	}
	rest := query[i+len(" WHERE "):]
	if j := strings.Index(rest, " ORDER BY "); j >= 0 {
		rest = rest[:j]
	}
	return strings.TrimSuffix(rest, ";")
}

func argValues(args []driver.NamedValue) []any {
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

func TestCountRangeSharesPredicateWithQueryCursor(t *testing.T) {
	readers := map[string]func(db *sql.DB) parityReader{
		"postgres":   func(db *sql.DB) parityReader { return &Postgres{db: db} },
		"mysql":      func(db *sql.DB) parityReader { return &MySQL{db: db} },
		"mariadb":    func(db *sql.DB) parityReader { return &MariaDB{MySQL: MySQL{db: db}} },
		"mssql":      func(db *sql.DB) parityReader { return &MSSQL{db: db} },
		"oracle":     func(db *sql.DB) parityReader { return &Oracle{db: db} },
		"clickhouse": func(db *sql.DB) parityReader { return &ClickHouse{db: db} },
		"trino":      func(db *sql.DB) parityReader { return &Trino{db: db} },
	}
	base := CursorQuery{Table: "orders", CursorColumn: "id", CursorDomain: CursorDomainInt64}
	cases := map[string]func(q CursorQuery) CursorQuery{
		"exclusive lower inclusive upper": func(q CursorQuery) CursorQuery {
			q.LowerBound, q.LowerExclusive, q.UpperBound, q.UpperInclusive = "10", true, "20", true
			return q
		},
		"inclusive lower exclusive upper": func(q CursorQuery) CursorQuery {
			q.LowerBound, q.UpperBound = "10", "20"
			return q
		},
		"lower only": func(q CursorQuery) CursorQuery {
			q.LowerBound, q.LowerExclusive = "10", true
			return q
		},
		"upper only": func(q CursorQuery) CursorQuery {
			q.UpperBound, q.UpperInclusive = "20", true
			return q
		},
		"where clause with bounds": func(q CursorQuery) CursorQuery {
			q.WhereClause = "status = 'open'"
			q.LowerBound, q.LowerExclusive, q.UpperBound, q.UpperInclusive = "10", true, "20", true
			return q
		},
		"where clause only": func(q CursorQuery) CursorQuery {
			q.WhereClause = "status = 'open'"
			return q
		},
		"query mode": func(q CursorQuery) CursorQuery {
			q.Table, q.SourceQuery = "", "SELECT id FROM orders"
			q.LowerBound, q.LowerExclusive, q.UpperBound, q.UpperInclusive = "10", true, "20", true
			return q
		},
		"query mode with where clause": func(q CursorQuery) CursorQuery {
			q.Table, q.SourceQuery, q.WhereClause = "", "SELECT id, status FROM orders", "status = 'open'"
			q.LowerBound, q.LowerExclusive, q.UpperBound, q.UpperInclusive = "10", true, "20", true
			return q
		},
	}
	for engine, newReader := range readers {
		for name, mutate := range cases {
			t.Run(engine+"/"+name, func(t *testing.T) {
				db, cap := newCaptureDB(t)
				r := newReader(db)
				q := mutate(base)
				rows, _, _, _, err := r.QueryCursor(context.Background(), q)
				if err != nil {
					t.Fatalf("QueryCursor: %v", err)
				}
				rows.Close()
				n, err := r.CountRange(context.Background(), q)
				if err != nil {
					t.Fatalf("CountRange: %v", err)
				}
				if n != 7 || len(cap.queries) != 2 {
					t.Fatalf("n=%d queries=%d", n, len(cap.queries))
				}
				sel, cnt := cap.queries[0], cap.queries[1]
				wantExpr := map[string]string{"mssql": "COUNT_BIG(*)", "clickhouse": "toInt64(count())"}[engine]
				if wantExpr == "" {
					wantExpr = "COUNT(*)"
				}
				if !strings.HasPrefix(cnt.query, "SELECT "+wantExpr+" FROM ") || strings.Contains(cnt.query, "ORDER BY") {
					t.Fatalf("count query shape: %s", cnt.query)
				}
				if whereOf(sel.query) != whereOf(cnt.query) {
					t.Fatalf("where differs:\nselect: %s\ncount:  %s", sel.query, cnt.query)
				}
				if !reflect.DeepEqual(argValues(sel.args), argValues(cnt.args)) {
					t.Fatalf("args differ: %v vs %v", sel.args, cnt.args)
				}
			})
		}
	}
}

func TestQueryModeAppliesWhereClause(t *testing.T) {
	q := CursorQuery{SourceQuery: "SELECT id, x FROM orders", CursorColumn: "id", CursorDomain: CursorDomainInt64, WhereClause: "x = 1", LowerBound: "1"}
	want := `WHERE (x = 1) AND "` + queryModeAlias + `"."id" >= $1`
	for name, build := range map[string]func(string, CursorQuery) (string, []any, error){
		"cursor": buildQueryModeCursorSQL,
		"count":  buildQueryModeCountSQL,
	} {
		sqlText, args, err := build("postgres", q)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !strings.Contains(sqlText, want) || len(args) != 1 {
			t.Fatalf("%s: sql=%s args=%v, want containing %q with one arg", name, sqlText, args, want)
		}
	}
}

func TestSupportsQueryModeWhereClause(t *testing.T) {
	for _, engine := range []string{"postgres", "mysql", "mariadb", "mssql", "oracle", "clickhouse", "trino"} {
		if !SupportsQueryModeWhereClause(engine) {
			t.Errorf("%s should apply where_clause in query mode", engine)
		}
	}
	for _, engine := range []string{"mongodb", "cassandra", "s3", "flightsql", "unknown"} {
		if SupportsQueryModeWhereClause(engine) {
			t.Errorf("%s cannot apply where_clause in query mode", engine)
		}
	}
}

func TestOpenCursorReaderWrapperForwardsCountRange(t *testing.T) {
	db, _ := newCaptureDB(t)
	wrapped := &errorWrappedSourceQueryReader{
		errorWrappedTableReader: errorWrappedTableReader{inner: &Postgres{db: db}},
	}
	var reader TableReader = wrapped
	counter, ok := reader.(RangeCounter)
	if !ok {
		t.Fatal("wrapped reader does not expose RangeCounter")
	}
	n, err := counter.CountRange(context.Background(), CursorQuery{Table: "orders", CursorColumn: "id", CursorDomain: CursorDomainInt64})
	if err != nil || n != 7 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestSupportsRangeCount(t *testing.T) {
	for _, engine := range []string{"postgres", "mysql", "mariadb", "mssql", "oracle", "clickhouse", "trino"} {
		if !SupportsRangeCount(engine) {
			t.Errorf("%s should support range count", engine)
		}
	}
	for _, engine := range []string{"mongodb", "cassandra", "s3", "flightsql", "unknown"} {
		if SupportsRangeCount(engine) {
			t.Errorf("%s should not support range count", engine)
		}
	}
}

// TestRangePredicateGolden pins the exact WHERE text of the extraction query,
// written from the per-driver code that predates the shared helper, so a
// regression in the helper cannot hide behind SELECT and COUNT agreeing.
func TestRangePredicateGolden(t *testing.T) {
	q := CursorQuery{Table: "orders", CursorColumn: "id", CursorDomain: CursorDomainInt64, WhereClause: "status = 'open'",
		LowerBound: "10", LowerExclusive: true, UpperBound: "20", UpperInclusive: true}
	queryMode := q
	queryMode.Table, queryMode.SourceQuery = "", "SELECT id FROM orders"
	alias := queryModeAlias
	tests := []struct {
		name   string
		reader func(db *sql.DB) parityReader
		query  CursorQuery
		want   string
	}{
		{"postgres", func(db *sql.DB) parityReader { return &Postgres{db: db} }, q, `(status = 'open') AND "id" > $1 AND "id" <= $2`},
		{"mysql", func(db *sql.DB) parityReader { return &MySQL{db: db} }, q, "(status = 'open') AND `id` > ? AND `id` <= ?"},
		{"mariadb", func(db *sql.DB) parityReader { return &MariaDB{MySQL: MySQL{db: db}} }, q, "(status = 'open') AND `id` > ? AND `id` <= ?"},
		{"mssql", func(db *sql.DB) parityReader { return &MSSQL{db: db} }, q, `(status = 'open') AND [id] > @p1 AND [id] <= @p2`},
		{"clickhouse", func(db *sql.DB) parityReader { return &ClickHouse{db: db} }, q, "(status = 'open') AND `id` > ? AND `id` <= ?"},
		{"trino", func(db *sql.DB) parityReader { return &Trino{db: db} }, q, `(status = 'open') AND "id" > ? AND "id" <= ?`},
		{"postgres query mode", func(db *sql.DB) parityReader { return &Postgres{db: db} }, queryMode, `(status = 'open') AND "` + alias + `"."id" > $1 AND "` + alias + `"."id" <= $2`},
		{"mysql query mode", func(db *sql.DB) parityReader { return &MySQL{db: db} }, queryMode, "(status = 'open') AND `" + alias + "`.`id` > ? AND `" + alias + "`.`id` <= ?"},
		{"mssql query mode", func(db *sql.DB) parityReader { return &MSSQL{db: db} }, queryMode, "(status = 'open') AND [" + alias + "].[id] > @p1 AND [" + alias + "].[id] <= @p2"},
		{"oracle query mode", func(db *sql.DB) parityReader { return &Oracle{db: db} }, queryMode, "(status = 'open') AND " + alias + `."id" > :1 AND ` + alias + `."id" <= :2`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, cap := newCaptureDB(t)
			rows, _, _, _, err := tc.reader(db).QueryCursor(context.Background(), tc.query)
			if err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if got := whereOf(cap.queries[0].query); got != tc.want {
				t.Fatalf("where=%q want %q", got, tc.want)
			}
		})
	}
}
