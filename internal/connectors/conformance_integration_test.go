package connectors

// Connector conformance tests against real databases. They run only when the
// matching variable is set, e.g. with docker-compose.ex-db.yml:
//
//	ORABBIT_IT_POSTGRES_DSN=postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable
//	ORABBIT_IT_MARIADB_DSN=root:root@tcp(localhost:3307)/test
//	ORABBIT_IT_MYSQL_DSN=root:root@tcp(localhost:3306)/test
//	ORABBIT_IT_CLICKHOUSE_DSN=clickhouse://default:clickhouse@localhost:9003/default
//	ORABBIT_IT_MONGODB_DSN=mongodb://root:root@localhost:27017/orabbit_it?authSource=admin
//	ORABBIT_IT_ORACLE_DSN=oracle://app:app@localhost:1521/FREEPDB1
//	ORABBIT_IT_TRINO_DSN=http://orabbit@localhost:8080?catalog=memory&schema=default
//	ORABBIT_IT_CASSANDRA_DSN=cassandra://localhost:9042/orabbit_it
//	ORABBIT_IT_MSSQL_DSN=sqlserver://sa:YourStrong!Passw0rd@localhost:1433?database=master
//
// Each SQL engine gets the same table (orabbit_it_orders, 100 rows, id 1..100,
// amount = id) and the same checks, so engines cannot drift apart.

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gocql/gocql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const conformanceTable = "orabbit_it_orders"

type sqlConformanceEngine struct {
	env    string
	engine string
	seedDB func(dsn string) (*sql.DB, error)
	ddl    string
	// table overrides conformanceTable (e.g. Oracle folds unquoted names to upper case).
	table string
	// timestamp formats the created value; default is a plain string literal.
	timestamp string
	// nullableKnown: the engine reports column nullability for validation.
	nullableKnown bool
}

func (e sqlConformanceEngine) tableName() string {
	if e.table != "" {
		return e.table
	}
	return conformanceTable
}

func sqlConformanceEngines() []sqlConformanceEngine {
	mysqlDDL := `CREATE TABLE ` + conformanceTable + ` (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NULL, amount DECIMAL(10,2) NOT NULL, created DATETIME NOT NULL)`
	return []sqlConformanceEngine{
		{
			env: "ORABBIT_IT_POSTGRES_DSN", engine: "postgres", nullableKnown: true,
			seedDB: func(dsn string) (*sql.DB, error) { return sql.Open("pgx", dsn) },
			ddl:    `CREATE TABLE ` + conformanceTable + ` (id BIGINT NOT NULL PRIMARY KEY, name TEXT NULL, amount NUMERIC(10,2) NOT NULL, created TIMESTAMP NOT NULL)`,
		},
		{env: "ORABBIT_IT_MARIADB_DSN", engine: "mariadb", nullableKnown: true, seedDB: func(dsn string) (*sql.DB, error) { return sql.Open("mysql", dsn) }, ddl: mysqlDDL},
		{env: "ORABBIT_IT_MYSQL_DSN", engine: "mysql", nullableKnown: true, seedDB: func(dsn string) (*sql.DB, error) { return sql.Open("mysql", dsn) }, ddl: mysqlDDL},
		{
			env: "ORABBIT_IT_CLICKHOUSE_DSN", engine: "clickhouse",
			seedDB: func(dsn string) (*sql.DB, error) {
				opts, err := clickhouse.ParseDSN(dsn)
				if err != nil {
					return nil, err
				}
				return clickhouse.OpenDB(opts), nil
			},
			ddl: `CREATE TABLE ` + conformanceTable + ` (id Int64, name Nullable(String), amount Decimal(10,2), created DateTime) ENGINE = MergeTree ORDER BY id`,
		},
		{
			// NUMBER(18): wider NUMBERs can exceed int64 and are not range cursors.
			env: "ORABBIT_IT_ORACLE_DSN", engine: "oracle", nullableKnown: true, table: "ORABBIT_IT_ORDERS",
			timestamp: "TIMESTAMP '2026-01-01 00:00:00'",
			seedDB:    func(dsn string) (*sql.DB, error) { return sql.Open("oracle", dsn) },
			ddl:       `CREATE TABLE ORABBIT_IT_ORDERS (ID NUMBER(18) NOT NULL PRIMARY KEY, NAME VARCHAR2(50) NULL, AMOUNT NUMBER(10,2) NOT NULL, CREATED TIMESTAMP NOT NULL)`,
		},
		{
			env: "ORABBIT_IT_MSSQL_DSN", engine: "mssql", nullableKnown: true, table: "dbo." + conformanceTable,
			seedDB: func(dsn string) (*sql.DB, error) { return sql.Open("sqlserver", dsn) },
			ddl:    `CREATE TABLE dbo.` + conformanceTable + ` (id BIGINT NOT NULL PRIMARY KEY, name NVARCHAR(50) NULL, amount DECIMAL(10,2) NOT NULL, created DATETIME2 NOT NULL)`,
		},
		{
			env: "ORABBIT_IT_TRINO_DSN", engine: "trino", timestamp: "TIMESTAMP '2026-01-01 00:00:00'",
			seedDB: func(dsn string) (*sql.DB, error) { return sql.Open("trino", dsn) },
			ddl:    `CREATE TABLE ` + conformanceTable + ` (id BIGINT NOT NULL, name VARCHAR, amount DECIMAL(10,2) NOT NULL, created TIMESTAMP NOT NULL)`,
		},
	}
}

func seedConformanceTable(t *testing.T, ctx context.Context, db *sql.DB, e sqlConformanceEngine) {
	t.Helper()
	table := e.tableName()
	ts := e.timestamp
	if ts == "" {
		ts = "'2026-01-01 00:00:00'"
	}
	dropTable := func(ctx context.Context) {
		if e.engine == "oracle" { // no IF EXISTS before 23ai's syntax in all drivers
			_, _ = db.ExecContext(ctx, "DROP TABLE "+table+" PURGE")
			return
		}
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table)
	}
	dropTable(ctx)
	if _, err := db.ExecContext(ctx, e.ddl); err != nil {
		t.Fatalf("create table: %v", err)
	}
	values := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		name := fmt.Sprintf("'n%d'", i)
		if i%10 == 0 {
			name = "NULL"
		}
		values = append(values, fmt.Sprintf("(%d, %s, %d, %s)", i, name, i, ts))
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" (id, name, amount, created) VALUES "+strings.Join(values, ", ")); err != nil {
		t.Fatalf("insert rows: %v", err)
	}
	if e.engine == "oracle" {
		_, _ = db.ExecContext(ctx, "BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, '"+table+"'); END;")
	}
	t.Cleanup(func() { dropTable(context.Background()) })
}

func countRows(t *testing.T, rows *sql.Rows) int {
	t.Helper()
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return n
}

func TestSQLConnectorConformanceIntegration(t *testing.T) {
	ran := false
	for _, e := range sqlConformanceEngines() {
		dsn := os.Getenv(e.env)
		if dsn == "" {
			continue
		}
		ran = true
		t.Run(e.engine, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			seed, err := e.seedDB(dsn)
			if err != nil {
				t.Fatalf("open seed connection: %v", err)
			}
			defer seed.Close()
			seedConformanceTable(t, ctx, seed, e)
			table := e.tableName()
			// Row counts are catalog estimates; refresh them as an operator would.
			switch e.engine {
			case "postgres":
				_, _ = seed.ExecContext(ctx, "ANALYZE "+conformanceTable)
			case "mysql", "mariadb":
				_, _ = seed.ExecContext(ctx, "ANALYZE TABLE "+conformanceTable)
			}

			r, err := OpenCursorReader(ctx, e.engine, dsn)
			if err != nil {
				t.Fatalf("open reader: %v", err)
			}
			defer r.Close()

			names, _, err := r.DescribeTable(ctx, table)
			if err != nil {
				t.Fatalf("describe: %v", err)
			}
			if got := strings.ToLower(strings.Join(names, ",")); got != "id,name,amount,created" {
				t.Fatalf("columns=%s", got)
			}

			v, err := r.ValidateCursorColumn(ctx, table, "id")
			if err != nil {
				t.Fatalf("validate id: %v", err)
			}
			if !v.Found || !v.RangeCapable || v.Domain != CursorDomainInt64 || (v.NullableKnown && v.Nullable) {
				t.Fatalf("id validation=%+v, want found, range-capable, int64, not null", v)
			}
			if e.nullableKnown {
				nv, err := r.ValidateCursorColumn(ctx, table, "name")
				if err != nil {
					t.Fatalf("validate name: %v", err)
				}
				if !nv.NullableKnown || !nv.Nullable {
					t.Fatalf("name validation=%+v, want known nullable", nv)
				}
			}
			if missing, err := r.ValidateCursorColumn(ctx, table, "no_such_column"); err == nil && missing.Found {
				t.Fatalf("missing column reported found: %+v", missing)
			}

			stats, err := r.DiscoverCursorStats(ctx, table, "id", CursorDomainInt64)
			if err != nil {
				t.Fatalf("stats: %v", err)
			}
			if stats.MinValue != "1" || stats.MaxValue != "100" || stats.RowCount <= 0 {
				t.Fatalf("stats=%+v, want min 1, max 100, rows > 0", stats)
			}

			rangeQuery := CursorQuery{Table: table, CursorColumn: "id", CursorDomain: CursorDomainInt64, LowerBound: "10", LowerExclusive: true, UpperBound: "20", UpperInclusive: true}
			rows, _, _, _, err := r.QueryCursor(ctx, rangeQuery)
			if err != nil {
				t.Fatalf("range query: %v", err)
			}
			if n := countRows(t, rows); n != 10 {
				t.Fatalf("(10, 20] returned %d rows, want 10", n)
			}

			rangeQuery.WhereClause = "amount >= 15"
			rows, _, _, _, err = r.QueryCursor(ctx, rangeQuery)
			if err != nil {
				t.Fatalf("filtered range query: %v", err)
			}
			if n := countRows(t, rows); n != 6 {
				t.Fatalf("(10, 20] with amount >= 15 returned %d rows, want 6", n)
			}

			// Column probes verify user column-type overrides against the data.
			if prober, ok := r.(QueryColumnProber); ok {
				tableQuery, err := TableAsQuery(e.engine, table)
				if err != nil {
					t.Fatalf("table as query: %v", err)
				}
				nameCol, amountCol := names[1], names[2]
				results, err := prober.ProbeQueryColumns(ctx, tableQuery, []ColumnProbe{
					{Column: amountCol, Range: true, Nulls: true, Fraction: true, Scale: 0},
					{Column: nameCol, Nulls: true},
				})
				if err != nil || len(results) != 2 {
					t.Fatalf("probe=%+v err=%v", results, err)
				}
				amount, name := results[0], results[1]
				if amount.Min == nil || amount.Min.Cmp(big.NewRat(1, 1)) != 0 || amount.Max.Cmp(big.NewRat(100, 1)) != 0 || amount.NullCount != 0 || amount.FractionCount != 0 {
					t.Fatalf("amount probe=%+v (min=%v max=%v), want 1..100, no nulls or fractions", amount, amount.Min, amount.Max)
				}
				if name.NullCount != 10 {
					t.Fatalf("name probe nulls=%d, want 10", name.NullCount)
				}
			}

			qr, ok := r.(SourceQueryReader)
			if !ok {
				t.Fatalf("%s reader does not support query mode", e.engine)
			}
			query := "SELECT id, name FROM " + table + " WHERE id <= 50"
			qnames, _, err := qr.DescribeQuery(ctx, query)
			if err != nil || strings.ToLower(strings.Join(qnames, ",")) != "id,name" {
				t.Fatalf("describe query=%v err=%v", qnames, err)
			}
			// As the planner does: validate first, then use the resolved name
			// (Oracle reports unquoted names in upper case).
			qv, err := qr.ValidateQueryCursorColumn(ctx, query, "id")
			if err != nil || !qv.Found {
				t.Fatalf("validate query cursor=%+v err=%v", qv, err)
			}
			qstats, err := qr.DiscoverQueryCursorStats(ctx, query, qv.ResolvedName, CursorDomainInt64)
			if err != nil || qstats.MinValue != "1" || qstats.MaxValue != "50" {
				t.Fatalf("query stats=%+v err=%v, want 1..50", qstats, err)
			}
		})
	}
	if !ran {
		t.Skip("set ORABBIT_IT_*_DSN to run against real databases")
	}
}

func TestMongoDocumentReaderConformanceIntegration(t *testing.T) {
	dsn := os.Getenv("ORABBIT_IT_MONGODB_DSN")
	if dsn == "" {
		t.Skip("set ORABBIT_IT_MONGODB_DSN to run against MongoDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dsn))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(context.Background())
	coll := client.Database("orabbit_it").Collection(conformanceTable)
	_ = coll.Drop(ctx)
	docs := make([]any, 0, 100)
	for i := 1; i <= 100; i++ {
		docs = append(docs, bson.M{"seq": int64(i), "name": fmt.Sprintf("n%d", i)})
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("insert: %v", err)
	}
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	r, err := OpenDocumentReader(ctx, "mongodb", dsn)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer r.Close()

	fields, err := r.DescribeCollection(ctx, conformanceTable)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	found := map[string]bool{}
	for _, f := range fields {
		found[f.Name] = true
	}
	if !found["seq"] || !found["name"] {
		t.Fatalf("fields=%v, want seq and name", fields)
	}
	stats, err := r.DiscoverCollectionStats(ctx, conformanceTable)
	if err != nil || stats.RowCount != 100 {
		t.Fatalf("collection stats=%+v err=%v, want 100 rows", stats, err)
	}
	v, err := r.ValidateCursorColumn(ctx, conformanceTable, "seq")
	if err != nil || !v.Found {
		t.Fatalf("validate seq=%+v err=%v", v, err)
	}
	cstats, err := r.DiscoverCursorStats(ctx, conformanceTable, "seq", CursorDomainInt64)
	if err != nil || cstats.MinValue != "1" || cstats.MaxValue != "100" {
		t.Fatalf("cursor stats=%+v err=%v, want 1..100", cstats, err)
	}

	filter, err := r.BuildCursorFilter(CursorQuery{CursorColumn: "seq", CursorDomain: CursorDomainInt64, LowerBound: "10", LowerExclusive: true, UpperBound: "20", UpperInclusive: true})
	if err != nil {
		t.Fatalf("cursor filter: %v", err)
	}
	it, err := r.StreamDocuments(ctx, conformanceTable, filter, 7)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer it.Close()
	n := 0
	for it.Next(ctx) {
		if _, err := it.Decode(); err != nil {
			t.Fatalf("decode: %v", err)
		}
		n++
	}
	if err := it.Err(); err != nil || n != 10 {
		t.Fatalf("(10, 20] streamed %d docs err=%v, want 10", n, err)
	}
}

// Cassandra reads through its own CQL path, so only the planning contract
// (describe, cursor validation, statistics) is shared with the SQL engines.
func TestCassandraConnectorConformanceIntegration(t *testing.T) {
	dsn := os.Getenv("ORABBIT_IT_CASSANDRA_DSN")
	if dsn == "" {
		t.Skip("set ORABBIT_IT_CASSANDRA_DSN to run against Cassandra")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	parsed, err := parseCassandraDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cluster := gocql.NewCluster(parsed.hosts...)
	cluster.Timeout = 30 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()
	ks := parsed.keyspace
	for _, stmt := range []string{
		"CREATE KEYSPACE IF NOT EXISTS " + ks + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		"DROP TABLE IF EXISTS " + ks + "." + conformanceTable,
		"CREATE TABLE " + ks + "." + conformanceTable + " (id bigint PRIMARY KEY, name text)",
	} {
		if err := session.Query(stmt).WithContext(ctx).Exec(); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() { _ = session.Query("DROP TABLE IF EXISTS " + ks + "." + conformanceTable).Exec() })
	for i := int64(1); i <= 100; i++ {
		if err := session.Query("INSERT INTO "+ks+"."+conformanceTable+" (id, name) VALUES (?, ?)", i, fmt.Sprintf("n%d", i)).WithContext(ctx).Exec(); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	r, err := OpenCursorReader(ctx, "cassandra", dsn)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer r.Close()

	names, _, err := r.DescribeTable(ctx, conformanceTable)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	got := map[string]bool{}
	for _, n := range names {
		got[strings.ToLower(n)] = true
	}
	if !got["id"] || !got["name"] {
		t.Fatalf("columns=%v", names)
	}
	v, err := r.ValidateCursorColumn(ctx, conformanceTable, "id")
	if err != nil || !v.Found || v.Domain != CursorDomainInt64 {
		t.Fatalf("validate id=%+v err=%v", v, err)
	}
	// Cassandra plans by murmur3 token range, so the bounds are sampled
	// partition tokens (int64), not id values.
	stats, err := r.DiscoverCursorStats(ctx, conformanceTable, "id", CursorDomainInt64)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	lo, errLo := strconv.ParseInt(stats.MinValue, 10, 64)
	hi, errHi := strconv.ParseInt(stats.MaxValue, 10, 64)
	if errLo != nil || errHi != nil || lo > hi {
		t.Fatalf("token bounds=%+v, want ordered int64 tokens", stats)
	}
}
