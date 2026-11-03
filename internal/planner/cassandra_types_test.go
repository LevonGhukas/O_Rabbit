package planner

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// cqlColumn is one described Cassandra column for the tests.
type cqlColumn struct {
	name, cqlType string
	nullable      bool
	// unknownNullability reports no nullability at all, as the real
	// Cassandra column types do.
	unknownNullability bool
}

func cqlColumns(t *testing.T, cols ...cqlColumn) ([]string, []*sql.ColumnType) {
	t.Helper()
	registerCQLTypeDriver.Do(func() { sql.Register("orabbit-test-cql-coltypes", cqlTypeDriver{}) })
	spec := make([]string, len(cols))
	names := make([]string, len(cols))
	for i, c := range cols {
		spec[i] = fmt.Sprintf("%s|%s|%t|%t", c.name, c.cqlType, c.nullable, c.unknownNullability)
		names[i] = c.name
	}
	db, err := sql.Open("orabbit-test-cql-coltypes", strings.Join(spec, ";"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	rows, err := db.Query("")
	require.NoError(t, err)
	defer rows.Close()
	cts, err := rows.ColumnTypes()
	require.NoError(t, err)
	return names, cts
}

var registerCQLTypeDriver sync.Once

type cqlTypeDriver struct{}
type cqlTypeConn struct{ cols []cqlColumn }
type cqlTypeRows struct{ cols []cqlColumn }

func (cqlTypeDriver) Open(dsn string) (driver.Conn, error) {
	var cols []cqlColumn
	for _, part := range strings.Split(dsn, ";") {
		f := strings.Split(part, "|")
		cols = append(cols, cqlColumn{name: f[0], cqlType: f[1], nullable: f[2] == "true", unknownNullability: f[3] == "true"})
	}
	return cqlTypeConn{cols: cols}, nil
}
func (c cqlTypeConn) Prepare(string) (driver.Stmt, error) { return cqlTypeStmt(c), nil }
func (cqlTypeConn) Close() error                          { return nil }
func (cqlTypeConn) Begin() (driver.Tx, error)             { return nil, errors.New("no tx") }

type cqlTypeStmt cqlTypeConn

func (cqlTypeStmt) Close() error                                { return nil }
func (cqlTypeStmt) NumInput() int                               { return 0 }
func (cqlTypeStmt) Exec([]driver.Value) (driver.Result, error)  { return nil, errors.New("no exec") }
func (s cqlTypeStmt) Query([]driver.Value) (driver.Rows, error) { return cqlTypeRows(s), nil }
func (r cqlTypeRows) Columns() []string                         { return columnNames(r.cols) }
func (cqlTypeRows) Close() error                                { return nil }
func (cqlTypeRows) Next([]driver.Value) error                   { return io.EOF }
func (r cqlTypeRows) ColumnTypeDatabaseTypeName(i int) string   { return r.cols[i].cqlType }
func (r cqlTypeRows) ColumnTypeNullable(i int) (bool, bool) {
	return r.cols[i].nullable, !r.cols[i].unknownNullability
}

func columnNames(cols []cqlColumn) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name
	}
	return out
}

// fakeDecimalScanner returns fixed stats and records what it was asked.
type fakeDecimalScanner struct {
	stats   map[string]connectors.DecimalColumnStats
	err     error
	scanned [][]string
}

func (f *fakeDecimalScanner) ScanDecimalColumns(_ context.Context, _, _ string, columns []string) (map[string]connectors.DecimalColumnStats, error) {
	f.scanned = append(f.scanned, columns)
	return f.stats, f.err
}

func newTable() existingTable {
	return existingTable{Columns: map[string]typesystem.LogicalType{}, Known: true}
}

func warningFor(warnings []typesystem.TypeWarning, col string) (typesystem.TypeWarning, bool) {
	for _, w := range warnings {
		if w.Column == col {
			return w, true
		}
	}
	return typesystem.TypeWarning{}, false
}

func TestCassandraAutomaticTypesForANewTable(t *testing.T) {
	cols, cts := cqlColumns(t,
		cqlColumn{name: "id", cqlType: "timeuuid", nullable: false},
		cqlColumn{name: "owner", cqlType: "uuid", nullable: true},
		cqlColumn{name: "balance", cqlType: "decimal", nullable: true},
		cqlColumn{name: "empty", cqlType: "decimal", nullable: true},
		cqlColumn{name: "huge", cqlType: "decimal", nullable: true},
		cqlColumn{name: "big", cqlType: "varint", nullable: true},
		cqlColumn{name: "scores", cqlType: "map<text, int>", nullable: true},
		cqlColumn{name: "pair", cqlType: "tuple<int, text>", nullable: true},
		cqlColumn{name: "name", cqlType: "text", nullable: true},
	)
	scanner := &fakeDecimalScanner{stats: map[string]connectors.DecimalColumnStats{
		"balance": {Column: "balance", Values: 3, IntegerDigits: 12, Scale: 4},
		"empty":   {Column: "empty"},
		"huge":    {Column: "huge", Values: 1, IntegerDigits: 30, Scale: 10},
	}}

	auto, warnings := cassandraAutomaticColumnTypes(context.Background(), scanner, "ks.accounts", "", cols, cts, nil, newTable())

	require.Equal(t, map[string]string{
		"balance": "nullable<decimal(38,4)>",
		"empty":   "nullable<decimal(38,18)>",
		"huge":    "nullable<string>",
	}, auto, "varint, map and tuple are native from the schema; uuid, timeuuid and text keep their inferred type")
	require.NotContains(t, auto, "owner", "UUIDs stay canonical text: ClickHouse cannot read Parquet UUID columns")
	require.Equal(t, [][]string{{"balance", "empty", "huge"}}, scanner.scanned, "one scan, decimal columns only")

	w, ok := warningFor(warnings, "huge")
	require.True(t, ok)
	require.Contains(t, w.Reason, "30 digits before and 10 after")
	w, ok = warningFor(warnings, "empty")
	require.True(t, ok)
	require.Contains(t, w.Reason, "no values yet")
	_, ok = warningFor(warnings, "balance")
	require.False(t, ok, "a decimal stored natively needs no warning")
	require.Len(t, warnings, 2)
}

func TestCassandraAutomaticTypesRespectTheUsersChoice(t *testing.T) {
	cols, cts := cqlColumns(t,
		cqlColumn{name: "amount", cqlType: "decimal", nullable: true},
		cqlColumn{name: "price", cqlType: "decimal", nullable: true},
		cqlColumn{name: "ref", cqlType: "uuid", nullable: true},
		cqlColumn{name: "code", cqlType: "uuid", nullable: true},
	)
	scanner := &fakeDecimalScanner{stats: map[string]connectors.DecimalColumnStats{"price": {Values: 1, IntegerDigits: 3, Scale: 2}}}
	overrides := map[string]string{
		"amount": "decimal(12,2)",    // the user's type wins
		"PRICE":  "source",           // automatic, NOT NULL as the placeholder asks
		"ref":    "nullable<source>", // automatic, nullable
		"code":   "string",           // the user's type wins
	}

	auto, _ := cassandraAutomaticColumnTypes(context.Background(), scanner, "ks.t", "", cols, cts, overrides, newTable())

	require.Equal(t, map[string]string{"price": "decimal(38,2)"}, auto)
	require.Equal(t, [][]string{{"price"}}, scanner.scanned)

	merged := withAutomaticColumnTypes(overrides, auto)
	require.Equal(t, map[string]string{"amount": "decimal(12,2)", "price": "decimal(38,2)", "ref": "nullable<source>", "code": "string"}, merged,
		"the placeholder spelled PRICE is replaced, not kept beside the automatic type")
}

func TestCassandraAutomaticTypesKeepAnExistingTablesTypes(t *testing.T) {
	cols, cts := cqlColumns(t,
		cqlColumn{name: "balance", cqlType: "decimal", nullable: true},
		cqlColumn{name: "owner", cqlType: "uuid", nullable: true},
		cqlColumn{name: "scores", cqlType: "map<text, int>", nullable: true},
		cqlColumn{name: "big", cqlType: "varint", nullable: true},
		cqlColumn{name: "tags", cqlType: "list<uuid>", nullable: true},
		cqlColumn{name: "added", cqlType: "decimal", nullable: true},
	)
	str := typesystem.Nullable(typesystem.LogicalType{Kind: typesystem.KindString})
	existing := existingTable{Known: true, Columns: map[string]typesystem.LogicalType{
		"balance": str, // written as text by an earlier release
		"owner":   typesystem.Nullable(typesystem.LogicalType{Kind: typesystem.KindUUID}),
		"scores":  str,
		"big":     typesystem.Nullable(typesystem.Decimal(38, 0)),
		"tags":    typesystem.Nullable(typesystem.ArrayOf(str)),
	}}
	scanner := &fakeDecimalScanner{stats: map[string]connectors.DecimalColumnStats{"added": {Values: 2, IntegerDigits: 2, Scale: 1}}}

	auto, warnings := cassandraAutomaticColumnTypes(context.Background(), scanner, "ks.t", "", cols, cts, nil, existing)

	require.Equal(t, map[string]string{
		"balance": "nullable<string>",
		"owner":   "nullable<uuid>",
		"scores":  "nullable<string>",
		"big":     "nullable<decimal(38,0)>",
		"tags":    "nullable<array<nullable<string>>>",
		"added":   "nullable<decimal(38,1)>", // a new column of an existing table is native
	}, auto)
	require.Equal(t, [][]string{{"added"}}, scanner.scanned, "columns the table already has are not scanned")

	for _, col := range []string{"balance", "scores"} {
		w, ok := warningFor(warnings, col)
		require.True(t, ok, col)
		require.Contains(t, w.Reason, "Iceberg cannot change it", col)
	}
	for _, col := range []string{"owner", "big", "tags", "added"} {
		_, ok := warningFor(warnings, col)
		require.False(t, ok, "%s already has the type a new table would get", col)
	}
}

func TestCassandraAutomaticTypesKeepEarlierTypesWhenTheTableIsUnknown(t *testing.T) {
	cols, cts := cqlColumns(t,
		cqlColumn{name: "balance", cqlType: "decimal", nullable: true},
		cqlColumn{name: "owner", cqlType: "uuid", nullable: false},
		cqlColumn{name: "scores", cqlType: "map<text, int>", nullable: true},
		cqlColumn{name: "big", cqlType: "varint", nullable: true},
		cqlColumn{name: "tags", cqlType: "list<frozen<map<text, int>>>", nullable: true},
	)
	scanner := &fakeDecimalScanner{}

	auto, warnings := cassandraAutomaticColumnTypes(context.Background(), scanner, "ks.t", "", cols, cts, nil, existingTable{Columns: map[string]typesystem.LogicalType{}, Known: false})

	require.Equal(t, map[string]string{
		"balance": "nullable<string>",
		"scores":  "nullable<string>",
		"big":     "nullable<string>",
		"tags":    "nullable<array<nullable<string>>>",
	}, auto, "owner is a UUID, stored as text either way, so it needs no type")
	require.Empty(t, scanner.scanned)
	require.Len(t, warnings, 4)
}

func TestCassandraAutomaticTypesFallBackToTextWhenTheScanFails(t *testing.T) {
	cols, cts := cqlColumns(t, cqlColumn{name: "balance", cqlType: "decimal", nullable: true})
	scanner := &fakeDecimalScanner{err: errors.New("read timeout")}

	auto, warnings := cassandraAutomaticColumnTypes(context.Background(), scanner, "ks.t", "", cols, cts, nil, newTable())

	require.Equal(t, map[string]string{"balance": "nullable<string>"}, auto)
	w, ok := warningFor(warnings, "balance")
	require.True(t, ok)
	require.Contains(t, w.Reason, "read timeout")
	require.Contains(t, w.Reason, "choose a decimal(p,s)")
}

// The real Cassandra column types report no nullability. Automatic types
// must then be nullable, or every value check would warn about NOT NULL.
func TestCassandraAutomaticTypesTreatUnknownNullabilityAsNullable(t *testing.T) {
	cols, cts := cqlColumns(t,
		cqlColumn{name: "owner", cqlType: "uuid", unknownNullability: true},
		cqlColumn{name: "balance", cqlType: "decimal", unknownNullability: true},
	)
	scanner := &fakeDecimalScanner{stats: map[string]connectors.DecimalColumnStats{"balance": {Values: 1, IntegerDigits: 1, Scale: 2}}}

	auto, warnings := cassandraAutomaticColumnTypes(context.Background(), scanner, "ks.t", "", cols, cts, nil, newTable())

	require.Equal(t, map[string]string{"balance": "nullable<decimal(38,2)>"}, auto)
	require.Empty(t, warnings)
}
