package arrowio

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

type nullableMetadataDriver struct{ nullable bool }

func (d nullableMetadataDriver) Open(string) (driver.Conn, error) {
	return nullableMetadataConn{nullable: d.nullable}, nil
}

type nullableMetadataConn struct{ nullable bool }

func (c nullableMetadataConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c nullableMetadataConn) Close() error                        { return nil }
func (c nullableMetadataConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c nullableMetadataConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &nullableMetadataRows{nullable: c.nullable}, nil
}

type nullableMetadataRows struct{ nullable bool }

func (r *nullableMetadataRows) Columns() []string                     { return []string{"value"} }
func (r *nullableMetadataRows) Close() error                          { return nil }
func (r *nullableMetadataRows) Next([]driver.Value) error             { return io.EOF }
func (r *nullableMetadataRows) ColumnTypeDatabaseTypeName(int) string { return "INTEGER" }
func (r *nullableMetadataRows) ColumnTypeNullable(int) (bool, bool)   { return r.nullable, true }

func columnTypesWithNullability(t *testing.T, nullable bool) []*sql.ColumnType {
	t.Helper()
	driverName := fmt.Sprintf("arrowio-nullability-%p", &nullable)
	sql.Register(driverName, nullableMetadataDriver{nullable: nullable})
	db, err := sql.Open(driverName, "")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	rows, err := db.Query("SELECT value")
	require.NoError(t, err)
	t.Cleanup(func() { rows.Close() })
	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	return columnTypes
}

func TestTargetOverridesPreserveSourceNullability(t *testing.T) {
	cols := []string{"value"}

	plans, schema, err := PlansFromSQLEngineWithOverrides("postgres", cols, columnTypesWithNullability(t, true), map[string]string{"value": "Int64"})
	require.NoError(t, err)
	require.True(t, schema.Field(0).Nullable)
	require.NotNil(t, plans[0].Policy)
	require.True(t, plans[0].Policy.Metadata.NullableKnown)
	require.True(t, plans[0].Policy.Metadata.Nullable)

	plans, schema, err = PlansFromSQLEngineWithOverrides("postgres", cols, columnTypesWithNullability(t, false), map[string]string{"value": "Int64"})
	require.NoError(t, err)
	require.False(t, schema.Field(0).Nullable)
	require.NotNil(t, plans[0].Policy)
	require.True(t, plans[0].Policy.Metadata.NullableKnown)
	require.False(t, plans[0].Policy.Metadata.Nullable)

	_, schema, err = PlansFromSQLEngineWithOverrides("postgres", cols, columnTypesWithNullability(t, false), map[string]string{"value": "Nullable(Int64)"})
	require.NoError(t, err)
	require.True(t, schema.Field(0).Nullable)

	_, schema, err = PlansFromSQLEngine("postgres", cols, columnTypesWithNullability(t, false))
	require.NoError(t, err)
	require.Equal(t, arrow.PrimitiveTypes.Int32, schema.Field(0).Type)
	require.False(t, schema.Field(0).Nullable)
}
