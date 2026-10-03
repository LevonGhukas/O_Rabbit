package arrowio

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

const testUUID = "123e4567-e89b-12d3-a456-426614174000"

func TestInferredUUIDStaysTextWithoutOverride(t *testing.T) {
	for _, overrides := range []map[string]string{nil, {"id": "source"}, {"id": "nullable<source>"}} {
		_, s, err := PlansFromSQLEngineWithOverrides("cassandra", []string{"id"}, uuidColumnTypes(t), overrides)
		require.NoError(t, err)
		require.Equal(t, arrow.BinaryTypes.String, s.Field(0).Type, "overrides %v", overrides)
	}
}

func TestExplicitUUIDOverrideWritesNativeUUIDThroughParquetToIceberg(t *testing.T) {
	plans, s, err := PlansFromSQLEngineWithOverrides("cassandra", []string{"id"}, uuidColumnTypes(t), map[string]string{"id": "nullable<uuid>"})
	require.NoError(t, err)
	require.Equal(t, "arrow.uuid", s.Field(0).Type.(arrow.ExtensionType).ExtensionName())

	b := plans[0].Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plans[0].Append(b, testUUID))
	require.NoError(t, plans[0].Append(b, nil))
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecord(s, []arrow.Array{col}, 2)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(s, &buf, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	_, isUUID := pf.MetaData().Schema.Column(0).LogicalType().(schema.UUIDLogicalType)
	require.True(t, isUUID, "parquet column must carry the UUID logical type")

	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := fr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()
	require.Equal(t, testUUID, tbl.Column(0).Data().Chunk(0).ValueStr(0))
	require.True(t, tbl.Column(0).Data().Chunk(0).IsNull(1))

	ice, err := icetable.ArrowSchemaToIcebergWithFreshIDs(s, false)
	require.NoError(t, err)
	require.Equal(t, iceberg.PrimitiveTypes.UUID, ice.Field(0).Type)
}

func TestExplicitUUIDOverrideReportsNativeTargetWithoutWarning(t *testing.T) {
	result, err := PlansFromSQLEngineResult("cassandra", []string{"id"}, uuidColumnTypes(t), map[string]string{"id": "uuid"})
	require.NoError(t, err)
	require.Empty(t, result.Warnings)

	_, mapping, err := PlanForOverride("id", "uuid", mustCassandraLogical(t, "uuid"))
	require.NoError(t, err)
	require.Equal(t, "uuid", mapping.Target)
	require.False(t, mapping.Fallback)
}

func mustCassandraLogical(t *testing.T, cqlType string) typesystem.LogicalType {
	t.Helper()
	l, err := LogicalTypeForCassandraColumn(cqlType, 0, 0, false)
	require.NoError(t, err)
	return l
}

// uuidColumnTypes returns a real *sql.ColumnType reporting the CQL type "uuid".
func uuidColumnTypes(t *testing.T) []*sql.ColumnType {
	t.Helper()
	registerUUIDTypeDriver.Do(func() { sql.Register("orabbit-test-uuid-coltype", uuidTypeDriver{}) })
	db, err := sql.Open("orabbit-test-uuid-coltype", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	rows, err := db.Query("")
	require.NoError(t, err)
	defer rows.Close()
	cts, err := rows.ColumnTypes()
	require.NoError(t, err)
	return cts
}

var registerUUIDTypeDriver sync.Once

type uuidTypeDriver struct{}
type uuidTypeConn struct{}
type uuidTypeStmt struct{}
type uuidTypeRows struct{}

func (uuidTypeDriver) Open(string) (driver.Conn, error)         { return uuidTypeConn{}, nil }
func (uuidTypeConn) Prepare(string) (driver.Stmt, error)        { return uuidTypeStmt{}, nil }
func (uuidTypeConn) Close() error                               { return nil }
func (uuidTypeConn) Begin() (driver.Tx, error)                  { return nil, fmt.Errorf("no tx") }
func (uuidTypeStmt) Close() error                               { return nil }
func (uuidTypeStmt) NumInput() int                              { return 0 }
func (uuidTypeStmt) Exec([]driver.Value) (driver.Result, error) { return nil, fmt.Errorf("no exec") }
func (uuidTypeStmt) Query([]driver.Value) (driver.Rows, error)  { return uuidTypeRows{}, nil }
func (uuidTypeRows) Columns() []string                          { return []string{"id"} }
func (uuidTypeRows) Close() error                               { return nil }
func (uuidTypeRows) Next([]driver.Value) error                  { return fmt.Errorf("EOF") }
func (uuidTypeRows) ColumnTypeDatabaseTypeName(int) string      { return "uuid" }
