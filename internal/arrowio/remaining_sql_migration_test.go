package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func TestTrinoLogicalMigration(t *testing.T) {
	for _, test := range []struct {
		source string
		want   typesystem.LogicalType
	}{{"boolean", typesystem.LogicalType{Kind: typesystem.KindBool}}, {"tinyint", typesystem.LogicalType{Kind: typesystem.KindInt8}}, {"decimal(18,2)", typesystem.Decimal(18, 2)}, {"decimal(50,10)", typesystem.Decimal(50, 10)}, {"time", typesystem.LogicalType{Kind: typesystem.KindTime}}, {"timestamp(6) with time zone", typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}}, {"array(uuid)", typesystem.ArrayOf(typesystem.LogicalType{Kind: typesystem.KindUUID})}} {
		got, err := LogicalTypeForTrinoColumn(test.source, 0, 0, false)
		require.NoError(t, err)
		require.True(t, got.Equal(test.want))
	}
	for _, source := range []string{"decimal", "time with time zone", "row(x bigint)", "map(varchar,bigint)", "ipaddress", "interval day to second", "extension"} {
		got, err := LogicalTypeForTrinoColumn(source, 0, 0, false)
		require.NoError(t, err)
		require.Equal(t, typesystem.KindUnknown, got.Kind)
	}
	for _, test := range []struct {
		source string
		value  any
	}{{"tinyint", 128}, {"boolean", "bad"}, {"decimal(18,2)", "bad"}, {"date", "bad"}, {"timestamp", "bad"}} {
		p := PlanForSQLColumn("trino", "v", test.source, 0, 0, false)
		b := p.Builder(memory.DefaultAllocator)
		require.Error(t, p.Append(b, test.value))
		b.Release()
	}
	fields := []arrow.Field{{Name: "big", Type: PlanForSQLColumn("trino", "big", "bigint", 0, 0, false).DataType}, {Name: "uuid", Type: PlanForSQLColumn("trino", "uuid", "uuid", 0, 0, false).DataType}, {Name: "items", Type: PlanForSQLColumn("trino", "items", "array(integer)", 0, 0, false).DataType}}
	schema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrow.NewSchema(fields, nil), false)
	require.NoError(t, err)
	require.Equal(t, "long", schema.Fields()[0].Type.String())
	require.Equal(t, "string", schema.Fields()[1].Type.String())
	require.Equal(t, "list<int>", schema.Fields()[2].Type.String())
}

func TestCassandraLogicalMigration(t *testing.T) {
	for _, test := range []struct {
		source string
		want   typesystem.Kind
	}{{"bigint", typesystem.KindInt64}, {"timestamp", typesystem.KindTimestampTZ}, {"blob", typesystem.KindBinary}, {"uuid", typesystem.KindUUID}, {"varint", typesystem.KindUnknown}, {"decimal", typesystem.KindUnknown}, {"list<text>", typesystem.KindUnknown}, {"inet", typesystem.KindUnknown}, {"extension", typesystem.KindUnknown}} {
		got, err := LogicalTypeForCassandraColumn(test.source, 0, 0, false)
		require.NoError(t, err)
		require.Equal(t, test.want, got.Kind)
	}
	decimal, err := LogicalTypeForCassandraColumn("decimal", 18, 2, true)
	require.NoError(t, err)
	require.True(t, decimal.Equal(typesystem.Decimal(18, 2)))
	p := PlanForSQLColumn("cassandra", "v", "blob", 0, 0, false)
	b := p.Builder(memory.DefaultAllocator)
	require.NoError(t, p.Append(b, []byte{1, 2}))
	bytes := b.NewArray().(*array.Binary)
	require.Equal(t, []byte{1, 2}, bytes.Value(0))
	bytes.Release()
	b.Release()
	for _, test := range []struct {
		source string
		value  any
	}{{"tinyint", 128}, {"boolean", "bad"}, {"date", "bad"}, {"timestamp", "bad"}} {
		p := PlanForSQLColumn("cassandra", "v", test.source, 0, 0, false)
		b := p.Builder(memory.DefaultAllocator)
		require.Error(t, p.Append(b, test.value))
		b.Release()
	}
}

func TestSQLiteLogicalMigration(t *testing.T) {
	for _, test := range []struct {
		source string
		want   typesystem.Kind
	}{{"INTEGER", typesystem.KindInt64}, {"INT8", typesystem.KindInt8}, {"UINT16", typesystem.KindUInt16}, {"BIGINT", typesystem.KindInt64}, {"REAL", typesystem.KindFloat64}, {"BOOL", typesystem.KindBool}, {"DATE", typesystem.KindDate}, {"TIME", typesystem.KindTime}, {"DATETIME", typesystem.KindTimestamp}, {"BLOB", typesystem.KindBinary}, {"VARCHAR(100)", typesystem.KindString}, {"CUSTOM_AFFINITY", typesystem.KindUnknown}, {"", typesystem.KindUnknown}} {
		got, err := LogicalTypeForSQLiteColumn(test.source, 0, 0, false)
		require.NoError(t, err)
		require.Equal(t, test.want, got.Kind)
	}
	decimal, err := LogicalTypeForSQLiteColumn("DECIMAL(18,2)", 0, 0, false)
	require.NoError(t, err)
	require.True(t, decimal.Equal(typesystem.Decimal(18, 2)))
	unknown, err := LogicalTypeForSQLiteColumn("NUMERIC", 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, typesystem.KindUnknown, unknown.Kind)
	for _, test := range []struct {
		source string
		value  any
	}{{"INT8", 128}, {"BOOL", "bad"}, {"DECIMAL(18,2)", "bad"}, {"DATE", "bad"}, {"TIME", "bad"}} {
		p := PlanForSQLColumn("sqlite", "v", test.source, 0, 0, false)
		b := p.Builder(memory.DefaultAllocator)
		require.Error(t, p.Append(b, test.value))
		b.Release()
	}
	fields := []arrow.Field{{Name: "i", Type: PlanForSQLColumn("sqlite", "i", "INTEGER", 0, 0, false).DataType}, {Name: "d", Type: PlanForSQLColumn("sqlite", "d", "DECIMAL(18,2)", 0, 0, false).DataType}, {Name: "u", Type: PlanForSQLColumn("sqlite", "u", "CUSTOM", 0, 0, false).DataType}}
	schema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrow.NewSchema(fields, nil), false)
	require.NoError(t, err)
	require.Equal(t, "long", schema.Fields()[0].Type.String())
	require.Equal(t, "decimal(18, 2)", schema.Fields()[1].Type.String())
	require.Equal(t, "string", schema.Fields()[2].Type.String())
}
