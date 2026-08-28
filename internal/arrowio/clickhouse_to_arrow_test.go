package arrowio

import (
	"context"
	"os"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestClickHouseAllTypesParquetSchema(t *testing.T) {
	colTypes := map[string]string{
		"id":                         "UInt64",
		"int8_col":                   "Int8",
		"int16_col":                  "Int16",
		"int32_col":                  "Int32",
		"int64_col":                  "Int64",
		"int128_col":                 "Int128",
		"int256_col":                 "Int256",
		"uint8_col":                  "UInt8",
		"uint16_col":                 "UInt16",
		"uint32_col":                 "UInt32",
		"uint64_col":                 "UInt64",
		"uint128_col":                "UInt128",
		"uint256_col":                "UInt256",
		"float32_col":                "Float32",
		"float64_col":                "Float64",
		"decimal32_col":              "Decimal(9, 2)",
		"decimal64_col":              "Decimal(18, 4)",
		"decimal128_col":             "Decimal(38, 6)",
		"decimal256_col":             "Decimal(76, 8)",
		"bool_col":                   "Bool",
		"string_col":                 "String",
		"fixed_string_col":           "FixedString(10)",
		"uuid_col":                   "UUID",
		"date_col":                   "Date",
		"date32_col":                 "Date32",
		"datetime_col":               "DateTime",
		"datetime64_col":             "DateTime64(3)",
		"enum8_col":                  "Enum8('unknown' = 0, 'active' = 1, 'inactive' = 2)",
		"enum16_col":                 "Enum16('small' = 1, 'medium' = 2, 'large' = 3)",
		"low_cardinality_string_col": "LowCardinality(String)",
		"nullable_int_col":           "Nullable(Int32)",
		"nullable_string_col":        "Nullable(String)",
		"nullable_date_col":          "Nullable(Date)",
		"array_int_col":              "Array(Int32)",
		"array_string_col":           "Array(String)",
		"array_nullable_int_col":     "Array(Nullable(Int32))",
		"tuple_col":                  "Tuple(Int32, String)",
		"tuple_named_col":            "Tuple(\n    number Int32,\n    text String)",
		"map_string_int_col":         "Map(String, Int32)",
		"map_string_string_col":      "Map(String, String)",
		"nested_col.name":            "Array(String)",
		"nested_col.value":           "Array(Int32)",
		"ipv4_col":                   "IPv4",
		"ipv6_col":                   "IPv6",
		"json_col":                   "JSON",
	}

	selectCols := []string{
		"id", "int8_col", "int16_col", "int32_col", "int64_col", "int128_col", "int256_col",
		"uint8_col", "uint16_col", "uint32_col", "uint64_col", "uint128_col", "uint256_col",
		"float32_col", "float64_col", "decimal32_col", "decimal64_col", "decimal128_col", "decimal256_col",
		"bool_col", "string_col", "fixed_string_col", "uuid_col", "date_col", "date32_col",
		"datetime_col", "datetime64_col", "enum8_col", "enum16_col", "low_cardinality_string_col",
		"nullable_int_col", "nullable_string_col", "nullable_date_col", "array_int_col", "array_string_col",
		"array_nullable_int_col", "tuple_col", "tuple_named_col", "map_string_int_col", "map_string_string_col",
		"nested_col.name", "nested_col.value", "ipv4_col", "ipv6_col", "json_col",
	}

	plans := make([]ColumnPlan, len(selectCols))
	fields := make([]arrow.Field, len(selectCols))
	builders := make([]array.Builder, len(selectCols))

	mem := memory.DefaultAllocator
	for i, col := range selectCols {
		dbType := colTypes[col]
		plans[i] = PlanForSQLColumn("clickhouse", col, dbType, 0, 0, false)
		fields[i] = arrow.Field{Name: col, Type: plans[i].DataType, Nullable: true}
		builders[i] = plans[i].Builder(mem)
		defer builders[i].Release()
		_ = plans[i].Append(builders[i], nil)
	}

	schema := arrow.NewSchema(fields, nil)
	arrays := make([]arrow.Array, len(selectCols))
	for i, b := range builders {
		arrays[i] = b.NewArray()
		defer arrays[i].Release()
	}

	batch := array.NewRecordBatch(schema, arrays, 1)
	defer batch.Release()

	w, path, err := parquetio.NewTempFileWriter(schema, parquetio.Options{})
	require.NoError(t, err)
	defer func() { _ = os.Remove(path) }()

	err = w.Write(batch)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	_, err = artifact.ValidateLocalParquet(context.Background(), path, 1, schema)
	require.NoError(t, err)
}
