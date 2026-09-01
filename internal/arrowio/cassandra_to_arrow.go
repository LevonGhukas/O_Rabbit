package arrowio

import (
	"fmt"
	"math"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func LogicalTypeForCassandraColumn(dbType string, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	raw := strings.TrimSpace(dbType)
	lower := strings.ToLower(raw)
	base := lower
	if i := strings.IndexAny(base, "(<"); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	known := func(k typesystem.Kind) (typesystem.LogicalType, error) { return typesystem.LogicalType{Kind: k}, nil }
	switch base {
	case "tinyint":
		return known(typesystem.KindInt8)
	case "smallint":
		return known(typesystem.KindInt16)
	case "int", "integer":
		return known(typesystem.KindInt32)
	case "bigint", "counter":
		return known(typesystem.KindInt64)
	case "float":
		return known(typesystem.KindFloat32)
	case "double":
		return known(typesystem.KindFloat64)
	case "boolean", "bool":
		return known(typesystem.KindBool)
	case "date":
		return known(typesystem.KindDate)
	case "time":
		return known(typesystem.KindTime)
	case "timestamp":
		return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}, nil
	case "blob":
		return known(typesystem.KindBinary)
	case "text", "varchar", "ascii":
		return known(typesystem.KindString)
	case "uuid", "timeuuid":
		return known(typesystem.KindUUID)
	case "decimal":
		if !hasDecimal || precision <= 0 || scale < 0 || scale > precision || precision > math.MaxInt32 || scale > math.MaxInt32 {
			return cassandraUnknown(base), nil
		}
		return typesystem.Decimal(int32(precision), int32(scale)), nil
	case "varint", "inet", "list", "set", "map", "tuple", "udt", "frozen", "duration":
		return cassandraUnknown(base), nil
	default:
		return cassandraUnknown(lower), nil
	}
}

func cassandraUnknown(source string) typesystem.LogicalType {
	return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: source}
}

func planCassandraColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	t, err := LogicalTypeForCassandraColumn(dbType, precision, scale, hasDecimal)
	if err == nil {
		if plan, _, planErr := PlanForLogicalType(name, t); planErr == nil {
			return plan
		}
	}
	plan, _, fallbackErr := PlanForLogicalType(name, cassandraUnknown(strings.ToLower(strings.TrimSpace(dbType))))
	if fallbackErr != nil {
		panic(fmt.Sprintf("Cassandra fallback plan: %v", fallbackErr))
	}
	return plan
}
