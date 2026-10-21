package arrowio

import (
	"fmt"
	"math"
	"regexp"
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
	case "inet":
		// Rendered as canonical IP text; Iceberg has no native IP type.
		return known(typesystem.KindString)
	case "duration":
		// Rendered as ISO-8601 duration text: months/days/nanos do not map to
		// a single fixed interval type.
		return known(typesystem.KindString)
	case "frozen":
		args, ok := cassandraTypeArgs(lower)
		if !ok || len(args) != 1 {
			return cassandraUnknown(lower), nil
		}
		return LogicalTypeForCassandraColumn(args[0], 0, 0, false)
	case "list", "set":
		args, ok := cassandraTypeArgs(lower)
		if !ok || len(args) != 1 {
			return cassandraUnknown(lower), nil
		}
		return cassandraArrayOf(args[0])
	case "vector":
		args, ok := cassandraTypeArgs(lower)
		if !ok || len(args) != 2 {
			return cassandraUnknown(lower), nil
		}
		return cassandraArrayOf(args[0])
	case "map", "tuple":
		// Stored as JSON text: native Iceberg map/struct conversion is not
		// implemented in the storage bridge.
		return typesystem.LogicalType{Kind: typesystem.KindJSON, SourceTypeName: lower}, nil
	case "varint":
		return cassandraUnknown(base), nil
	default:
		if cassandraIdentPattern.MatchString(base) {
			// Any other bare identifier in system_schema.columns is a user-defined
			// type; its values arrive as field maps and are stored as JSON text.
			return typesystem.LogicalType{Kind: typesystem.KindJSON, SourceTypeName: lower}, nil
		}
		return cassandraUnknown(lower), nil
	}
}

var cassandraIdentPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func cassandraArrayOf(elementType string) (typesystem.LogicalType, error) {
	element, err := LogicalTypeForCassandraColumn(elementType, 0, 0, false)
	if err != nil {
		return typesystem.LogicalType{}, err
	}
	return typesystem.ArrayOf(typesystem.Nullable(element)), nil
}

// cassandraTypeArgs splits the top-level arguments of a parameterized CQL type,
// e.g. "map<text, frozen<list<int>>>" -> ["text", "frozen<list<int>>"].
func cassandraTypeArgs(cqlType string) ([]string, bool) {
	cqlType = strings.TrimSpace(cqlType)
	open := strings.IndexByte(cqlType, '<')
	if open < 0 || !strings.HasSuffix(cqlType, ">") {
		return nil, false
	}
	body := cqlType[open+1 : len(cqlType)-1]
	var args []string
	depth, start := 0, 0
	for i, r := range body {
		switch r {
		case '<':
			depth++
		case '>':
			depth--
			if depth < 0 {
				return nil, false
			}
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(body[start:i]))
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, false
	}
	args = append(args, strings.TrimSpace(body[start:]))
	for _, a := range args {
		if a == "" {
			return nil, false
		}
	}
	return args, true
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
