package arrowio

import (
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// LogicalTypeForPostgresColumn translates PostgreSQL source metadata into
// source-independent semantics. Unsupported PostgreSQL semantic types remain
// unknown so their values take the explicit lossless fallback path.
func LogicalTypeForPostgresColumn(dbType string, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	raw := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(raw, "(")[0])
	if strings.HasSuffix(clean, "[]") {
		element, err := LogicalTypeForPostgresColumn(strings.TrimSuffix(clean, "[]"), precision, scale, hasDecimal)
		if err != nil {
			return typesystem.LogicalType{}, err
		}
		// PostgreSQL array text represents NULL elements explicitly, but column
		// metadata does not expose element nullability. Preserve that practical
		// source behavior rather than rejecting valid driver values.
		element.Nullable = true
		return typesystem.ArrayOf(element), nil
	}
	if strings.HasPrefix(clean, "_") {
		element, err := LogicalTypeForPostgresColumn(strings.TrimPrefix(clean, "_"), precision, scale, hasDecimal)
		if err != nil {
			return typesystem.LogicalType{}, err
		}
		element.Nullable = true
		return typesystem.ArrayOf(element), nil
	}
	known := func(kind typesystem.Kind) (typesystem.LogicalType, error) {
		return typesystem.LogicalType{Kind: kind}, nil
	}
	switch clean {
	case "INT2", "SMALLINT", "SMALLSERIAL":
		return known(typesystem.KindInt16)
	case "INT4", "INTEGER", "INT", "SERIAL":
		return known(typesystem.KindInt32)
	case "INT8", "BIGINT", "BIGSERIAL":
		return known(typesystem.KindInt64)
	case "FLOAT4", "REAL":
		return known(typesystem.KindFloat32)
	case "FLOAT8", "DOUBLE PRECISION", "FLOAT":
		return known(typesystem.KindFloat64)
	case "NUMERIC", "DECIMAL":
		if !hasDecimal {
			return postgresUnknown(clean), nil
		}
		t := typesystem.Decimal(int32(precision), int32(scale))
		if err := t.Validate(); err != nil {
			return postgresUnknown(clean), nil
		}
		return t, nil
	case "MONEY":
		return typesystem.Decimal(19, 2), nil
	case "BOOL", "BOOLEAN":
		return known(typesystem.KindBool)
	case "DATE":
		return known(typesystem.KindDate)
	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		return known(typesystem.KindTimestamp)
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}, nil
	case "TIME", "TIME WITHOUT TIME ZONE":
		return known(typesystem.KindTime)
	case "TIMETZ", "TIME WITH TIME ZONE":
		return postgresUnknown(clean), nil
	case "BYTEA":
		return known(typesystem.KindBinary)
	case "UUID":
		return known(typesystem.KindUUID)
	case "JSON", "JSONB":
		return known(typesystem.KindJSON)
	case "TEXT", "VARCHAR", "CHAR", "BPCHAR", "NAME", "CITEXT":
		return known(typesystem.KindString)
	default:
		return postgresUnknown(clean), nil
	}
}

func postgresUnknown(source string) typesystem.LogicalType {
	return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: source}
}

// planPostgresColumn retains the legacy planner signature while routing only
// PostgreSQL through LogicalType, shared conversion, and canonical appenders.
func planPostgresColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	t, err := LogicalTypeForPostgresColumn(dbType, precision, scale, hasDecimal)
	if err == nil {
		plan, _, planErr := PlanForLogicalType(name, t)
		if planErr == nil {
			return plan
		}
	}
	plan, _, fallbackErr := PlanForLogicalType(name, postgresUnknown(strings.ToUpper(strings.TrimSpace(dbType))))
	if fallbackErr != nil {
		panic(fmt.Sprintf("postgres fallback plan: %v", fallbackErr))
	}
	return plan
}
