package arrowio

import (
	"strings"
)

func planPostgresColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])

	// 1. Array types (e.g. INTEGER[], NUMERIC[], _INT4, _TEXT, etc.)
	if strings.HasSuffix(clean, "[]") {
		elemType := strings.TrimSuffix(clean, "[]")
		elemPlan := planPostgresColumn("item", elemType, precision, scale, hasDecimal)
		if !hasDecimal && (elemType == "NUMERIC" || elemType == "DECIMAL") {
			// PostgreSQL array semantics are deferred to Phase 2; retain the
			// existing array element mapping in this focused scalar decimal change.
			elemPlan = planDecimal128("item", 38, 10)
		}
		return planList(name, elemPlan)
	}
	if strings.HasPrefix(clean, "_") {
		elemType := strings.TrimPrefix(clean, "_")
		elemPlan := planPostgresColumn("item", elemType, precision, scale, hasDecimal)
		return planList(name, elemPlan)
	}

	switch clean {
	// 2. Integers & Serials
	case "INT2", "SMALLINT", "SMALLSERIAL":
		return planInt16(name)
	case "INT4", "INTEGER", "INT", "SERIAL":
		return planInt32(name)
	case "INT8", "BIGINT", "BIGSERIAL":
		return planInt64(name)

	// 3. Floats
	case "FLOAT4", "REAL":
		return planFloat32(name)
	case "FLOAT8", "DOUBLE PRECISION", "FLOAT":
		return planFloat64(name)

	// 4. Exact Decimals & Monetary
	case "NUMERIC", "DECIMAL":
		return planDeclaredDecimal(name, precision, scale, hasDecimal)

	case "MONEY":
		return planDecimal128(name, 19, 2)

	// 5. Booleans & Bits
	case "BOOL", "BOOLEAN":
		return planBool(name)
	case "BIT":
		if base == "BIT(1)" {
			return planBool(name)
		}
		return planUint64(name)
	case "VARBIT":
		return planString(name)

	// 6. Dates & Timestamps
	case "DATE":
		return planDate32(name)
	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		return planTimestampUs(name, "")
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return planTimestampUs(name, "UTC")
	case "TIME", "TIME WITHOUT TIME ZONE", "TIMETZ", "TIME WITH TIME ZONE":
		return planTime64(name)

	// 7. Binary & Strings
	case "BYTEA":
		return planBinary(name)
	case "UUID", "JSON", "JSONB", "XML", "TEXT", "VARCHAR", "CHAR", "BPCHAR", "NAME", "CITEXT", "INET", "CIDR", "MACADDR", "MACADDR8":
		return planString(name)

	default:
		return planGenericSQLColumn(name, base, precision, scale, hasDecimal)
	}
}
