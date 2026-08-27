package connectors

import (
	"fmt"
	"strings"
)

// FallbackProjectionSQL returns an engine-native exact text expression. The
// caller supplies a safely quoted column reference and aliases the result back
// to the original result-column name.
func FallbackProjectionSQL(engine, column string, p FallbackProjection) (string, bool) {
	if p.Name == "" || column == "" {
		return "", false
	}
	switch NormalizeSourceEngine(engine) {
	case "mssql":
		switch p.Encoding {
		case "mssql_datetime2_text_v1":
			return fmt.Sprintf("CONVERT(NVARCHAR(34), %s, 126)", column), true
		case "mssql_time_text_v1":
			return fmt.Sprintf("CONVERT(NVARCHAR(32), %s)", column), true
		case "mssql_datetimeoffset_text_v1":
			return fmt.Sprintf("CONVERT(NVARCHAR(40), %s, 127)", column), true
		case "xml_utf8_text_v1":
			return fmt.Sprintf("CONVERT(NVARCHAR(MAX), %s)", column), true
		}
	case "oracle":
		precision := p.TemporalPrecision
		if precision < 0 || precision > 9 {
			return "", false
		}
		frac := fmt.Sprintf("FF%d", precision)
		switch p.Encoding {
		case "oracle_timestamp_text_v1":
			return fmt.Sprintf("TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS.%s')", column, frac), true
		case "oracle_timestamptz_text_v1":
			return fmt.Sprintf("TO_CHAR(%s, 'YYYY-MM-DD\"T\"HH24:MI:SS.%s TZH:TZM')", column, frac), true
		}
	case "clickhouse":
		if p.Encoding == "clickhouse_datetime64_text_v1" {
			return fmt.Sprintf("toString(%s)", column), true
		}
	}
	return "", false
}

func fallbackProjectionForName(projections []FallbackProjection, name string) (FallbackProjection, bool) {
	for _, p := range projections {
		if strings.EqualFold(strings.TrimSpace(p.Name), strings.TrimSpace(name)) {
			return p, true
		}
	}
	return FallbackProjection{}, false
}
