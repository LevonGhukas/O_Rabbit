package arrowio

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// LogicalFamily is source meaning, independent of cursor classification.
type LogicalFamily string
type TemporalSemantics string
type RepresentationMode string

const (
	RepresentationNative      RepresentationMode = "native"
	RepresentationFallback    RepresentationMode = "fallback"
	RepresentationUnsupported RepresentationMode = "unsupported"

	TemporalNone           TemporalSemantics = ""
	TemporalDate           TemporalSemantics = "date"
	TemporalTime           TemporalSemantics = "time"
	TemporalLocalTimestamp TemporalSemantics = "local_timestamp"
	TemporalInstant        TemporalSemantics = "instant"
	TemporalZonedTime      TemporalSemantics = "unsupported_zoned_time"
)

// TypeCapability records whether the configured Arrow/Parquet/Iceberg v2 path
// can preserve a field exactly. A false value is a planning error, never a
// request to coerce a source value.
type TypeCapability struct {
	ArrowExact, ParquetExact, IcebergExact, ClickHouseExact bool
	Reason                                                  string
}

const (
	LogicalUnsupported LogicalFamily = "unsupported"
	LogicalSignedInt   LogicalFamily = "signed_integer"
	LogicalUnsignedInt LogicalFamily = "unsigned_integer"
	LogicalDecimal     LogicalFamily = "decimal"
	LogicalFloat       LogicalFamily = "float"
	LogicalBoolean     LogicalFamily = "boolean"
	LogicalString      LogicalFamily = "string"
	LogicalBinary      LogicalFamily = "binary"
	LogicalDate        LogicalFamily = "date"
	LogicalTime        LogicalFamily = "time"
	LogicalTimestamp   LogicalFamily = "local_timestamp"
	LogicalInstant     LogicalFamily = "instant"
	LogicalUUID        LogicalFamily = "uuid"
)

// SourceFieldDescriptor is immutable after planning. It carries source facts
// used to produce both Arrow fields and conversion codecs.
type SourceFieldDescriptor struct {
	Name, Engine, SourceType, SourceTimezone string
	Ordinal                                  int
	Nullable, NullableKnown                  bool
	LogicalFamily                            LogicalFamily
	BitWidth                                 int
	Signed                                   *bool
	Precision, Scale                         int32
	PrecisionKnown, ScaleKnown               bool
	Length                                   int64
	LengthKnown, FixedLength                 bool
	TemporalPrecision                        int
	TemporalPrecisionKnown                   bool
	TemporalSemantics                        TemporalSemantics
	Unsigned                                 bool
	FixedBinary                              bool
	FallbackEncoding                         string
	Representation                           RepresentationMode
	Capability                               TypeCapability
	ArrowType                                arrow.DataType
}

// ConversionError reports an exactness failure without exposing source value.
type ConversionError struct {
	Column, SourceType, TargetType, ValueType, Reason string
}

func (e *ConversionError) Error() string {
	return fmt.Sprintf("column %q %s -> %s: cannot convert driver value %s losslessly: %s", e.Column, e.SourceType, e.TargetType, e.ValueType, e.Reason)
}

func descriptorsFromSQL(engine string, cols []string, colTypes []*sql.ColumnType) ([]SourceFieldDescriptor, error) {
	if colTypes != nil && len(cols) != len(colTypes) {
		return nil, fmt.Errorf("cols/colTypes length mismatch")
	}
	out := make([]SourceFieldDescriptor, len(cols))
	for i, name := range cols {
		d := SourceFieldDescriptor{Name: name, Ordinal: i, Engine: strings.ToLower(strings.TrimSpace(engine)), LogicalFamily: LogicalUnsupported}
		if colTypes != nil && colTypes[i] != nil {
			ct := colTypes[i]
			rawType := strings.TrimSpace(ct.DatabaseTypeName())
			d.SourceType = strings.ToUpper(rawType)
			if d.Engine == "clickhouse" || d.Engine == "ch" {
				d.SourceTimezone = clickHouseTimezone(rawType)
			}
			if p, s, ok := ct.DecimalSize(); ok {
				d.Precision, d.Scale, d.PrecisionKnown, d.ScaleKnown = int32(p), int32(s), true, true
			}
			if n, ok := ct.Nullable(); ok {
				d.Nullable, d.NullableKnown = n, true
			}
			if n, ok := ct.Length(); ok {
				d.Length, d.LengthKnown = n, true
			}
		}
		if exactDecimalType(d.Engine, d.SourceType) && (!d.PrecisionKnown || !d.ScaleKnown) {
			if m := precScaleRe.FindStringSubmatch(d.SourceType); m != nil && m[1] != "" {
				if p, err := strconv.ParseInt(m[1], 10, 32); err == nil {
					d.Precision, d.PrecisionKnown = int32(p), true
				}
				if len(m) > 2 && m[2] != "" {
					if s, err := strconv.ParseInt(m[2], 10, 32); err == nil {
						d.Scale, d.ScaleKnown = int32(s), true
					}
				}
			}
		}
		classifyTemporalDescriptor(&d)
		classifySourceRepresentation(&d)
		// clickhouse-go may expose only decimal scale in DecimalSize. Width is
		// intrinsic to Decimal32/64/128/256, so recover it from native type.
		if d.Engine == "clickhouse" {
			upper := strings.ToUpper(d.SourceType)
			for _, spec := range []struct {
				prefix    string
				precision int32
			}{{"DECIMAL32", 9}, {"DECIMAL64", 18}, {"DECIMAL128", 38}, {"DECIMAL256", 76}} {
				if strings.HasPrefix(upper, spec.prefix) {
					d.Precision, d.PrecisionKnown = spec.precision, true
					if m := precScaleRe.FindStringSubmatch(upper); m != nil && m[1] != "" {
						if s, err := strconv.ParseInt(m[1], 10, 32); err == nil {
							d.Scale, d.ScaleKnown = int32(s), true
						}
					}
					break
				}
			}
		}
		classifyFallbackRepresentation(&d)
		out[i] = d
	}
	return out, nil
}

func arrowFieldFromDescriptor(d SourceFieldDescriptor) arrow.Field {
	md := arrow.NewMetadata([]string{
		"orabbit.source.engine", "orabbit.source.type", "orabbit.source.precision", "orabbit.source.scale", "orabbit.source.logical_family", "orabbit.source.temporal_semantics", "orabbit.source.temporal_precision", "orabbit.source.timezone", "orabbit.source.unsigned", "orabbit.source.fixed_binary", "orabbit.representation", "orabbit.fallback.encoding",
	}, []string{d.Engine, d.SourceType, strconv.FormatInt(int64(d.Precision), 10), strconv.FormatInt(int64(d.Scale), 10), string(d.LogicalFamily), string(d.TemporalSemantics), strconv.Itoa(d.TemporalPrecision), d.SourceTimezone, strconv.FormatBool(d.Unsigned), strconv.FormatBool(d.FixedBinary), string(d.Representation), d.FallbackEncoding})
	return arrow.Field{Name: d.Name, Type: d.ArrowType, Nullable: d.Nullable || !d.NullableKnown, Metadata: md}
}

func classifyFallbackRepresentation(d *SourceFieldDescriptor) {
	if d.FallbackEncoding != "" {
		d.Representation = RepresentationFallback
	}
	if d.TemporalSemantics == TemporalZonedTime {
		if d.Engine == "postgres" || d.Engine == "postgresql" || d.Engine == "pg" {
			d.Representation, d.FallbackEncoding = RepresentationFallback, "postgres_timetz_text_v1"
			return
		}
		d.Representation = RepresentationUnsupported
		return
	}
	if d.TemporalPrecisionKnown && d.TemporalPrecision > 6 {
		switch d.Engine {
		case "mssql", "sqlserver", "ms-sql", "ms_sql":
			switch d.TemporalSemantics {
			case TemporalTime:
				d.FallbackEncoding = "mssql_time_text_v1"
			case TemporalLocalTimestamp:
				d.FallbackEncoding = "mssql_datetime2_text_v1"
			case TemporalInstant:
				d.FallbackEncoding = "mssql_datetimeoffset_text_v1"
			}
		case "oracle", "ora":
			if d.TemporalSemantics == TemporalInstant {
				d.FallbackEncoding = "oracle_timestamptz_text_v1"
			} else {
				d.FallbackEncoding = "oracle_timestamp_text_v1"
			}
		case "clickhouse", "ch":
			d.FallbackEncoding = "clickhouse_datetime64_text_v1"
		}
		if d.FallbackEncoding != "" {
			d.Representation = RepresentationFallback
		}
	}
	if exactDecimalType(d.Engine, d.SourceType) && (!d.PrecisionKnown || !d.ScaleKnown || d.Precision > 38) {
		d.Representation, d.FallbackEncoding = RepresentationFallback, "decimal_text_v1"
	}
	if d.Representation == "" {
		d.Representation = RepresentationNative
	}
}

// universalFallbackDescriptor is final planning tier. source_text_v1 accepts
// only exact textual driver values; unknown type names are never rejected only
// because no mapping switch mentioned them.
func universalFallbackDescriptor(d *SourceFieldDescriptor) {
	if d.Representation == RepresentationUnsupported || d.Representation == RepresentationFallback || d.FallbackEncoding != "" {
		return
	}
	d.Representation = RepresentationFallback
	d.FallbackEncoding = "source_text_v1"
}

func classifySourceRepresentation(d *SourceFieldDescriptor) {
	t := strings.ToUpper(unwrapClickHouseType(d.SourceType))
	switch d.Engine {
	case "clickhouse", "ch":
		if strings.HasPrefix(t, "INT128") || strings.HasPrefix(t, "INT256") || strings.HasPrefix(t, "UINT128") || strings.HasPrefix(t, "UINT256") {
			d.FallbackEncoding = "integer_text_v1"
		}
		if t == "UINT64" {
			d.Unsigned, d.BitWidth = true, 64
			signed := false
			d.Signed = &signed
			d.LogicalFamily = LogicalUnsignedInt
			d.FallbackEncoding = "unsigned_decimal20_v1"
		}
		if strings.HasPrefix(t, "FIXEDSTRING(") {
			d.FixedBinary, d.Length, d.LengthKnown = true, int64(temporalPrecision(t, -1)), true
		}
		if t == "UUID" {
			d.LogicalFamily, d.FallbackEncoding = LogicalUUID, "canonical_uuid_text_v1"
		}
		if t == "JSON" {
			d.FallbackEncoding = "json_utf8_text_v1"
		}
		if t == "STRING" {
			d.FallbackEncoding = "utf8_text_v1"
		}
	case "postgres", "postgresql", "pg":
		if t == "UUID" {
			d.LogicalFamily, d.FallbackEncoding = LogicalUUID, "canonical_uuid_text_v1"
		}
		if t == "JSON" || t == "JSONB" {
			d.FallbackEncoding = "json_utf8_text_v1"
		}
		if t == "TEXT" || strings.HasPrefix(t, "VARCHAR") || strings.HasPrefix(t, "CHAR") || t == "BPCHAR" || t == "NAME" || t == "CITEXT" {
			d.FallbackEncoding = "utf8_text_v1"
		}
	case "mssql", "sqlserver", "ms-sql", "ms_sql":
		if t == "UNIQUEIDENTIFIER" {
			d.LogicalFamily, d.FallbackEncoding = LogicalUUID, "canonical_uuid_text_v1"
		}
		if strings.HasPrefix(t, "BINARY(") {
			d.FixedBinary, d.Length, d.LengthKnown = true, int64(temporalPrecision(t, -1)), true
		}
		if strings.HasPrefix(t, "VARCHAR") || strings.HasPrefix(t, "NVARCHAR") || t == "TEXT" || t == "NTEXT" || t == "CHAR" || t == "NCHAR" {
			d.FallbackEncoding = "utf8_text_v1"
		}
		if t == "XML" {
			d.FallbackEncoding = "xml_utf8_text_v1"
		}
	case "oracle", "ora":
		if strings.HasPrefix(t, "RAW(") {
			d.FixedBinary, d.Length, d.LengthKnown = true, int64(temporalPrecision(t, -1)), true
		}
		if strings.Contains(t, "XML") {
			d.FallbackEncoding = "xml_utf8_text_v1"
		}
		if t == "ROWID" || t == "UROWID" {
			d.FallbackEncoding = "oracle_rowid_text_v1"
		}
		if strings.HasPrefix(t, "VARCHAR") || strings.HasPrefix(t, "NVARCHAR") || t == "CHAR" || t == "NCHAR" || t == "CLOB" || t == "NCLOB" || t == "LONG" {
			d.FallbackEncoding = "utf8_text_v1"
		}
	}
}

func classifyTemporalDescriptor(d *SourceFieldDescriptor) {
	t := strings.ToUpper(d.SourceType)
	if t == "" {
		return
	}
	set := func(s TemporalSemantics, precision int, known bool) {
		d.TemporalSemantics, d.TemporalPrecision, d.TemporalPrecisionKnown = s, precision, known
	}
	switch d.Engine {
	case "postgres", "postgresql", "pg":
		switch {
		case strings.HasPrefix(t, "TIMETZ") || strings.Contains(t, "TIME WITH TIME ZONE"):
			set(TemporalZonedTime, 6, true)
		case strings.HasPrefix(t, "TIMESTAMPTZ") || strings.Contains(t, "TIMESTAMP WITH TIME ZONE"):
			set(TemporalInstant, temporalPrecision(t, 6), true)
		case strings.HasPrefix(t, "TIMESTAMP"):
			set(TemporalLocalTimestamp, temporalPrecision(t, 6), true)
		case strings.HasPrefix(t, "TIME"):
			set(TemporalTime, temporalPrecision(t, 6), true)
		case strings.HasPrefix(t, "DATE"):
			set(TemporalDate, 0, true)
		}
	case "mssql", "sqlserver", "ms-sql", "ms_sql":
		switch {
		case strings.HasPrefix(t, "DATETIMEOFFSET"):
			set(TemporalInstant, temporalPrecision(t, 7), true)
		case strings.HasPrefix(t, "DATETIME2"):
			set(TemporalLocalTimestamp, temporalPrecision(t, 7), true)
		case strings.HasPrefix(t, "DATETIME"):
			set(TemporalLocalTimestamp, 3, true)
		case strings.HasPrefix(t, "SMALLDATETIME"):
			set(TemporalLocalTimestamp, 0, true)
		case strings.HasPrefix(t, "TIME"):
			set(TemporalTime, temporalPrecision(t, 7), true)
		case strings.HasPrefix(t, "DATE"):
			set(TemporalDate, 0, true)
		}
	case "oracle", "ora":
		switch {
		case strings.Contains(t, "WITH LOCAL TIME ZONE"):
			// Result depends on Oracle session time zone. Do not guess its meaning.
			set(TemporalZonedTime, temporalPrecision(t, 6), true)
		case strings.Contains(t, "WITH TIME ZONE"):
			set(TemporalInstant, temporalPrecision(t, 6), true)
		case strings.HasPrefix(t, "TIMESTAMP"):
			set(TemporalLocalTimestamp, temporalPrecision(t, 6), true)
		case t == "DATE":
			set(TemporalLocalTimestamp, 0, true)
		}
	case "clickhouse", "ch":
		base := unwrapClickHouseType(t)
		switch {
		case strings.HasPrefix(base, "DATETIME64"):
			set(TemporalInstant, temporalPrecision(base, -1), true)
			if d.SourceTimezone == "" {
				d.SourceTimezone = clickHouseTimezone(d.SourceType)
			}
		case strings.HasPrefix(base, "DATETIME"):
			set(TemporalInstant, 0, true)
			if d.SourceTimezone == "" {
				d.SourceTimezone = clickHouseTimezone(d.SourceType)
			}
		case strings.HasPrefix(base, "DATE32") || base == "DATE":
			set(TemporalDate, 0, true)
		}
	}
}

func temporalPrecision(t string, fallback int) int {
	if i := strings.IndexByte(t, '('); i >= 0 {
		rest := strings.TrimLeft(t[i+1:], " ")
		end := 0
		for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
			end++
		}
		if end > 0 {
			if p, err := strconv.Atoi(rest[:end]); err == nil {
				return p
			}
		}
	}
	return fallback
}

func unwrapClickHouseType(t string) string {
	for (strings.HasPrefix(t, "NULLABLE(") || strings.HasPrefix(t, "LOWCARDINALITY(")) && strings.HasSuffix(t, ")") {
		if i := strings.IndexByte(t, '('); i >= 0 {
			t = strings.TrimSpace(t[i+1 : len(t)-1])
		}
	}
	return t
}

func clickHouseTimezone(t string) string {
	if i := strings.Index(t, "'"); i >= 0 {
		if j := strings.Index(t[i+1:], "'"); j >= 0 {
			return t[i+1 : i+1+j]
		}
	}
	return ""
}
