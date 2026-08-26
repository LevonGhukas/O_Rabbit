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

const (
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
		out[i] = d
	}
	return out, nil
}

func arrowFieldFromDescriptor(d SourceFieldDescriptor) arrow.Field {
	md := arrow.NewMetadata([]string{
		"orabbit.source.engine", "orabbit.source.type", "orabbit.source.precision", "orabbit.source.scale", "orabbit.source.logical_family", "orabbit.source.temporal_semantics", "orabbit.source.temporal_precision", "orabbit.source.timezone",
	}, []string{d.Engine, d.SourceType, strconv.FormatInt(int64(d.Precision), 10), strconv.FormatInt(int64(d.Scale), 10), string(d.LogicalFamily), string(d.TemporalSemantics), strconv.Itoa(d.TemporalPrecision), d.SourceTimezone})
	return arrow.Field{Name: d.Name, Type: d.ArrowType, Nullable: d.Nullable || !d.NullableKnown, Metadata: md}
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
