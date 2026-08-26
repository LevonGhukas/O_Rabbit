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
)

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
			d.SourceType = strings.ToUpper(strings.TrimSpace(ct.DatabaseTypeName()))
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
		upperType := strings.ToUpper(d.SourceType)
		if strings.Contains(upperType, "TIME") || strings.Contains(upperType, "DATE") {
			switch {
			case strings.Contains(upperType, "TIME") && (strings.Contains(upperType, "ZONE") || strings.Contains(upperType, "OFFSET")):
				d.TemporalSemantics = TemporalInstant
			case strings.Contains(upperType, "TIMESTAMP") || strings.Contains(upperType, "DATETIME"):
				d.TemporalSemantics = TemporalLocalTimestamp
			case strings.HasPrefix(upperType, "TIME"):
				d.TemporalSemantics = TemporalTime
			case strings.HasPrefix(upperType, "DATE"):
				d.TemporalSemantics = TemporalDate
			}
			if m := precScaleRe.FindStringSubmatch(upperType); m != nil && m[1] != "" {
				if p, err := strconv.Atoi(m[1]); err == nil {
					d.TemporalPrecision, d.TemporalPrecisionKnown = p, true
				}
			}
		}
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
		"orabbit.source.engine", "orabbit.source.type", "orabbit.source.precision", "orabbit.source.scale", "orabbit.source.logical_family", "orabbit.source.temporal_semantics",
	}, []string{d.Engine, d.SourceType, strconv.FormatInt(int64(d.Precision), 10), strconv.FormatInt(int64(d.Scale), 10), string(d.LogicalFamily), string(d.TemporalSemantics)})
	return arrow.Field{Name: d.Name, Type: d.ArrowType, Nullable: d.Nullable || !d.NullableKnown, Metadata: md}
}
