package arrowio

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FallbackCodec serializes one known source family into a versioned, reversible
// Arrow string representation. It never receives implicit fmt.Sprint output.
type FallbackCodec interface {
	Encoding() string
	ArrowType() arrow.DataType
	EncodeExact(value any) (string, error)
}

type textFallbackCodec struct{ descriptor SourceFieldDescriptor }

func (c textFallbackCodec) Encoding() string        { return c.descriptor.FallbackEncoding }
func (textFallbackCodec) ArrowType() arrow.DataType { return arrow.BinaryTypes.String }

func (c textFallbackCodec) EncodeExact(value any) (string, error) {
	v := dereferenceValue(value)
	var s string
	switch x := v.(type) {
	case string:
		s = x
	case []byte:
		if !utf8Valid(x) {
			return "", fmt.Errorf("fallback text is not UTF-8")
		}
		s = string(x)
	default:
		return "", fmt.Errorf("driver value %T lacks original textual precision", value)
	}
	if err := validateFallbackText(c.descriptor, s); err != nil {
		return "", err
	}
	return s, nil
}

func planFallback(name string, codec FallbackCodec) ColumnPlan {
	return ColumnPlan{Name: name, DataType: codec.ArrowType(), Builder: func(mem memory.Allocator) array.Builder {
		return array.NewStringBuilder(mem)
	}, Append: func(b array.Builder, value any) error {
		bb := b.(*array.StringBuilder)
		if dereferenceValue(value) == nil {
			bb.AppendNull()
			return nil
		}
		s, err := codec.EncodeExact(value)
		if err != nil {
			return conversionFailure(name, codec.Encoding(), value, err.Error())
		}
		bb.Append(s)
		return nil
	}}
}

func fallbackPlanForDescriptor(d SourceFieldDescriptor) (ColumnPlan, error) {
	switch d.FallbackEncoding {
	case "canonical_uuid_text_v1":
		return planUUID(d.Name), nil
	case "json_utf8_text_v1":
		return planJSONText(d.Name), nil
	case "utf8_text_v1", "xml_utf8_text_v1", "oracle_rowid_text_v1", "source_text_v1":
		return planFallback(d.Name, textFallbackCodec{descriptor: d}), nil
	case "hex_v1":
		return planFallback(d.Name, hexFallbackCodec{descriptor: d}), nil
	case "mssql_time_text_v1", "mssql_datetime2_text_v1", "mssql_datetimeoffset_text_v1", "oracle_timestamp_text_v1", "oracle_timestamptz_text_v1", "clickhouse_datetime64_text_v1", "postgres_timetz_text_v1", "decimal_text_v1", "integer_text_v1":
		return planFallback(d.Name, textFallbackCodec{descriptor: d}), nil
	default:
		return ColumnPlan{}, fmt.Errorf("no fallback codec for %q", d.FallbackEncoding)
	}
}

// hexFallbackCodec is universal binary fallback. It accepts byte containers
// only, so arbitrary binary is never mistaken for UTF-8 source text.
type hexFallbackCodec struct{ descriptor SourceFieldDescriptor }

func (c hexFallbackCodec) Encoding() string        { return c.descriptor.FallbackEncoding }
func (hexFallbackCodec) ArrowType() arrow.DataType { return arrow.BinaryTypes.String }
func (hexFallbackCodec) EncodeExact(value any) (string, error) {
	v := dereferenceValue(value)
	b, ok := v.([]byte)
	if !ok {
		return "", fmt.Errorf("driver value %T is not exact binary", value)
	}
	return hex.EncodeToString(b), nil
}

func validateFallbackText(d SourceFieldDescriptor, raw string) error {
	// Empty text is a valid source value. NULL is handled before codec use.
	precision := d.TemporalPrecision
	frac := ""
	if precision > 0 {
		frac = `\.\d{` + fmt.Sprint(precision) + `}`
	}
	var pattern string
	switch d.FallbackEncoding {
	case "mssql_time_text_v1":
		pattern = `^\d{2}:\d{2}:\d{2}` + frac + `$`
	case "mssql_datetime2_text_v1", "oracle_timestamp_text_v1", "clickhouse_datetime64_text_v1":
		pattern = `^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}` + frac + `$`
	case "mssql_datetimeoffset_text_v1", "oracle_timestamptz_text_v1":
		pattern = `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}` + frac + `(Z|[+-]\d{2}:\d{2})$`
	case "postgres_timetz_text_v1":
		pattern = `^\d{2}:\d{2}:\d{2}(\.\d{1,6})?[+-]\d{2}:\d{2}$`
	case "decimal_text_v1":
		pattern = `^[+-]?\d+(\.\d+)?$`
	case "integer_text_v1":
		pattern = `^[+-]?\d+$`
	}
	if pattern != "" && !regexp.MustCompile(pattern).MatchString(strings.TrimSpace(raw)) {
		return fmt.Errorf("text does not match %s exact representation", d.FallbackEncoding)
	}
	if d.FallbackEncoding == "decimal_text_v1" && d.PrecisionKnown && d.ScaleKnown {
		if !decimalTextFitsDescriptor(raw, d) {
			return fmt.Errorf("decimal text exceeds declared precision or scale")
		}
	}
	return nil
}

func decimalTextFitsDescriptor(raw string, d SourceFieldDescriptor) bool {
	s := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(raw), "+"), "-")
	parts := strings.Split(s, ".")
	if len(parts) > 2 || len(parts) == 0 {
		return false
	}
	frac := ""
	if len(parts) == 2 {
		frac = parts[1]
	}
	digits := strings.TrimLeft(parts[0]+frac, "0")
	if digits == "" {
		digits = "0"
	}
	return d.PrecisionKnown && d.ScaleKnown && len(digits) <= int(d.Precision) && len(frac) <= int(d.Scale)
}

func utf8Valid(b []byte) bool { return utf8.Valid(b) }
