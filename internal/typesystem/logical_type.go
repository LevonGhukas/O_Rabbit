package typesystem

import (
	"fmt"
	"strings"
)

// LogicalType represents ORabbit's canonical type semantics independently of
// any source database, Arrow, Parquet, or Iceberg representation.
type LogicalType struct {
	Kind     Kind
	Nullable bool

	Precision *int32
	Scale     *int32
	Timezone  string

	Element *LogicalType
	Key     *LogicalType
	Value   *LogicalType
	Fields  []LogicalField

	// SourceTypeName is informational metadata. It is intentionally excluded
	// from Equal because it does not alter logical semantics.
	SourceTypeName string
}

// Decimal constructs a logical decimal with explicit precision and scale.
func Decimal(precision, scale int32) LogicalType {
	return LogicalType{
		Kind:      KindDecimal,
		Precision: &precision,
		Scale:     &scale,
	}
}

// ArrayOf constructs an array whose elements have the supplied logical type.
func ArrayOf(element LogicalType) LogicalType {
	return LogicalType{Kind: KindArray, Element: &element}
}

// MapOf constructs a map with the supplied key and value logical types.
func MapOf(key, value LogicalType) LogicalType {
	return LogicalType{Kind: KindMap, Key: &key, Value: &value}
}

// Nullable returns a copy of t marked nullable.
func Nullable(t LogicalType) LogicalType {
	t.Nullable = true
	return t
}

// String renders a stable ORabbit-native representation suitable for logs,
// validation responses, and future APIs.
func (t LogicalType) String() string {
	var rendered string
	switch t.Kind {
	case KindDecimal:
		rendered = renderDecimal(t.Precision, t.Scale)
	case KindTimestampTZ:
		rendered = KindTimestampTZ.String()
		if t.Timezone != "" {
			rendered += "[" + t.Timezone + "]"
		}
	case KindArray:
		rendered = "array<" + nestedString(t.Element) + ">"
	case KindMap:
		rendered = "map<" + nestedString(t.Key) + "," + nestedString(t.Value) + ">"
	case KindStruct:
		fields := make([]string, len(t.Fields))
		for i, field := range t.Fields {
			fields[i] = field.Name + ":" + field.Type.String()
		}
		rendered = "struct<" + strings.Join(fields, ",") + ">"
	default:
		rendered = t.Kind.String()
	}
	if t.Nullable {
		return "nullable<" + rendered + ">"
	}
	return rendered
}

func renderDecimal(precision, scale *int32) string {
	if precision == nil && scale == nil {
		return "decimal"
	}
	precisionText := "?"
	if precision != nil {
		precisionText = fmt.Sprintf("%d", *precision)
	}
	scaleText := "?"
	if scale != nil {
		scaleText = fmt.Sprintf("%d", *scale)
	}
	return "decimal(" + precisionText + "," + scaleText + ")"
}

func nestedString(t *LogicalType) string {
	if t == nil {
		return KindUnknown.String()
	}
	return t.String()
}

// Equal compares logical semantics. SourceTypeName is deliberately ignored.
func (t LogicalType) Equal(other LogicalType) bool {
	if t.Kind != other.Kind ||
		t.Nullable != other.Nullable ||
		t.Timezone != other.Timezone ||
		!equalInt32(t.Precision, other.Precision) ||
		!equalInt32(t.Scale, other.Scale) ||
		!equalTypePointer(t.Element, other.Element) ||
		!equalTypePointer(t.Key, other.Key) ||
		!equalTypePointer(t.Value, other.Value) ||
		len(t.Fields) != len(other.Fields) {
		return false
	}
	for i := range t.Fields {
		if t.Fields[i].Name != other.Fields[i].Name || !t.Fields[i].Type.Equal(other.Fields[i].Type) {
			return false
		}
	}
	return true
}

func equalInt32(left, right *int32) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func equalTypePointer(left, right *LogicalType) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Equal(*right)
}

// Validate checks only structural correctness of the logical model. It does
// not impose physical-format limitations such as Arrow's decimal precision.
func (t LogicalType) Validate() error {
	switch t.Kind {
	case KindDecimal:
		if t.Precision == nil || t.Scale == nil {
			return fmt.Errorf("decimal requires precision and scale")
		}
		if *t.Precision <= 0 {
			return fmt.Errorf("decimal precision must be > 0")
		}
		if *t.Scale < 0 {
			return fmt.Errorf("decimal scale must be >= 0")
		}
		if *t.Scale > *t.Precision {
			return fmt.Errorf("decimal scale must be <= precision")
		}
	case KindArray:
		if t.Element == nil {
			return fmt.Errorf("array requires element type")
		}
		if err := t.Element.Validate(); err != nil {
			return fmt.Errorf("array element: %w", err)
		}
	case KindMap:
		if t.Key == nil {
			return fmt.Errorf("map requires key type")
		}
		if t.Value == nil {
			return fmt.Errorf("map requires value type")
		}
		if err := t.Key.Validate(); err != nil {
			return fmt.Errorf("map key: %w", err)
		}
		if err := t.Value.Validate(); err != nil {
			return fmt.Errorf("map value: %w", err)
		}
	case KindStruct:
		seen := make(map[string]struct{}, len(t.Fields))
		for _, field := range t.Fields {
			if strings.TrimSpace(field.Name) == "" {
				return fmt.Errorf("struct field name is required")
			}
			if _, exists := seen[field.Name]; exists {
				return fmt.Errorf("duplicate struct field %q", field.Name)
			}
			seen[field.Name] = struct{}{}
			if err := field.Type.Validate(); err != nil {
				return fmt.Errorf("struct field %q: %w", field.Name, err)
			}
		}
	}
	return nil
}
