package arrowio

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

const (
	postgresUUIDTextCodec      = "postgres-uuid-text"
	postgresJSONTextCodec      = "postgres-json-text"
	postgresJSONBTextCodec     = "postgres-jsonb-text"
	postgresIntervalTextCodec  = "postgres-interval-text"
	postgresNetworkTextCodec   = "postgres-network-text"
	postgresMACTextCodec       = "postgres-mac-text"
	postgresTimetzTextCodec    = "postgres-timetz-text"
	postgresArrayTextCodec     = "postgres-array-text"
	postgresBitTextCodec       = "postgres-bit-text"
	postgresEnumTextCodec      = "postgres-enum-text"
	postgresDomainTextCodec    = "postgres-domain-text"
	postgresCompositeTextCodec = "postgres-composite-text"
)

var (
	postgresUUIDTextRe = regexp.MustCompile(`^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$`)
	postgresMACTextRe  = regexp.MustCompile(`^[[:xdigit:]]{2}([:-][[:xdigit:]]{2}){5}$`)
	postgresMAC8TextRe = regexp.MustCompile(`^[[:xdigit:]]{2}([:-][[:xdigit:]]{2}){7}$`)
)

func planPostgresColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])

	// PostgreSQL exposes arrays both as T[] and _T. A declaration with more
	// than one [] explicitly needs dimensional semantics Arrow List cannot
	// carry, so it is kept as PostgreSQL text rather than flattened.
	if elemType, dimensions, ok := postgresArrayType(clean); ok {
		if dimensions != 1 || !postgresNativeArrayElement(elemType) {
			return planPostgresArrayText(name, clean, elemType, dimensions)
		}
		return planPostgresArrayList(name, elemType, planPostgresColumn("item", elemType, precision, scale, hasDecimal))
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

	// 5. Booleans & Bits. BIT is a bit string, not an integer: use text for
	// every width so leading zeroes and values wider than 64 bits survive.
	case "BOOL", "BOOLEAN":
		return planBool(name)
	case "BIT", "VARBIT":
		return planPostgresBitText(name, clean, base)

	// 6. Dates & Timestamps
	case "DATE":
		return planDate32(name)
	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		return planTimestampUs(name, "")
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return planTimestampUs(name, "UTC")
	case "TIME", "TIME WITHOUT TIME ZONE":
		return planTime64(name)
	case "TIMETZ", "TIME WITH TIME ZONE":
		return planPostgresText(name, clean, MappingFallback, postgresTimetzTextCodec, "time-with-offset", func(text string) bool {
			return strings.Contains(text, "+") || strings.LastIndex(text, "-") > 1
		})

	// 7. Binary & Strings
	case "BYTEA":
		return planBinary(name)
	case "UUID":
		return planPostgresText(name, clean, MappingFallback, postgresUUIDTextCodec, "uuid", func(text string) bool {
			return postgresUUIDTextRe.MatchString(text)
		})
	case "JSON":
		return planPostgresText(name, clean, MappingFallback, postgresJSONTextCodec, "json", func(text string) bool { return json.Valid([]byte(text)) })
	case "JSONB":
		return planPostgresText(name, clean, MappingFallback, postgresJSONBTextCodec, "jsonb", func(text string) bool { return json.Valid([]byte(text)) })
	case "XML":
		return planPostgresText(name, clean, MappingNative, "", "xml", func(string) bool { return true })
	case "INTERVAL":
		return planPostgresText(name, clean, MappingFallback, postgresIntervalTextCodec, "interval", func(text string) bool { return text != "" })
	case "INET":
		return planPostgresText(name, clean, MappingFallback, postgresNetworkTextCodec, "inet", validPostgresInet)
	case "CIDR":
		return planPostgresText(name, clean, MappingFallback, postgresNetworkTextCodec, "cidr", validPostgresCIDR)
	case "MACADDR":
		return planPostgresText(name, clean, MappingFallback, postgresMACTextCodec, "macaddr", postgresMACTextRe.MatchString)
	case "MACADDR8":
		return planPostgresText(name, clean, MappingFallback, postgresMACTextCodec, "macaddr8", postgresMAC8TextRe.MatchString)
	case "TEXT", "VARCHAR", "CHAR", "BPCHAR", "NAME", "CITEXT":
		return planString(name)

	default:
		return planGenericSQLColumn(name, base, precision, scale, hasDecimal)
	}
}

// PlanForPostgresColumnWithMetadata applies connector-owned catalog metadata
// without allowing Arrow conversion code to query PostgreSQL itself.
func PlanForPostgresColumnWithMetadata(name, dbType string, precision, scale int64, hasDecimal bool, metadata *connectors.PostgresTypeMetadata) ColumnPlan {
	if metadata == nil || metadata.Kind == "" {
		return PlanForSQLColumn("postgres", name, dbType, precision, scale, hasDecimal)
	}
	var plan ColumnPlan
	switch metadata.Kind {
	case "enum":
		labels := make(map[string]struct{}, len(metadata.EnumLabels))
		for _, label := range metadata.EnumLabels {
			labels[label] = struct{}{}
		}
		plan = planPostgresText(name, dbType, MappingFallback, postgresEnumTextCodec, "enum", func(text string) bool {
			if len(labels) == 0 {
				return true
			}
			_, ok := labels[text]
			return ok
		})
	case "domain":
		if metadata.BaseType == "" {
			plan = planPostgresText(name, dbType, MappingFallback, postgresDomainTextCodec, "domain", func(string) bool { return true })
		} else {
			plan = PlanForSQLColumn("postgres", name, metadata.BaseType, precision, scale, hasDecimal)
		}
	case "composite":
		plan = planPostgresText(name, dbType, MappingFallback, postgresCompositeTextCodec, "composite", func(string) bool { return true })
	default:
		return PlanForSQLColumn("postgres", name, dbType, precision, scale, hasDecimal)
	}
	plan = withSQLTypePolicy(plan, "postgres", dbType, precision, scale, hasDecimal)
	if metadata.Kind == "domain" && plan.Policy.MappingKind == MappingFallback && plan.Policy.Fallback != nil && plan.Policy.Fallback.Name == genericTextFallbackCodec {
		plan.Policy.Fallback = &FallbackCodec{Name: postgresDomainTextCodec, Version: 1}
	}
	properties := plan.Policy.Metadata.Properties
	if properties == nil {
		properties = map[string]string{}
		plan.Policy.Metadata.Properties = properties
	}
	properties["postgres.type_kind"] = metadata.Kind
	properties["postgres.type_name"] = metadata.TypeName
	properties["postgres.schema"] = metadata.Schema
	if metadata.Kind == "enum" {
		properties["postgres.enum_labels"] = postgresPolicyJSON(metadata.EnumLabels)
	}
	if metadata.Kind == "domain" {
		properties["postgres.domain_base_type"] = metadata.BaseType
		properties["postgres.domain_not_null"] = strconv.FormatBool(metadata.DomainNotNull)
		if len(metadata.DomainChain) > 0 {
			properties["postgres.domain_chain"] = postgresPolicyJSON(metadata.DomainChain)
		}
	}
	if metadata.Kind == "composite" {
		properties["postgres.composite_fields"] = postgresPolicyJSON(metadata.CompositeFields)
	}
	return plan
}

func postgresPolicyJSON(values []string) string {
	encoded, err := json.Marshal(values)
	if err != nil {
		return "[]"
	}
	return string(encoded)
}

func postgresArrayType(clean string) (element string, dimensions int, ok bool) {
	if strings.HasPrefix(clean, "_") && !strings.Contains(clean, "[") {
		return strings.TrimPrefix(clean, "_"), 1, true
	}
	for strings.HasSuffix(clean, "[]") {
		dimensions++
		clean = strings.TrimSuffix(clean, "[]")
	}
	return clean, dimensions, dimensions > 0
}

func postgresNativeArrayElement(element string) bool {
	switch element {
	case "INT2", "SMALLINT", "SMALLSERIAL", "INT4", "INTEGER", "INT", "SERIAL", "INT8", "BIGINT", "BIGSERIAL", "FLOAT4", "REAL", "FLOAT8", "DOUBLE PRECISION", "FLOAT", "BOOL", "BOOLEAN", "TEXT", "VARCHAR", "CHAR", "BPCHAR", "NAME", "CITEXT":
		return true
	default:
		return false
	}
}

func planPostgresArrayText(name, sourceType, elementType string, dimensions int) ColumnPlan {
	plan := planPostgresText(name, sourceType, MappingFallback, postgresArrayTextCodec, "array", func(string) bool { return true })
	plan.Policy.Metadata.Properties["postgres.array"] = "true"
	plan.Policy.Metadata.Properties["postgres.array_element_type"] = elementType
	if dimensions > 0 {
		plan.Policy.Metadata.Properties["postgres.array_dimensions"] = strconv.Itoa(dimensions)
	}
	return plan
}

func planPostgresArrayList(name, elementType string, itemPlan ColumnPlan) ColumnPlan {
	plan := planListWithItems(name, itemPlan, postgresOneDimensionalArrayItems)
	plan.Policy = &TypePolicy{Version: MappingPolicyVersionV1, MappingKind: MappingNative, Metadata: SourceTypeMetadata{Properties: map[string]string{
		"postgres.array":              "true",
		"postgres.array_element_type": elementType,
		"postgres.array_dimensions":   "1",
	}}}
	return plan
}

func planPostgresBitText(name, clean, base string) ColumnPlan {
	width, known := postgresBitWidth(base)
	plan := planPostgresText(name, clean, MappingFallback, postgresBitTextCodec, "bit-string", func(text string) bool {
		if text == "" {
			return false
		}
		for _, c := range text {
			if c != '0' && c != '1' {
				return false
			}
		}
		return !known || clean == "VARBIT" || int64(len(text)) == width
	})
	plan.Policy.Metadata.Properties["postgres.bit_type"] = strings.ToLower(clean)
	if known {
		plan.Policy.Metadata.BitWidthKnown = true
		plan.Policy.Metadata.BitWidth = width
	}
	return plan
}

func postgresBitWidth(base string) (int64, bool) {
	m := regexp.MustCompile(`^(?:BIT|VARBIT)\s*\(\s*([0-9]+)\s*\)$`).FindStringSubmatch(base)
	if m == nil {
		return 0, false
	}
	width, err := strconv.ParseInt(m[1], 10, 64)
	return width, err == nil
}

// postgresOneDimensionalArrayItems parses only the PostgreSQL array text that
// can be represented exactly by one Arrow List. Dimension prefixes and nested
// braces deliberately fail here; their declarations are planned as text when
// known, and an unexpected runtime value is rejected rather than flattened.
func postgresOneDimensionalArrayItems(v any) ([]any, bool) {
	v = dereferenceValue(v)
	switch x := v.(type) {
	case string:
		return parsePostgresOneDimensionalArray(x)
	case []byte:
		return parsePostgresOneDimensionalArray(string(x))
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	items := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		item := dereferenceValue(rv.Index(i).Interface())
		if item != nil {
			itemValue := reflect.ValueOf(item)
			if itemValue.Kind() == reflect.Slice || itemValue.Kind() == reflect.Array {
				return nil, false
			}
		}
		items[i] = item
	}
	return items, true
}

func parsePostgresOneDimensionalArray(raw string) ([]any, bool) {
	if strings.HasPrefix(raw, "[") { // lower-bound dimension prefix
		return nil, false
	}
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return nil, false
	}
	if raw == "{}" {
		return []any{}, true
	}
	items := make([]any, 0)
	for i := 1; i < len(raw)-1; {
		quoted := raw[i] == '"'
		var value strings.Builder
		if quoted {
			i++
			closed := false
			for i < len(raw)-1 {
				if raw[i] == '\\' {
					i++
					if i >= len(raw)-1 {
						return nil, false
					}
					value.WriteByte(raw[i])
					i++
					continue
				}
				if raw[i] == '"' {
					i++
					closed = true
					break
				}
				value.WriteByte(raw[i])
				i++
			}
			if !closed {
				return nil, false
			}
		} else {
			for i < len(raw)-1 && raw[i] != ',' {
				if raw[i] == '{' || raw[i] == '}' || raw[i] == '"' {
					return nil, false
				}
				if raw[i] == '\\' {
					i++
					if i >= len(raw)-1 {
						return nil, false
					}
				}
				value.WriteByte(raw[i])
				i++
			}
		}
		if i < len(raw)-1 && raw[i] != ',' {
			return nil, false
		}
		if !quoted && value.String() == "NULL" {
			items = append(items, nil)
		} else {
			items = append(items, value.String())
		}
		if i == len(raw)-1 {
			break
		}
		i++
		if i == len(raw)-1 {
			return nil, false
		}
	}
	return items, true
}

func planPostgresText(name, sourceType string, kind MappingKind, codec, semanticType string, valid func(string) bool) ColumnPlan {
	plan := ColumnPlan{
		Name:     name,
		DataType: arrow.BinaryTypes.String,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.StringBuilder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			text, ok := postgresTextValue(v)
			if !ok || !valid(text) {
				return &ScalarConversionError{Target: fmt.Sprintf("PostgreSQL %s text", sourceType), InputType: fmt.Sprintf("%T", v), Reason: "invalid PostgreSQL textual representation"}
			}
			bb.Append(text)
			return nil
		},
	}
	policy := &TypePolicy{
		Version:     MappingPolicyVersionV1,
		MappingKind: kind,
		Metadata: SourceTypeMetadata{Properties: map[string]string{
			"postgres.semantic_type": semanticType,
		}},
	}
	if kind == MappingFallback {
		policy.Fallback = &FallbackCodec{Name: codec, Version: 1}
	}
	plan.Policy = policy
	return plan
}

func postgresTextValue(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	default:
		return "", false
	}
}

func validPostgresInet(text string) bool {
	if _, err := netip.ParseAddr(text); err == nil {
		return true
	}
	_, err := netip.ParsePrefix(text)
	return err == nil
}

func validPostgresCIDR(text string) bool {
	_, err := netip.ParsePrefix(text)
	return err == nil
}
