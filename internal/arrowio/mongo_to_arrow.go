package arrowio

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

type MongoInferenceWarning struct {
	Field, Reason string
	SourceTypes   []string
}
type mongoFieldPlan struct {
	Name        string
	LogicalType typesystem.LogicalType
	ColumnPlan  ColumnPlan
	Mapping     typesystem.MappingResult
}
type MongoInferenceResult struct {
	Schema   *arrow.Schema
	Fields   []mongoFieldPlan
	Warnings []MongoInferenceWarning
}

func InferMongoSchema(docs []map[string]any) (*arrow.Schema, error) {
	return InferMongoSchemaWithFieldOrder(docs, nil)
}
func InferMongoSchemaWithFieldOrder(docs []map[string]any, fieldOrder []string) (*arrow.Schema, error) {
	result, err := InferMongoSchemaResult(docs, fieldOrder)
	if err != nil {
		return nil, err
	}
	return result.Schema, nil
}

func InferMongoSchemaResult(docs []map[string]any, fieldOrder []string) (*MongoInferenceResult, error) {
	if len(docs) == 0 {
		return nil, fmt.Errorf("cannot infer schema from empty documents")
	}
	values := map[string][]any{}
	present := map[string]int{}
	for _, doc := range docs {
		for key, value := range doc {
			values[key] = append(values[key], value)
			present[key]++
		}
	}
	keys := mongoOrderedKeys(values, fieldOrder)
	result := &MongoInferenceResult{Fields: make([]mongoFieldPlan, 0, len(keys))}
	fields := make([]arrow.Field, 0, len(keys))
	for _, key := range keys {
		logical, warning := inferMongoField(values[key], present[key] != len(docs))
		if warning != nil {
			warning.Field = key
			result.Warnings = append(result.Warnings, *warning)
		}
		plan, mapping, err := PlanForLogicalType(key, logical)
		if err != nil {
			return nil, err
		}
		result.Fields = append(result.Fields, mongoFieldPlan{Name: key, LogicalType: logical, ColumnPlan: plan, Mapping: mapping})
		fields = append(fields, arrow.Field{Name: key, Type: plan.DataType, Nullable: logical.Nullable})
	}
	result.Schema = arrow.NewSchema(fields, nil)
	return result, nil
}

func mongoOrderedKeys(values map[string][]any, order []string) []string {
	keys := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, k := range order {
		if _, ok := values[k]; ok && !seen[k] {
			keys = append(keys, k)
			seen[k] = true
		}
	}
	rest := make([]string, 0, len(values))
	for k := range values {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	return append(keys, rest...)
}

func inferMongoField(values []any, missing bool) (typesystem.LogicalType, *MongoInferenceWarning) {
	nullable := missing
	nonnil := make([]any, 0, len(values))
	var logical typesystem.LogicalType
	var warning *MongoInferenceWarning
	for _, v := range values {
		if v == nil {
			nullable = true
			continue
		}
		nonnil = append(nonnil, v)
		candidate, w := mongoCandidate(v)
		if w != "" {
			warning = &MongoInferenceWarning{Reason: w}
		}
		if len(nonnil) == 1 {
			logical = candidate
			continue
		}
		merged, ok := mergeMongoTypes(logical, candidate, nonnil)
		if !ok {
			logical = mongoUnknown("mixed")
			warning = &MongoInferenceWarning{Reason: "incompatible sampled field types use lossless fallback"}
		} else {
			logical = merged
		}
	}
	if len(nonnil) == 0 {
		logical = mongoUnknown("all_null")
		warning = &MongoInferenceWarning{Reason: "all-null field uses fallback"}
	}
	logical.Nullable = nullable
	return logical, warning
}

func mongoCandidate(v any) (typesystem.LogicalType, string) {
	switch x := v.(type) {
	case string:
		return mongoKind(typesystem.KindString), ""
	case int, int32, int64:
		return mongoKind(typesystem.KindInt64), ""
	case float32, float64:
		return mongoKind(typesystem.KindFloat64), ""
	case bool:
		return mongoKind(typesystem.KindBool), ""
	case time.Time, primitive.DateTime:
		return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}, ""
	case primitive.Timestamp:
		return mongoUnknown("bson_timestamp"), "BSON timestamp preserves T/I through fallback"
	case primitive.ObjectID:
		return mongoUnknown("objectid"), "ObjectID is not UUID"
	case primitive.Binary, []byte:
		return mongoKind(typesystem.KindBinary), ""
	case primitive.Decimal128:
		p, s, ok := mongoDecimalShape(x.String())
		if !ok {
			return mongoUnknown("decimal128"), "decimal has no stable exact shape"
		}
		return typesystem.Decimal(p, s), ""
	case bson.M, bson.D, map[string]any, primitive.Regex, primitive.JavaScript, primitive.CodeWithScope, primitive.MinKey, primitive.MaxKey, primitive.Undefined, primitive.Symbol:
		return mongoUnknown("document"), "document/special BSON uses Extended JSON fallback"
	}
	if mongoSlice(v) != nil {
		items := mongoSlice(v)
		t, w := inferMongoField(items, false)
		if w != nil {
			return typesystem.ArrayOf(t), "array element fallback"
		}
		return typesystem.ArrayOf(t), ""
	}
	return mongoUnknown("bson_special"), "special BSON uses lossless fallback"
}
func mongoKind(k typesystem.Kind) typesystem.LogicalType { return typesystem.LogicalType{Kind: k} }
func mongoUnknown(s string) typesystem.LogicalType {
	return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: s}
}

func mergeMongoTypes(a, b typesystem.LogicalType, values []any) (typesystem.LogicalType, bool) {
	if a.Equal(b) {
		return a, true
	}
	if a.Kind == typesystem.KindDecimal && b.Kind == typesystem.KindDecimal {
		scale := max(*a.Scale, *b.Scale)
		precision := max(*a.Precision+scale-*a.Scale, *b.Precision+scale-*b.Scale)
		return typesystem.Decimal(precision, scale), true
	}
	if a.Kind == typesystem.KindInt64 && b.Kind == typesystem.KindFloat64 || a.Kind == typesystem.KindFloat64 && b.Kind == typesystem.KindInt64 {
		for _, v := range values {
			switch n := v.(type) {
			case int:
				if math.Abs(float64(n)) > 1<<53 {
					return mongoUnknown("mixed_numeric"), false
				}
			case int32:
			case int64:
				if n > 1<<53 || n < -(1<<53) {
					return mongoUnknown("mixed_numeric"), false
				}
			}
		}
		return mongoKind(typesystem.KindFloat64), true
	}
	if a.Kind == typesystem.KindArray && b.Kind == typesystem.KindArray {
		e, ok := mergeMongoTypes(*a.Element, *b.Element, nil)
		if ok {
			return typesystem.ArrayOf(e), true
		}
	}
	return mongoUnknown("mixed"), false
}

func mongoDecimalShape(text string) (int32, int32, bool) {
	text = strings.TrimPrefix(strings.TrimSpace(text), "-")
	if strings.ContainsAny(text, "Ee") {
		return 0, 0, false
	}
	parts := strings.Split(text, ".")
	if len(parts) > 2 || text == "" {
		return 0, 0, false
	}
	digits := strings.TrimLeft(parts[0], "0")
	scale := 0
	if len(parts) == 2 {
		scale = len(parts[1])
		digits += parts[1]
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		digits = "0"
	}
	if len(digits) > math.MaxInt32 || scale > math.MaxInt32 {
		return 0, 0, false
	}
	return int32(len(digits)), int32(scale), true
}

func mongoSlice(v any) []any {
	switch x := v.(type) {
	case primitive.A:
		return []any(x)
	case []any:
		return x
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) || rv.Type().Elem().Kind() == reflect.Uint8 {
		return nil
	}
	r := make([]any, rv.Len())
	for i := range r {
		r[i] = rv.Index(i).Interface()
	}
	return r
}

func MongoDocsToRecord(alloc memory.Allocator, schema *arrow.Schema, docs []map[string]any) (arrow.RecordBatch, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	result, err := InferMongoSchemaResult(docs, fieldNames(schema))
	if err != nil {
		return nil, err
	}
	plans := map[string]mongoFieldPlan{}
	for _, p := range result.Fields {
		plans[p.Name] = p
	}
	builders := make([]array.Builder, schema.NumFields())
	defer func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}()
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		p, ok := plans[f.Name]
		if !ok {
			p = mongoFieldPlan{Name: f.Name, LogicalType: mongoLogicalFromArrow(f.Type)}
			p.ColumnPlan, _, err = PlanForLogicalType(f.Name, p.LogicalType)
			if err != nil {
				return nil, err
			}
		}
		builders[i] = p.ColumnPlan.Builder(alloc)
		builders[i].Reserve(len(docs))
		for _, doc := range docs {
			v, exists := doc[p.Name]
			if !exists || v == nil {
				builders[i].AppendNull()
				continue
			}
			normalized, err := mongoNormalize(v, p.LogicalType)
			if err != nil {
				return nil, fmt.Errorf("Mongo field %s: %w", p.Name, err)
			}
			if err := p.ColumnPlan.Append(builders[i], normalized); err != nil {
				return nil, fmt.Errorf("Mongo field %s: %w", p.Name, err)
			}
		}
	}
	arrays := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrays[i] = b.NewArray()
	}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()
	return array.NewRecordBatch(schema, arrays, int64(len(docs))), nil
}
func fieldNames(s *arrow.Schema) []string {
	r := make([]string, s.NumFields())
	for i := range r {
		r[i] = s.Field(i).Name
	}
	return r
}
func mongoLogicalFromArrow(dt arrow.DataType) typesystem.LogicalType {
	switch dt.ID() {
	case arrow.INT64:
		return mongoKind(typesystem.KindInt64)
	case arrow.FLOAT64:
		return mongoKind(typesystem.KindFloat64)
	case arrow.BOOL:
		return mongoKind(typesystem.KindBool)
	case arrow.BINARY:
		return mongoKind(typesystem.KindBinary)
	case arrow.TIMESTAMP:
		return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}
	default:
		return mongoKind(typesystem.KindString)
	}
}

func mongoNormalize(v any, t typesystem.LogicalType) (any, error) {
	if t.Kind == typesystem.KindUnknown || t.Kind == typesystem.KindJSON {
		return mongoLosslessString(v)
	}
	if t.Kind == typesystem.KindArray {
		items := mongoSlice(v)
		if items == nil {
			return nil, fmt.Errorf("expected array, got %T", v)
		}
		out := make([]any, len(items))
		for i, x := range items {
			if x == nil {
				continue
			}
			var err error
			out[i], err = mongoNormalize(x, *t.Element)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	switch x := v.(type) {
	case primitive.DateTime:
		return x.Time(), nil
	case primitive.Binary:
		return x.Data, nil
	case primitive.Decimal128:
		return x.String(), nil
	case primitive.ObjectID:
		return x.Hex(), nil
	case time.Time:
		return x, nil
	}
	return v, nil
}
func mongoLosslessString(v any) (string, error) {
	switch x := v.(type) {
	case primitive.ObjectID:
		return x.Hex(), nil
	case primitive.Decimal128:
		return x.String(), nil
	case string, bool, int, int32, int64, float32, float64, []byte, time.Time:
		return typesystem.ToLosslessString(x)
	}
	b, err := bson.MarshalExtJSON(bson.D{{Key: "value", Value: v}}, true, false)
	if err == nil {
		return string(b), nil
	}
	return "", fmt.Errorf("Mongo Extended JSON fallback: %w", err)
}

// mongoValueToString is retained for package compatibility but is now strict.
func mongoValueToString(v any) string {
	text, err := mongoLosslessString(v)
	if err != nil {
		return ""
	}
	return text
}
