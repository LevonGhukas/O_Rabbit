package arrowio

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	// MongoSchemaSampleSize is shared by extraction and auto-create inference.
	MongoSchemaSampleSize  = 10000
	mongoExtendedJSONCodec = "mongodb-extended-json"
	mongoTimestampCodec    = "mongodb-bson-timestamp-json"
	mongoDecimalCodec      = "mongodb-decimal128-text"
	mongoObjectIDCodec     = "mongodb-objectid-text"
)

// MongoSchemaViolationError prevents a late BSON value or field from being
// silently coerced after the sampled schema has been locked.
type MongoSchemaViolationError struct{ Field, Reason string }

func (e *MongoSchemaViolationError) Error() string {
	return fmt.Sprintf("mongodb schema violation for %q: %s", e.Field, e.Reason)
}

type mongoValueClass string

const (
	mongoNull      mongoValueClass = "null"
	mongoString    mongoValueClass = "string"
	mongoBool      mongoValueClass = "bool"
	mongoInt32     mongoValueClass = "int32"
	mongoInt64     mongoValueClass = "int64"
	mongoDouble    mongoValueClass = "double"
	mongoDate      mongoValueClass = "date"
	mongoTimestamp mongoValueClass = "timestamp"
	mongoDecimal   mongoValueClass = "decimal128"
	mongoBinary    mongoValueClass = "binary"
	mongoObjectID  mongoValueClass = "objectid"
	mongoArray     mongoValueClass = "array"
	mongoDocument  mongoValueClass = "document"
	mongoRare      mongoValueClass = "rare"
)

type mongoPlannedField struct {
	name          string
	typ           arrow.DataType
	policy        *TypePolicy
	classes       map[mongoValueClass]struct{}
	binarySubtype *byte
}
type MongoSchemaPlan struct {
	Schema *arrow.Schema
	fields []mongoPlannedField
	byName map[string]mongoPlannedField
}

func InferMongoSchemaPlan(docs []map[string]any, fieldOrder []string) (*MongoSchemaPlan, error) {
	if len(docs) == 0 {
		return nil, fmt.Errorf("cannot infer schema from empty documents")
	}
	seen := map[string]map[mongoValueClass]struct{}{}
	subtypes := map[string]map[byte]struct{}{}
	for _, doc := range docs {
		for name, value := range doc {
			if seen[name] == nil {
				seen[name] = map[mongoValueClass]struct{}{}
			}
			class := classifyMongoValue(value)
			seen[name][class] = struct{}{}
			if bin, ok := value.(primitive.Binary); ok {
				if subtypes[name] == nil {
					subtypes[name] = map[byte]struct{}{}
				}
				subtypes[name][bin.Subtype] = struct{}{}
			}
		}
	}
	names := orderedMongoFieldNames(seen, fieldOrder)
	fields := make([]mongoPlannedField, 0, len(names))
	arrowFields := make([]arrow.Field, 0, len(names))
	for _, name := range names {
		field := planMongoField(name, seen[name], subtypes[name])
		fields = append(fields, field)
		arrowFields = append(arrowFields, arrow.Field{Name: name, Type: field.typ, Nullable: true})
	}
	plan := &MongoSchemaPlan{Schema: arrow.NewSchema(arrowFields, nil), fields: fields, byName: map[string]mongoPlannedField{}}
	for _, field := range fields {
		plan.byName[field.name] = field
	}
	return plan, nil
}

func orderedMongoFieldNames(seen map[string]map[mongoValueClass]struct{}, order []string) []string {
	names, used := make([]string, 0, len(seen)), map[string]bool{}
	for _, n := range order {
		if _, ok := seen[n]; ok && !used[n] {
			names, used[n] = append(names, n), true
		}
	}
	var rest []string
	for n := range seen {
		if !used[n] {
			rest = append(rest, n)
		}
	}
	sort.Strings(rest)
	return append(names, rest...)
}

func planMongoField(name string, classes map[mongoValueClass]struct{}, subtypes map[byte]struct{}) mongoPlannedField {
	f := mongoPlannedField{name: name, classes: classes}
	props := map[string]string{"mongodb.observed_types": mongoClassesText(classes), "mongodb.missing_and_null_collapsed": "true"}
	policy := &TypePolicy{Version: MappingPolicyVersionV1, SourceEngine: "mongodb", SourceType: "bson", MappingKind: MappingNative, Metadata: SourceTypeMetadata{Properties: props}}
	f.typ, f.policy = arrow.BinaryTypes.String, policy
	nonNull := map[mongoValueClass]struct{}{}
	for c := range classes {
		if c != mongoNull {
			nonNull[c] = struct{}{}
		}
	}
	if len(nonNull) == 0 {
		return mongoFallbackField(f, mongoExtendedJSONCodec, "null-or-missing")
	}
	if hasMongoClass(nonNull, mongoRare) || hasMongoClass(nonNull, mongoDocument) || hasMongoClass(nonNull, mongoArray) || len(nonNull) > 1 && !(onlyMongoNumeric(nonNull)) {
		return mongoFallbackField(f, mongoExtendedJSONCodec, "heterogeneous")
	}
	if hasMongoClass(nonNull, mongoTimestamp) {
		return mongoFallbackField(f, mongoTimestampCodec, "timestamp")
	}
	if hasMongoClass(nonNull, mongoDecimal) {
		return mongoFallbackField(f, mongoDecimalCodec, "decimal128")
	}
	if hasMongoClass(nonNull, mongoObjectID) {
		return mongoFallbackField(f, mongoObjectIDCodec, "objectid")
	}
	if hasMongoClass(nonNull, mongoBinary) {
		if len(subtypes) != 1 {
			return mongoFallbackField(f, mongoExtendedJSONCodec, "binary-mixed-subtype")
		}
		for subtype := range subtypes {
			f.binarySubtype = &subtype
			props["mongodb.binary_subtype"] = fmt.Sprintf("%d", subtype)
		}
		f.typ = arrow.BinaryTypes.Binary
		props["mongodb.bson_type"] = "binary"
		return f
	}
	if hasMongoClass(nonNull, mongoDate) {
		f.typ = &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
		props["mongodb.bson_type"], props["mongodb.temporal_semantics"] = "date", "instant"
		return f
	}
	if hasMongoClass(nonNull, mongoDouble) {
		f.typ = arrow.PrimitiveTypes.Float64
		props["mongodb.numeric_widening"] = "float64"
		return f
	}
	if hasMongoClass(nonNull, mongoInt64) {
		f.typ = arrow.PrimitiveTypes.Int64
		props["mongodb.numeric_widening"] = "int64"
		return f
	}
	if hasMongoClass(nonNull, mongoInt32) {
		f.typ = arrow.PrimitiveTypes.Int32
		props["mongodb.numeric_widening"] = "int32"
		return f
	}
	if hasMongoClass(nonNull, mongoBool) {
		f.typ = arrow.FixedWidthTypes.Boolean
		return f
	}
	if hasMongoClass(nonNull, mongoString) {
		f.typ = arrow.BinaryTypes.String
		return f
	}
	return mongoFallbackField(f, mongoExtendedJSONCodec, "unsupported")
}

func mongoFallbackField(f mongoPlannedField, codec, semantic string) mongoPlannedField {
	f.typ = arrow.BinaryTypes.String
	f.policy.MappingKind = MappingFallback
	f.policy.Fallback = &FallbackCodec{Name: codec, Version: 1}
	f.policy.Metadata.Properties["mongodb.semantic_type"] = semantic
	return f
}
func hasMongoClass(s map[mongoValueClass]struct{}, target mongoValueClass) bool {
	_, ok := s[target]
	return ok
}
func onlyMongoNumeric(s map[mongoValueClass]struct{}) bool {
	for c := range s {
		if c != mongoInt32 && c != mongoInt64 && c != mongoDouble {
			return false
		}
	}
	return true
}
func mongoClassesText(s map[mongoValueClass]struct{}) string {
	v := make([]string, 0, len(s))
	for c := range s {
		v = append(v, string(c))
	}
	sort.Strings(v)
	return strings.Join(v, ",")
}

func (p *MongoSchemaPlan) Policies() []TypePolicy {
	out := make([]TypePolicy, len(p.fields))
	for i, f := range p.fields {
		out[i] = *f.policy
	}
	return out
}

// MongoMappingDiagnostics exposes the locked BSON policy without row values.
func MongoMappingDiagnostics(plan *MongoSchemaPlan) []MappingDiagnostic {
	if plan == nil {
		return nil
	}
	out := make([]MappingDiagnostic, 0, len(plan.fields))
	for _, field := range plan.fields {
		d := MappingDiagnostic{ColumnName: field.name, SourceEngine: field.policy.SourceEngine, SourceType: field.policy.Metadata.Properties["mongodb.observed_types"], MappingKind: field.policy.MappingKind, TargetArrowType: field.typ.String()}
		if field.policy.Fallback != nil {
			d.FallbackCodecName, d.FallbackCodecVersion = field.policy.Fallback.Name, field.policy.Fallback.Version
		}
		out = append(out, d)
	}
	return out
}

func MongoDocsToRecordWithPlan(alloc memory.Allocator, plan *MongoSchemaPlan, docs []map[string]any) (arrow.RecordBatch, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	builders := make([]array.Builder, len(plan.fields))
	for i, f := range plan.fields {
		builders[i] = mongoBuilderForField(alloc, f)
		builders[i].Reserve(len(docs))
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()
	for _, doc := range docs {
		for name := range doc {
			if _, ok := plan.byName[name]; !ok {
				return nil, &MongoSchemaViolationError{Field: name, Reason: "field appeared after schema lock"}
			}
		}
		for i, f := range plan.fields {
			value, present := doc[f.name]
			if !present || value == nil {
				builders[i].AppendNull()
				continue
			}
			if err := appendMongoField(builders[i], f, value); err != nil {
				return nil, err
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
	return array.NewRecordBatch(plan.Schema, arrays, int64(len(docs))), nil
}

func mongoBuilderForField(alloc memory.Allocator, f mongoPlannedField) array.Builder {
	switch f.typ.ID() {
	case arrow.INT32:
		return array.NewInt32Builder(alloc)
	case arrow.INT64:
		return array.NewInt64Builder(alloc)
	case arrow.FLOAT64:
		return array.NewFloat64Builder(alloc)
	case arrow.BOOL:
		return array.NewBooleanBuilder(alloc)
	case arrow.BINARY:
		return array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
	case arrow.TIMESTAMP:
		return array.NewTimestampBuilder(alloc, f.typ.(*arrow.TimestampType))
	default:
		return array.NewStringBuilder(alloc)
	}
}
func appendMongoField(b array.Builder, f mongoPlannedField, v any) error {
	class := classifyMongoValue(v)
	if _, ok := f.classes[class]; !ok {
		return &MongoSchemaViolationError{Field: f.name, Reason: "runtime BSON type " + string(class) + " was not observed during inference"}
	}
	if f.policy.MappingKind == MappingFallback {
		text, err := mongoFallbackText(v, f.policy.Fallback.Name)
		if err != nil {
			return &MongoSchemaViolationError{Field: f.name, Reason: err.Error()}
		}
		b.(*array.StringBuilder).Append(text)
		return nil
	}
	switch x := v.(type) {
	case int32:
		switch bb := b.(type) {
		case *array.Int32Builder:
			bb.Append(x)
		case *array.Int64Builder:
			bb.Append(int64(x))
		case *array.Float64Builder:
			bb.Append(float64(x))
		default:
			return &MongoSchemaViolationError{Field: f.name, Reason: "numeric plan mismatch"}
		}
	case int64:
		switch bb := b.(type) {
		case *array.Int64Builder:
			bb.Append(x)
		case *array.Float64Builder:
			bb.Append(float64(x))
		default:
			return &MongoSchemaViolationError{Field: f.name, Reason: "numeric plan mismatch"}
		}
	case int:
		switch bb := b.(type) {
		case *array.Int64Builder:
			bb.Append(int64(x))
		case *array.Float64Builder:
			bb.Append(float64(x))
		default:
			return &MongoSchemaViolationError{Field: f.name, Reason: "numeric plan mismatch"}
		}
	case float64:
		b.(*array.Float64Builder).Append(x)
	case bool:
		b.(*array.BooleanBuilder).Append(x)
	case string:
		b.(*array.StringBuilder).Append(x)
	case time.Time:
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(x.UTC().UnixMilli()))
	case primitive.DateTime:
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(x))
	case primitive.Binary:
		if f.binarySubtype == nil || x.Subtype != *f.binarySubtype {
			return &MongoSchemaViolationError{Field: f.name, Reason: "binary subtype changed after schema lock"}
		}
		b.(*array.BinaryBuilder).Append(x.Data)
	case []byte:
		b.(*array.BinaryBuilder).Append(x)
	default:
		return &MongoSchemaViolationError{Field: f.name, Reason: "unsupported native BSON representation"}
	}
	return nil
}

func mongoFallbackText(v any, codec string) (string, error) {
	if codec == mongoDecimalCodec {
		if d, ok := v.(primitive.Decimal128); ok {
			return d.String(), nil
		}
		return "", fmt.Errorf("expected Decimal128")
	}
	if codec == mongoObjectIDCodec {
		if id, ok := v.(primitive.ObjectID); ok {
			return id.Hex(), nil
		}
		return "", fmt.Errorf("expected ObjectId")
	}
	raw, err := bson.MarshalExtJSON(bson.D{{Key: "v", Value: v}}, true, false)
	if err != nil {
		return "", err
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", err
	}
	return string(envelope["v"]), nil
}
func classifyMongoValue(v any) mongoValueClass {
	switch v.(type) {
	case nil:
		return mongoNull
	case string:
		return mongoString
	case bool:
		return mongoBool
	case int32:
		return mongoInt32
	case int64, int:
		return mongoInt64
	case float64, float32:
		return mongoDouble
	case time.Time, primitive.DateTime:
		return mongoDate
	case primitive.Timestamp:
		return mongoTimestamp
	case primitive.Decimal128:
		return mongoDecimal
	case primitive.Binary, []byte:
		return mongoBinary
	case primitive.ObjectID:
		return mongoObjectID
	case primitive.A, []any:
		return mongoArray
	case bson.M, bson.D, map[string]any:
		return mongoDocument
	default:
		return mongoRare
	}
}
