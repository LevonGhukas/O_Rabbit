package arrowio

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type mongoFieldPlan struct {
	Name    string
	Type    arrow.DataType
	Builder func(mem memory.Allocator) array.Builder
	Append  func(b array.Builder, v any)
}

func InferMongoSchema(docs []map[string]any) (*arrow.Schema, error) {
	plan, err := InferMongoSchemaPlan(docs, nil)
	if err != nil {
		return nil, err
	}
	return plan.Schema, nil
}

// InferMongoSchemaWithFieldOrder builds a document schema using fieldOrder
// when a source parser has already supplied an authoritative order. Fields not
// present in fieldOrder retain the deterministic alphabetical fallback used by
// generic document sources.
func InferMongoSchemaWithFieldOrder(docs []map[string]any, fieldOrder []string) (*arrow.Schema, error) {
	plan, err := InferMongoSchemaPlan(docs, fieldOrder)
	if err != nil {
		return nil, err
	}
	return plan.Schema, nil
}

func buildPlansFromSchema(schema *arrow.Schema) []mongoFieldPlan {
	plans := make([]mongoFieldPlan, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		dt := f.Type
		builderFn, appendFn := mongoBuilderFromArrowType(dt)
		plans[i] = mongoFieldPlan{
			Name:    f.Name,
			Type:    dt,
			Builder: builderFn,
			Append:  appendFn,
		}
	}
	return plans
}

func MongoDocsToRecord(alloc memory.Allocator, schema *arrow.Schema, docs []map[string]any) (arrow.RecordBatch, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	// Compatibility entry point. New extraction code retains MongoSchemaPlan so
	// late values are checked against the locked sampled policy.
	order := make([]string, schema.NumFields())
	for i := range order {
		order[i] = schema.Field(i).Name
	}
	plan, err := InferMongoSchemaPlan(docs, order)
	if err != nil {
		return nil, err
	}
	if !mongoArrowSchemaEqual(schema, plan.Schema) {
		return nil, &MongoSchemaViolationError{Reason: "provided Arrow schema does not match deterministic BSON schema"}
	}
	return MongoDocsToRecordWithPlan(alloc, plan, docs)

	plans := buildPlansFromSchema(schema)
	builders := make([]array.Builder, len(plans))
	for i, p := range plans {
		b := p.Builder(alloc)
		b.Reserve(len(docs))
		builders[i] = b
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	for _, doc := range docs {
		for i, p := range plans {
			val := doc[p.Name]
			if val == nil {
				builders[i].AppendNull()
			} else {
				p.Append(builders[i], val)
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

func mongoArrowSchemaEqual(left, right *arrow.Schema) bool {
	if left.NumFields() != right.NumFields() {
		return false
	}
	for i := 0; i < left.NumFields(); i++ {
		if left.Field(i).Name != right.Field(i).Name || !arrow.TypeEqual(left.Field(i).Type, right.Field(i).Type) {
			return false
		}
	}
	return true
}

func mongoValueToArrowType(v any) (arrow.DataType, func(memory.Allocator) array.Builder, func(array.Builder, any)) {
	switch v.(type) {
	case string:
		return arrow.BinaryTypes.String,
			func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
			func(b array.Builder, val any) {
				if s, ok := val.(string); ok {
					b.(*array.StringBuilder).Append(s)
				} else {
					b.(*array.StringBuilder).Append(fmt.Sprint(val))
				}
			}
	case int32, int64, int:
		return arrow.PrimitiveTypes.Int64,
			func(mem memory.Allocator) array.Builder { return array.NewInt64Builder(mem) },
			func(b array.Builder, val any) {
				b.(*array.Int64Builder).Append(mongoToInt64(val))
			}
	case float64:
		return arrow.PrimitiveTypes.Float64,
			func(mem memory.Allocator) array.Builder { return array.NewFloat64Builder(mem) },
			func(b array.Builder, val any) {
				if f, ok := val.(float64); ok {
					b.(*array.Float64Builder).Append(f)
				} else {
					b.(*array.Float64Builder).AppendNull()
				}
			}
	case bool:
		return arrow.FixedWidthTypes.Boolean,
			func(mem memory.Allocator) array.Builder { return array.NewBooleanBuilder(mem) },
			func(b array.Builder, val any) {
				if bl, ok := val.(bool); ok {
					b.(*array.BooleanBuilder).Append(bl)
				} else {
					b.(*array.BooleanBuilder).AppendNull()
				}
			}
	case time.Time:
		tsType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
		return tsType,
			func(mem memory.Allocator) array.Builder { return array.NewTimestampBuilder(mem, tsType) },
			func(b array.Builder, val any) {
				if t, ok := val.(time.Time); ok {
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UTC().UnixMilli()))
				} else {
					b.(*array.TimestampBuilder).AppendNull()
				}
			}
	case primitive.ObjectID:
		return arrow.BinaryTypes.String,
			func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
			func(b array.Builder, val any) {
				if oid, ok := val.(primitive.ObjectID); ok {
					b.(*array.StringBuilder).Append(oid.Hex())
				} else {
					b.(*array.StringBuilder).Append(fmt.Sprint(val))
				}
			}
	case primitive.DateTime:
		tsType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
		return tsType,
			func(mem memory.Allocator) array.Builder { return array.NewTimestampBuilder(mem, tsType) },
			func(b array.Builder, val any) {
				if dt, ok := val.(primitive.DateTime); ok {
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(dt))
				} else {
					b.(*array.TimestampBuilder).AppendNull()
				}
			}
	case primitive.Timestamp:
		tsType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
		return tsType,
			func(mem memory.Allocator) array.Builder { return array.NewTimestampBuilder(mem, tsType) },
			func(b array.Builder, val any) {
				if ts, ok := val.(primitive.Timestamp); ok {
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(int64(ts.T) * 1000))
				} else {
					b.(*array.TimestampBuilder).AppendNull()
				}
			}
	case primitive.Decimal128:
		decType := &arrow.Decimal128Type{Precision: 38, Scale: 18}
		return decType,
			func(mem memory.Allocator) array.Builder { return array.NewDecimal128Builder(mem, decType) },
			func(b array.Builder, val any) {
				if d, ok := val.(primitive.Decimal128); ok {
					if num, ok := asDecimal128(d.String(), 38, 18); ok {
						b.(*array.Decimal128Builder).Append(num)
						return
					}
				}
				b.(*array.Decimal128Builder).AppendNull()
			}
	case primitive.Binary:
		return arrow.BinaryTypes.Binary,
			func(mem memory.Allocator) array.Builder { return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary) },
			func(b array.Builder, val any) {
				if bin, ok := val.(primitive.Binary); ok {
					b.(*array.BinaryBuilder).Append(bin.Data)
				} else {
					b.(*array.BinaryBuilder).AppendNull()
				}
			}
	case []byte:
		return arrow.BinaryTypes.Binary,
			func(mem memory.Allocator) array.Builder { return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary) },
			func(b array.Builder, val any) {
				if data, ok := val.([]byte); ok {
					b.(*array.BinaryBuilder).Append(data)
				} else {
					b.(*array.BinaryBuilder).AppendNull()
				}
			}
	default:
		return arrow.BinaryTypes.String,
			func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
			func(b array.Builder, val any) {
				if val == nil {
					b.(*array.StringBuilder).AppendNull()
				} else {
					b.(*array.StringBuilder).Append(mongoValueToString(val))
				}
			}
	}
}

func mongoBuilderFromArrowType(dt arrow.DataType) (func(memory.Allocator) array.Builder, func(array.Builder, any)) {
	switch dt.ID() {
	case arrow.STRING:
		return func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
			func(b array.Builder, val any) {
				if val == nil {
					b.(*array.StringBuilder).AppendNull()
				} else if s, ok := val.(string); ok {
					b.(*array.StringBuilder).Append(s)
				} else {
					b.(*array.StringBuilder).Append(mongoValueToString(val))
				}
			}
	case arrow.INT64:
		return func(mem memory.Allocator) array.Builder { return array.NewInt64Builder(mem) },
			func(b array.Builder, val any) {
				b.(*array.Int64Builder).Append(mongoToInt64(val))
			}
	case arrow.FLOAT64:
		return func(mem memory.Allocator) array.Builder { return array.NewFloat64Builder(mem) },
			func(b array.Builder, val any) {
				if f, ok := val.(float64); ok {
					b.(*array.Float64Builder).Append(f)
				} else {
					b.(*array.Float64Builder).AppendNull()
				}
			}
	case arrow.DECIMAL128:
		decType := dt.(*arrow.Decimal128Type)
		return func(mem memory.Allocator) array.Builder { return array.NewDecimal128Builder(mem, decType) },
			func(b array.Builder, val any) {
				if d, ok := val.(primitive.Decimal128); ok {
					if num, ok := asDecimal128(d.String(), decType.Precision, decType.Scale); ok {
						b.(*array.Decimal128Builder).Append(num)
						return
					}
				}
				b.(*array.Decimal128Builder).AppendNull()
			}
	case arrow.BOOL:
		return func(mem memory.Allocator) array.Builder { return array.NewBooleanBuilder(mem) },
			func(b array.Builder, val any) {
				if bl, ok := val.(bool); ok {
					b.(*array.BooleanBuilder).Append(bl)
				} else {
					b.(*array.BooleanBuilder).AppendNull()
				}
			}
	case arrow.TIMESTAMP:
		return func(mem memory.Allocator) array.Builder {
				return array.NewTimestampBuilder(mem, dt.(*arrow.TimestampType))
			},
			func(b array.Builder, val any) {
				switch v := val.(type) {
				case time.Time:
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(v.UTC().UnixMilli()))
				case primitive.DateTime:
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(v))
				case primitive.Timestamp:
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(int64(v.T) * 1000))
				case int64:
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(v))
				default:
					b.(*array.TimestampBuilder).AppendNull()
				}
			}
	case arrow.BINARY:
		return func(mem memory.Allocator) array.Builder { return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary) },
			func(b array.Builder, val any) {
				switch v := val.(type) {
				case []byte:
					b.(*array.BinaryBuilder).Append(v)
				case primitive.Binary:
					b.(*array.BinaryBuilder).Append(v.Data)
				default:
					b.(*array.BinaryBuilder).AppendNull()
				}
			}
	default:
		return func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
			func(b array.Builder, val any) {
				if val == nil {
					b.(*array.StringBuilder).AppendNull()
				} else {
					b.(*array.StringBuilder).Append(mongoValueToString(val))
				}
			}
	}
}

func mongoToInt64(v any) int64 {
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	default:
		return 0
	}
}

func mongoValueToString(val any) string {
	switch v := val.(type) {
	case primitive.M, primitive.D, primitive.A, map[string]any, []any:
		b, err := bson.MarshalExtJSON(v, true, false)
		if err == nil {
			return string(b)
		}
		return fmt.Sprint(v)
	case primitive.Regex:
		return v.String()
	case primitive.JavaScript:
		return string(v)
	case primitive.CodeWithScope:
		scopeBytes, err := bson.MarshalExtJSON(v.Scope, true, false)
		if err == nil {
			return fmt.Sprintf(`{"$code":%q,"$scope":%s}`, v.Code, string(scopeBytes))
		}
		jsonScope, _ := json.Marshal(v.Scope)
		return fmt.Sprintf(`{"$code":%q,"$scope":%s}`, v.Code, string(jsonScope))
	case primitive.MinKey:
		return "$MinKey"
	case primitive.MaxKey:
		return "$MaxKey"
	case primitive.Undefined:
		return "$Undefined"
	case primitive.Symbol:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}
