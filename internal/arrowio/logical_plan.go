package arrowio

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// PlanForLogicalType builds a plan whose Append path is strictly raw-value ->
// typesystem.Convert -> canonical Arrow append. PostgreSQL is its first user.
func PlanForLogicalType(name string, t typesystem.LogicalType) (ColumnPlan, typesystem.MappingResult, error) {
	dataType, mapping, err := ArrowTypeForLogicalType(t)
	if err != nil {
		return ColumnPlan{}, typesystem.MappingResult{}, err
	}
	plan := ColumnPlan{Name: name, DataType: dataType, Builder: func(mem memory.Allocator) array.Builder { return array.NewBuilder(mem, dataType) }}
	plan.Append = func(builder array.Builder, raw any) error {
		canonical, err := typesystem.Convert(raw, t)
		if err != nil {
			return err
		}
		return appendLogicalValue(builder, dataType, canonical)
	}
	return plan, mapping, nil
}

func appendLogicalValue(builder array.Builder, dataType arrow.DataType, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		b.Append(value.(bool))
	case *array.Int8Builder:
		b.Append(value.(int8))
	case *array.Int16Builder:
		b.Append(value.(int16))
	case *array.Int32Builder:
		b.Append(value.(int32))
	case *array.Int64Builder:
		b.Append(value.(int64))
	case *array.Uint8Builder:
		b.Append(value.(uint8))
	case *array.Uint16Builder:
		b.Append(value.(uint16))
	case *array.Uint32Builder:
		b.Append(value.(uint32))
	case *array.Uint64Builder:
		b.Append(value.(uint64))
	case *array.Float32Builder:
		b.Append(value.(float32))
	case *array.Float64Builder:
		b.Append(value.(float64))
	case *array.StringBuilder:
		text, ok := value.(string)
		if !ok {
			var err error
			text, err = typesystem.ToLosslessString(value)
			if err != nil {
				return err
			}
		}
		b.Append(text)
	case *array.BinaryBuilder:
		b.Append(value.([]byte))
	case *array.Date32Builder:
		date := value.(time.Time).UTC()
		b.Append(arrow.Date32(date.Unix() / 86400))
	case *array.Time64Builder:
		b.Append(arrow.Time64(value.(time.Duration).Microseconds()))
	case *array.TimestampBuilder:
		b.Append(arrow.Timestamp(value.(time.Time).UnixMicro()))
	case *array.Decimal128Builder:
		decimal := value.(typesystem.DecimalValue)
		if decimal.Unscaled == nil {
			return fmt.Errorf("decimal append: nil unscaled value")
		}
		b.Append(decimal128.FromBigInt(decimal.Unscaled))
	case *array.ListBuilder:
		items, ok := value.([]any)
		if !ok {
			return fmt.Errorf("list append: expected []any, got %T", value)
		}
		b.Append(true)
		elementType := dataType.(*arrow.ListType).Elem()
		for _, item := range items {
			if err := appendLogicalValue(b.ValueBuilder(), elementType, item); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported canonical Arrow builder %T", builder)
	}
	return nil
}
