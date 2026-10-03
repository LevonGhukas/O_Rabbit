package arrowio

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// PlanForOverride plans a column whose type came from a column_types override.
// An explicit "uuid" override is stored as a native UUID (Arrow arrow.uuid ->
// Parquet UUID -> Iceberg uuid). Inferred UUIDs and the "source" placeholder
// keep canonical UUID text, so tables created with string UUID columns stay
// appendable; native UUID is opt-in per column.
func PlanForOverride(name, raw string, logical typesystem.LogicalType) (ColumnPlan, typesystem.MappingResult, error) {
	if isSource, _ := typesystem.IsSourceOverride(raw); strings.TrimSpace(raw) != "" && !isSource && logical.Kind == typesystem.KindUUID {
		return planNativeUUID(name, logical)
	}
	return PlanForLogicalType(name, logical)
}

func planNativeUUID(name string, t typesystem.LogicalType) (ColumnPlan, typesystem.MappingResult, error) {
	dataType := extensions.NewUUIDType()
	mapping := typesystem.MappingFor(t, "uuid", typesystem.MappingExact, "")
	mapping.Target = "uuid"
	plan := ColumnPlan{
		Name:     name,
		DataType: dataType,
		Builder:  func(mem memory.Allocator) array.Builder { return extensions.NewUUIDBuilder(mem) },
	}
	plan.Append = func(builder array.Builder, raw any) error {
		canonical, err := typesystem.Convert(raw, t)
		if err != nil {
			return err
		}
		b, ok := builder.(*extensions.UUIDBuilder)
		if !ok {
			return fmt.Errorf("uuid append: unexpected builder %T", builder)
		}
		if canonical == nil {
			b.AppendNull()
			return nil
		}
		return b.AppendValueFromString(canonical.(string))
	}
	return plan, mapping, nil
}
