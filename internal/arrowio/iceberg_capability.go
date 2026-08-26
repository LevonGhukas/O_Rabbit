package arrowio

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// ValidateArrowSchemaForIcebergV2 rejects Arrow temporal schemas which the
// configured Iceberg v2 path cannot preserve exactly. This is used by Flight
// SQL, where source Arrow batches bypass SQL ColumnType descriptors.
func ValidateArrowSchemaForIcebergV2(schema *arrow.Schema) error {
	if schema == nil {
		return fmt.Errorf("nil Arrow schema")
	}
	for _, field := range schema.Fields() {
		switch dt := field.Type.(type) {
		case *arrow.Time64Type:
			if dt.Unit != arrow.Microsecond {
				return fmt.Errorf("column %q: Iceberg v2 supports only time[us], got %s", field.Name, dt)
			}
		case *arrow.TimestampType:
			if dt.Unit != arrow.Microsecond {
				return fmt.Errorf("column %q: source timestamp %s cannot be represented exactly by Iceberg v2 (maximum microseconds)", field.Name, dt)
			}
			if dt.TimeZone != "" && dt.TimeZone != "UTC" {
				return fmt.Errorf("column %q: timestamp timezone %q is not an Iceberg instant contract; use UTC or a timezone-free local timestamp", field.Name, dt.TimeZone)
			}
		}
	}
	return nil
}
