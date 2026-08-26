package arrowio

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// TargetCapabilities describes only a target path proven by this deployment.
// Newer Iceberg format versions must not be selected until every configured
// writer, catalog, and reader has passed the end-to-end compatibility tests.
type TargetCapabilities struct {
	IcebergFormatVersion int
	MaxTemporalPrecision int
	Reason               string
}

// ConfiguredTargetCapabilities is intentionally v2. The configured ClickHouse
// images are 25.8.9 and 25.3.8; Iceberg v3 nanosecond timestamp reading first
// appears in ClickHouse 26.4. No compatible live catalog/read-back test exists.
func ConfiguredTargetCapabilities() TargetCapabilities {
	return TargetCapabilities{
		IcebergFormatVersion: 2,
		MaxTemporalPrecision: 6,
		Reason:               "configured ClickHouse 25.8.9/25.3.8 path lacks proven Iceberg v3 nanosecond timestamp read support; exact export supports temporal values through microseconds only",
	}
}

func (c TargetCapabilities) ValidateTemporalDescriptor(d SourceFieldDescriptor) (TypeCapability, error) {
	if d.TemporalSemantics == TemporalNone {
		return TypeCapability{}, nil
	}
	if d.TemporalSemantics == TemporalZonedTime {
		return TypeCapability{Reason: "Iceberg has no zoned-time type"}, fmt.Errorf("column %q %s: %s has no lossless Iceberg v%d representation", d.Name, d.SourceType, d.TemporalSemantics, c.IcebergFormatVersion)
	}
	if !d.TemporalPrecisionKnown || d.TemporalPrecision < 0 || d.TemporalPrecision > c.MaxTemporalPrecision {
		return TypeCapability{Reason: c.Reason}, fmt.Errorf("column %q %s: source precision %d cannot be represented exactly by configured Iceberg v%d temporal types (maximum microseconds)", d.Name, d.SourceType, d.TemporalPrecision, c.IcebergFormatVersion)
	}
	return TypeCapability{ArrowExact: true, ParquetExact: true, IcebergExact: true, ClickHouseExact: true}, nil
}

// ValidateArrowSchemaForTarget rejects temporal schemas which the configured
// target path cannot preserve exactly. Flight SQL uses it because its batches
// bypass SQL ColumnType descriptors.
func ValidateArrowSchemaForTarget(schema *arrow.Schema, c TargetCapabilities) error {
	if schema == nil {
		return fmt.Errorf("nil Arrow schema")
	}
	for _, field := range schema.Fields() {
		switch dt := field.Type.(type) {
		case *arrow.Time64Type:
			if dt.Unit != arrow.Microsecond {
				return fmt.Errorf("column %q: configured Iceberg v%d supports only time[us], got %s", field.Name, c.IcebergFormatVersion, dt)
			}
		case *arrow.TimestampType:
			if dt.Unit != arrow.Microsecond {
				return fmt.Errorf("column %q: source timestamp %s cannot be represented exactly by configured Iceberg v%d (maximum microseconds)", field.Name, dt, c.IcebergFormatVersion)
			}
			if dt.TimeZone != "" && dt.TimeZone != "UTC" {
				return fmt.Errorf("column %q: timestamp timezone %q is not an Iceberg instant contract; use UTC or a timezone-free local timestamp", field.Name, dt.TimeZone)
			}
		}
	}
	return nil
}

// ValidateArrowSchemaForConfiguredTarget is used at all descriptor-bypass
// boundaries, including Flight SQL and Arrow-to-Iceberg schema conversion.
func ValidateArrowSchemaForConfiguredTarget(schema *arrow.Schema) error {
	return ValidateArrowSchemaForTarget(schema, ConfiguredTargetCapabilities())
}

// ValidateArrowSchemaForIcebergV2 remains for callers outside this package.
func ValidateArrowSchemaForIcebergV2(schema *arrow.Schema) error {
	return ValidateArrowSchemaForConfiguredTarget(schema)
}
