package arrowio

import "fmt"

// MappingPolicyVersionV1 identifies the first internal type-mapping policy.
const MappingPolicyVersionV1 = "v1"

// MappingKind describes whether the selected representation preserves a source
// type natively, through a structured representation, or through a fallback.
type MappingKind string

const (
	MappingNative     MappingKind = "native"
	MappingStructured MappingKind = "structured"
	MappingFallback   MappingKind = "fallback"
)

// FallbackCodec describes a versioned, lossless fallback encoding. Phase 0
// records this intent only; it does not implement or persist codecs.
type FallbackCodec struct {
	Name    string
	Version int
}

const genericTextFallbackCodec = "generic-text"

// SourceTypeMetadata carries common source semantics without changing Arrow,
// Parquet, or Iceberg schemas. Known flags distinguish a source value of zero
// or false from metadata unavailable from the source driver.
type SourceTypeMetadata struct {
	NullableKnown bool
	Nullable      bool

	PrecisionKnown bool
	Precision      int64
	ScaleKnown     bool
	Scale          int64

	UnsignedKnown bool
	Unsigned      bool
	BitWidthKnown bool
	BitWidth      int64

	TemporalPrecisionKnown bool
	TemporalPrecision      int64
	TemporalUnit           string
	TemporalSemantics      string

	FixedLengthKnown bool
	FixedLength      int64

	// Properties holds uncommon, source-specific semantics such as enum labels,
	// PostgreSQL array identity, geometry SRID, or BSON binary subtype.
	Properties map[string]string
}

// TypePolicy is the internal canonical description of a source field's current
// mapping. It is intentionally not written into Arrow field metadata in Phase 0.
type TypePolicy struct {
	Version      string
	SourceEngine string
	SourceType   string
	MappingKind  MappingKind
	Metadata     SourceTypeMetadata
	Fallback     *FallbackCodec
}

// Validate reports malformed policy combinations without interpreting values.
func (p TypePolicy) Validate() error {
	switch p.MappingKind {
	case MappingNative, MappingStructured, MappingFallback:
	default:
		return fmt.Errorf("unknown mapping kind %q", p.MappingKind)
	}
	if p.Version == "" {
		return fmt.Errorf("mapping policy version is required")
	}
	if p.SourceEngine == "" {
		return fmt.Errorf("source engine is required")
	}
	if p.MappingKind == MappingFallback {
		if p.Fallback == nil || p.Fallback.Name == "" || p.Fallback.Version <= 0 {
			return fmt.Errorf("fallback mapping requires a versioned codec")
		}
		return nil
	}
	if p.Fallback != nil {
		return fmt.Errorf("%s mapping must not specify a fallback codec", p.MappingKind)
	}
	return nil
}
