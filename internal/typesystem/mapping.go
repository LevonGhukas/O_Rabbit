package typesystem

// MappingClass describes how faithfully a logical type reaches a destination.
// It is deliberately destination-neutral so Arrow and Iceberg decisions can be
// compared without either package importing the other.
type MappingClass string

const (
	MappingExact               MappingClass = "exact"
	MappingSafePromotion       MappingClass = "safe_promotion"
	MappingSemanticFallback    MappingClass = "semantic_fallback"
	MappingUnsupportedFallback MappingClass = "unsupported_fallback"
)

// MappingResult records a destination mapping decision for later diagnostics.
// Destination is intentionally textual: the physical type remains owned by
// the destination package, while this metadata stays dependency-free.
type MappingResult struct {
	LogicalType LogicalType
	Destination string
	Class       MappingClass
	Fallback    bool
	Reason      string
}

func MappingFor(t LogicalType, destination string, class MappingClass, reason string) MappingResult {
	return MappingResult{
		LogicalType: t,
		Destination: destination,
		Class:       class,
		Fallback:    class == MappingSemanticFallback || class == MappingUnsupportedFallback,
		Reason:      reason,
	}
}
