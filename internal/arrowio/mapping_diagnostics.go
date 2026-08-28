package arrowio

// MappingDiagnostic exposes a schema-level mapping decision without carrying
// source row values. It is passive: callers decide whether to inspect or log it.
type MappingDiagnostic struct {
	ColumnName           string
	SourceEngine         string
	SourceType           string
	MappingKind          MappingKind
	TargetArrowType      string
	FallbackCodecName    string
	FallbackCodecVersion int
}

// MappingDiagnostics returns one diagnostic per plan with a canonical policy.
func MappingDiagnostics(plans []ColumnPlan) []MappingDiagnostic {
	diagnostics := make([]MappingDiagnostic, 0, len(plans))
	for _, plan := range plans {
		if plan.Policy == nil {
			continue
		}
		diagnostic := MappingDiagnostic{
			ColumnName:      plan.Name,
			SourceEngine:    plan.Policy.SourceEngine,
			SourceType:      plan.Policy.SourceType,
			MappingKind:     plan.Policy.MappingKind,
			TargetArrowType: plan.DataType.String(),
		}
		if plan.Policy.Fallback != nil {
			diagnostic.FallbackCodecName = plan.Policy.Fallback.Name
			diagnostic.FallbackCodecVersion = plan.Policy.Fallback.Version
		}
		diagnostics = append(diagnostics, diagnostic)
	}
	return diagnostics
}
