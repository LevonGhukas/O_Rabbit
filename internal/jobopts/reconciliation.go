package jobopts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

// Reconciliation modes: whether the master independently counts the planned
// source range after extraction and compares it with the rows written.
const (
	ReconciliationModeOff   = "off"
	ReconciliationModeCount = "count"
)

// Reconciliation consistency declares what the operator asserts about the
// source while a run is in flight.
const (
	// ReconciliationConsistencyNone makes no claim: the source may change, so
	// a count difference is only indeterminate (recorded as a warning).
	ReconciliationConsistencyNone = "none"
	// ReconciliationConsistencyImmutable asserts that rows in the planned range
	// do not change during the run, so a count difference is proven data loss.
	ReconciliationConsistencyImmutable = "immutable"
)

// Reconciliation on_mismatch actions for a proven mismatch.
const (
	ReconciliationOnMismatchFail   = "fail"
	ReconciliationOnMismatchReview = "review"
)

const (
	reconciliationKey   = "reconciliation"
	reconciliationField = "." + reconciliationKey
)

// Reconciliation is options_json.reconciliation. Absent means mode "off".
type Reconciliation struct {
	Mode        string `json:"mode,omitempty"`
	Consistency string `json:"consistency,omitempty"`
	OnMismatch  string `json:"on_mismatch,omitempty"`
}

// EffectiveReconciliation returns the reconciliation settings with defaults
// applied and values normalized (mode off, consistency none, on_mismatch fail).
func (o Options) EffectiveReconciliation() Reconciliation {
	r := Reconciliation{}
	if o.Reconciliation != nil {
		r = *o.Reconciliation
	}
	return Reconciliation{
		Mode:        defaultedLower(r.Mode, ReconciliationModeOff),
		Consistency: defaultedLower(r.Consistency, ReconciliationConsistencyNone),
		OnMismatch:  defaultedLower(r.OnMismatch, ReconciliationOnMismatchFail),
	}
}

// Enabled reports whether source reconciliation runs for this job.
func (r Reconciliation) Enabled() bool { return r.Mode == ReconciliationModeCount }

func defaultedLower(value, fallback string) string {
	if v := strings.ToLower(strings.TrimSpace(value)); v != "" {
		return v
	}
	return fallback
}

// validateReconciliation checks values and combinations that do not depend on
// the source engine. Unlike consistency_mode this option has no legacy values
// to tolerate, so it is checked in strict and lenient mode alike.
func (o Options) validateReconciliation(add func(field, format string, args ...any)) {
	if o.Reconciliation == nil {
		return
	}
	r := o.EffectiveReconciliation()
	switch r.Mode {
	case ReconciliationModeOff, ReconciliationModeCount:
	default:
		add(reconciliationField+".mode", "must be %s or %s, got %q", ReconciliationModeOff, ReconciliationModeCount, o.Reconciliation.Mode)
	}
	switch r.Consistency {
	case ReconciliationConsistencyNone, ReconciliationConsistencyImmutable:
	default:
		add(reconciliationField+".consistency", "must be %s or %s, got %q", ReconciliationConsistencyNone, ReconciliationConsistencyImmutable, o.Reconciliation.Consistency)
	}
	switch r.OnMismatch {
	case ReconciliationOnMismatchFail, ReconciliationOnMismatchReview:
	default:
		add(reconciliationField+".on_mismatch", "must be %s or %s, got %q", ReconciliationOnMismatchFail, ReconciliationOnMismatchReview, o.Reconciliation.OnMismatch)
	}
	if !r.Enabled() {
		return
	}
	if o.NormalizedPartitionStrategy() != "ordered_cursor" {
		add(reconciliationField+".mode", "count needs partition_strategy ordered_cursor so extraction and the count share one planned range")
	}
}

// ValidateReconciliationEngine rejects mode=count for a source engine that
// cannot count a cursor range. It is separate from Validate because
// options_json does not carry the engine.
func ValidateReconciliationEngine(o Options, engine string) error {
	if !o.EffectiveReconciliation().Enabled() || connectors.SupportsRangeCount(engine) {
		return nil
	}
	return &ValidationError{Errors: []FieldError{{
		Field: "options_json" + reconciliationField + ".mode",
		Error: fmt.Sprintf("count is not supported for source engine %q", connectors.NormalizeSourceEngine(engine)),
	}}}
}

// checkReconciliationKeys reports unknown keys so a typo such as "modee" never
// silently leaves reconciliation off.
func checkReconciliationKeys(value json.RawMessage, add func(field, format string, args ...any)) {
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.DisallowUnknownFields()
	var r Reconciliation
	if err := dec.Decode(&r); err != nil {
		add(reconciliationField, "%v", err)
	}
}

// ValidateReconciliation checks the reconciliation settings alone, for callers
// that build Options without a stored options_json (such as run submit).
func (o Options) ValidateReconciliation() error {
	var errs []FieldError
	o.validateReconciliation(func(field, format string, args ...any) {
		errs = append(errs, FieldError{Field: "options_json" + field, Error: fmt.Sprintf(format, args...)})
	})
	if len(errs) == 0 {
		return nil
	}
	return &ValidationError{Errors: errs}
}
