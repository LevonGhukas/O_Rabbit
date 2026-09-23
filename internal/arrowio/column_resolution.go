package arrowio

import (
	"database/sql"
	"fmt"
	"math/big"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// ColumnResolution tracks one result column from its source type and the
// user's requested override to the effective type that is actually written.
//
// User overrides are authoritative. They are only replaced when the source
// data proves (or cannot be verified to prove) that honoring them would lose
// data; each replacement produces a TypeWarning explaining why.
type ColumnResolution struct {
	Column              string
	Source              typesystem.LogicalType
	SourceNullableKnown bool
	KnownNotNull        bool // catalog-proven NOT NULL (driver could not tell)

	RequestedRaw string
	Requested    *typesystem.LogicalType

	Probe connectors.ColumnProbe
}

// NeedsProbe reports whether verifying this column requires reading data.
func (r ColumnResolution) NeedsProbe() bool {
	return r.Probe.Range || r.Probe.Nulls || r.Probe.Fraction
}

// PlanColumnResolutions pairs every result column with its override (if any)
// and decides which data facts are needed to verify that override.
func PlanColumnResolutions(engine string, cols []string, colTypes []*sql.ColumnType, overrides map[string]string, knownNotNull map[string]bool) ([]ColumnResolution, error) {
	out := make([]ColumnResolution, 0, len(cols))
	for i, col := range cols {
		var ct *sql.ColumnType
		if colTypes != nil && i < len(colTypes) {
			ct = colTypes[i]
		}
		source, nullableKnown, err := SourceLogicalTypeForSQLColumn(engine, ct)
		if err != nil {
			return nil, fmt.Errorf("column %s source type: %w", col, err)
		}
		r := ColumnResolution{Column: col, Source: source, SourceNullableKnown: nullableKnown, Probe: connectors.ColumnProbe{Column: col}}
		if !nullableKnown && knownNotNull[col] {
			r.KnownNotNull = true
		}
		// Unsigned 64-bit columns (IDs, counters) are stored as int64 when every
		// value fits; decimal(20,0) is only needed beyond int64's range.
		r.Probe.Range = source.Kind == typesystem.KindUInt64
		if raw, ok := lookupColumnOverride(overrides, col); ok {
			requested, err := typesystem.ResolveOverride(raw, source)
			if err != nil {
				return nil, fmt.Errorf("column %s: invalid type %q: %w", col, raw, err)
			}
			r.RequestedRaw = strings.TrimSpace(raw)
			r.setRequested(requested)
		}
		out = append(out, r)
	}
	return out, nil
}

// setRequested records an override and plans the data checks that verify it.
func (r *ColumnResolution) setRequested(requested typesystem.LogicalType) {
	r.Requested = &requested
	r.Probe.Range, r.Probe.Fraction = typeProbeNeeds(r.Source, requested)
	// uint64 is stored as int64 when values fit, which needs the range.
	r.Probe.Range = r.Probe.Range || requested.Kind == typesystem.KindUInt64
	if requested.Kind == typesystem.KindDecimal {
		r.Probe.Scale = *requested.Scale
	}
	r.Probe.Nulls = !requested.Nullable && r.sourceMayBeNull()
}

func (r ColumnResolution) sourceMayBeNull() bool {
	if r.KnownNotNull {
		return false
	}
	return !r.SourceNullableKnown || r.Source.Nullable
}

// FinalizeColumnResolutions turns probe results into effective column types
// and user-facing warnings. probeErr is set when the source could not be read
// for verification; affected overrides then fall back to lossless choices.
func FinalizeColumnResolutions(resolutions []ColumnResolution, stats map[string]connectors.ColumnProbeResult, probeErr error) (map[string]string, []typesystem.TypeWarning) {
	effective := map[string]string{}
	var warnings []typesystem.TypeWarning
	for _, r := range resolutions {
		st, hasStats := stats[r.Column]
		hasStats = hasStats && probeErr == nil
		if r.Requested == nil {
			// No override: keep the source-inferred type, narrowing uint64 to
			// int64 when the data allows it, and carry catalog-proven NOT NULL
			// that the driver could not report.
			if r.Source.Kind == typesystem.KindUInt64 && hasStats && (st.Min == nil || fitsLogical(typesystem.LogicalType{Kind: typesystem.KindInt64}, st.Min, st.Max)) {
				effective[r.Column] = typesystem.LogicalType{Kind: typesystem.KindInt64, Nullable: r.sourceMayBeNull()}.String()
			} else if r.KnownNotNull {
				effective[r.Column] = typesystem.SourceTypeKeyword
			}
			continue
		}

		final, useSource, typeReason := resolveOverrideType(r, st, hasStats, probeErr)
		if !useSource && final.Kind == typesystem.KindUInt64 && hasStats && (st.Min == nil || fitsLogical(typesystem.LogicalType{Kind: typesystem.KindInt64}, st.Min, st.Max)) {
			// Same storage rule as the default path: uint64 values that fit
			// int64 are stored as int64 rather than decimal(20,0).
			final = typesystem.LogicalType{Kind: typesystem.KindInt64}
		}
		nullable, nullReason := resolveOverrideNullability(r, st, hasStats, probeErr)

		var rendered string
		if useSource {
			rendered = typesystem.SourceTypeKeyword
			if nullable {
				rendered = "nullable<" + typesystem.SourceTypeKeyword + ">"
			}
		} else {
			final.Nullable = nullable
			rendered = final.String()
		}
		effective[r.Column] = rendered

		if typeReason == "" && nullReason == "" {
			continue
		}
		storage := rendered
		if useSource {
			src := r.Source
			src.Nullable = nullable
			storage = src.String() + " (source type)"
		}
		reason := strings.TrimSpace(strings.Join(nonEmpty(typeReason, nullReason), " "))
		warnings = append(warnings, typesystem.TypeWarning{
			Column:      r.Column,
			SourceType:  sourceTypeLabel(r.Source),
			LogicalType: r.RequestedRaw,
			StorageType: storage,
			Class:       typesystem.MappingOverrideFallback,
			Reason:      fmt.Sprintf("Column %q: %s", r.Column, reason),
		})
	}
	return effective, warnings
}

func resolveOverrideType(r ColumnResolution, st connectors.ColumnProbeResult, hasStats bool, probeErr error) (typesystem.LogicalType, bool, string) {
	final, useSource, reason := resolveOverrideRange(r, st, hasStats, probeErr)
	if useSource {
		return final, true, reason
	}
	if widened, scaleReason, ok := widenDecimalScale(r.Source, final); ok {
		return widened, false, strings.TrimSpace(reason + " " + scaleReason)
	}
	return final, false, reason
}

func resolveOverrideRange(r ColumnResolution, st connectors.ColumnProbeResult, hasStats bool, probeErr error) (typesystem.LogicalType, bool, string) {
	req := *r.Requested
	if !r.Probe.Range && !r.Probe.Fraction {
		return req, false, ""
	}
	label := req.Kind.String()
	if req.Kind == typesystem.KindDecimal {
		label = renderDecimalLabel(req)
	}
	if !hasStats {
		return req, true, fmt.Sprintf("you selected %s, but the source values could not be checked (%v), so the source type %s is kept to avoid possible data loss.", label, probeErrText(probeErr), sourceTypeLabel(r.Source))
	}
	if st.Min == nil || st.Max == nil {
		return req, false, "" // no non-NULL values: any numeric type holds them
	}
	if st.FractionCount > 0 && r.Probe.Fraction {
		what := "have a fractional part and would be truncated"
		if req.Kind == typesystem.KindDecimal {
			what = fmt.Sprintf("have more than %d decimal places and would be rounded", *req.Scale)
		}
		return req, true, fmt.Sprintf("you selected %s, but %d value(s) %s, so the source type %s is kept.", label, st.FractionCount, what, sourceTypeLabel(r.Source))
	}
	if fitsLogical(req, st.Min, st.Max) {
		return req, false, ""
	}
	rangeText := fmt.Sprintf("values range from %s to %s", ratText(st.Min), ratText(st.Max))
	if fallback, ok := widerFallback(req, st.Min, st.Max); ok {
		return fallback, false, fmt.Sprintf("you selected %s, but the %s, which does not fit %s%s. Using %s instead so no values are lost.", label, rangeText, label, boundsText(req), fallbackLabel(fallback))
	}
	return req, true, fmt.Sprintf("you selected %s, but the %s, which does not fit %s, so the source type %s is kept.", label, rangeText, label, sourceTypeLabel(r.Source))
}

func resolveOverrideNullability(r ColumnResolution, st connectors.ColumnProbeResult, hasStats bool, probeErr error) (bool, string) {
	if r.Requested.Nullable {
		return true, ""
	}
	if !r.Probe.Nulls {
		return false, ""
	}
	if !hasStats {
		return true, fmt.Sprintf("it was marked NOT NULL, but NULL values could not be checked (%v), so it is kept nullable.", probeErrText(probeErr))
	}
	if st.NullCount > 0 {
		return true, fmt.Sprintf("it was marked NOT NULL, but %d row(s) contain NULL, so it is kept nullable.", st.NullCount)
	}
	return false, ""
}

// typeProbeNeeds decides which data checks prove source values fit requested.
func typeProbeNeeds(source, requested typesystem.LogicalType) (needRange, needFraction bool) {
	class := numericClassOf(source)
	switch {
	case isIntegerKind(requested.Kind):
		switch class {
		case numericInteger:
			return !integerKindFits(source.Kind, requested.Kind), false
		case numericExact, numericFloat:
			return true, !(source.Kind == typesystem.KindDecimal && source.Scale != nil && *source.Scale == 0)
		}
	case requested.Kind == typesystem.KindDecimal:
		switch class {
		case numericInteger:
			return integerDigits(source.Kind) > int(*requested.Precision-*requested.Scale), false
		case numericExact:
			if source.Kind == typesystem.KindDecimal {
				// Extra source scale is widened at schema level (widenDecimalScale).
				return int(*source.Precision-*source.Scale) > int(*requested.Precision-*requested.Scale), false
			}
			return true, true
		case numericFloat:
			// Floats are approximate; rounding them to the requested scale is
			// the conversion the user asked for, so only the range is checked.
			return true, false
		}
	}
	return false, false
}

// widenDecimalScale keeps source fractional digits that a narrower requested
// decimal scale would otherwise round away.
func widenDecimalScale(source, requested typesystem.LogicalType) (typesystem.LogicalType, string, bool) {
	if source.Kind != typesystem.KindDecimal || requested.Kind != typesystem.KindDecimal || source.Scale == nil || *source.Scale <= *requested.Scale {
		return typesystem.LogicalType{}, "", false
	}
	// Integer digits were already checked (typeProbeNeeds/range probe), so
	// only the scale is widened here.
	scale := *source.Scale
	precision := *requested.Precision - *requested.Scale + scale
	if precision > 38 {
		return typesystem.LogicalType{}, "", false
	}
	widened := typesystem.Decimal(precision, scale)
	return widened, fmt.Sprintf("you selected %s, but the source keeps %d decimal places, which would be rounded. Using %s instead so no digits are lost.", renderDecimalLabel(requested), scale, renderDecimalLabel(widened)), true
}

type numericClass int

const (
	numericNone numericClass = iota
	numericInteger
	numericExact
	numericFloat
)

func numericClassOf(t typesystem.LogicalType) numericClass {
	switch {
	case isIntegerKind(t.Kind):
		return numericInteger
	case t.Kind == typesystem.KindDecimal:
		return numericExact
	case t.Kind == typesystem.KindFloat32 || t.Kind == typesystem.KindFloat64:
		return numericFloat
	case t.Kind == typesystem.KindUnknown:
		// Unconstrained NUMERIC/DECIMAL/NUMBER columns have no precision in
		// metadata but are still numeric in the source, so they can be probed.
		name := strings.ToUpper(strings.TrimSpace(t.SourceTypeName))
		for _, n := range []string{"NUMERIC", "DECIMAL", "NUMBER", "DEC", "FIXED"} {
			if name == n || strings.HasPrefix(name, n+"(") {
				return numericExact
			}
		}
	}
	return numericNone
}

func isIntegerKind(k typesystem.Kind) bool {
	switch k {
	case typesystem.KindInt8, typesystem.KindInt16, typesystem.KindInt32, typesystem.KindInt64,
		typesystem.KindUInt8, typesystem.KindUInt16, typesystem.KindUInt32, typesystem.KindUInt64:
		return true
	}
	return false
}

func integerBounds(k typesystem.Kind) (*big.Int, *big.Int) {
	bits := map[typesystem.Kind]uint{
		typesystem.KindInt8: 8, typesystem.KindInt16: 16, typesystem.KindInt32: 32, typesystem.KindInt64: 64,
		typesystem.KindUInt8: 8, typesystem.KindUInt16: 16, typesystem.KindUInt32: 32, typesystem.KindUInt64: 64,
	}[k]
	one := big.NewInt(1)
	switch k {
	case typesystem.KindUInt8, typesystem.KindUInt16, typesystem.KindUInt32, typesystem.KindUInt64:
		max := new(big.Int).Sub(new(big.Int).Lsh(one, bits), one)
		return big.NewInt(0), max
	}
	max := new(big.Int).Sub(new(big.Int).Lsh(one, bits-1), one)
	min := new(big.Int).Neg(new(big.Int).Lsh(one, bits-1))
	return min, max
}

func integerKindFits(source, target typesystem.Kind) bool {
	smin, smax := integerBounds(source)
	tmin, tmax := integerBounds(target)
	return smin.Cmp(tmin) >= 0 && smax.Cmp(tmax) <= 0
}

func integerDigits(k typesystem.Kind) int {
	min, max := integerBounds(k)
	if d := len(new(big.Int).Abs(min).String()); d > len(max.String()) {
		return d
	}
	return len(max.String())
}

func fitsLogical(t typesystem.LogicalType, min, max *big.Rat) bool {
	switch {
	case isIntegerKind(t.Kind):
		lo, hi := integerBounds(t.Kind)
		return min.Cmp(new(big.Rat).SetInt(lo)) >= 0 && max.Cmp(new(big.Rat).SetInt(hi)) <= 0
	case t.Kind == typesystem.KindDecimal:
		limit := decimalIntegerLimit(*t.Precision - *t.Scale)
		return new(big.Rat).Abs(min).Cmp(limit) < 0 && new(big.Rat).Abs(max).Cmp(limit) < 0
	}
	return true
}

// decimalIntegerLimit returns 10^digits: the exclusive magnitude bound for a
// decimal with that many integer digits.
func decimalIntegerLimit(digits int32) *big.Rat {
	return new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(digits)), nil))
}

// widerFallback returns the smallest lossless type at least as wide as req
// that holds [min, max].
func widerFallback(req typesystem.LogicalType, min, max *big.Rat) (typesystem.LogicalType, bool) {
	if isIntegerKind(req.Kind) {
		signed := []typesystem.Kind{typesystem.KindInt8, typesystem.KindInt16, typesystem.KindInt32, typesystem.KindInt64}
		unsigned := []typesystem.Kind{typesystem.KindUInt8, typesystem.KindUInt16, typesystem.KindUInt32, typesystem.KindUInt64}
		candidates := signed
		if isUnsignedKind(req.Kind) && min.Sign() >= 0 {
			candidates = unsigned
		}
		for _, k := range candidates {
			if t := (typesystem.LogicalType{Kind: k}); fitsLogical(t, min, max) {
				return t, true
			}
		}
		d := typesystem.Decimal(38, 0)
		return d, fitsLogical(d, min, max)
	}
	if req.Kind == typesystem.KindDecimal {
		for p := *req.Precision + 1; p <= 38; p++ {
			d := typesystem.Decimal(p, *req.Scale)
			if fitsLogical(d, min, max) {
				return d, true
			}
		}
	}
	return typesystem.LogicalType{}, false
}

func isUnsignedKind(k typesystem.Kind) bool {
	switch k {
	case typesystem.KindUInt8, typesystem.KindUInt16, typesystem.KindUInt32, typesystem.KindUInt64:
		return true
	}
	return false
}

func boundsText(t typesystem.LogicalType) string {
	if isIntegerKind(t.Kind) {
		lo, hi := integerBounds(t.Kind)
		return fmt.Sprintf(" (%s to %s)", lo.String(), hi.String())
	}
	if t.Kind == typesystem.KindDecimal {
		return fmt.Sprintf(" (at most %d digits before the decimal point)", *t.Precision-*t.Scale)
	}
	return ""
}

func fallbackLabel(t typesystem.LogicalType) string {
	if t.Kind == typesystem.KindDecimal {
		return renderDecimalLabel(t)
	}
	return t.Kind.String()
}

func renderDecimalLabel(t typesystem.LogicalType) string {
	t.Nullable = false
	return t.String()
}

func sourceTypeLabel(t typesystem.LogicalType) string {
	if name := strings.TrimSpace(t.SourceTypeName); name != "" && t.Kind == typesystem.KindUnknown {
		return name
	}
	t.Nullable = false
	return t.String()
}

func ratText(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	return strings.TrimRight(strings.TrimRight(r.FloatString(10), "0"), ".")
}

func probeErrText(err error) string {
	if err == nil {
		return "no statistics returned"
	}
	return err.Error()
}

func nonEmpty(values ...string) []string {
	out := values[:0]
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out
}
