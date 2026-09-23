package arrowio

import (
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func resolutionFor(t *testing.T, source typesystem.LogicalType, nullableKnown bool, raw string) ColumnResolution {
	t.Helper()
	requested, err := typesystem.ResolveOverride(raw, source)
	if err != nil {
		t.Fatalf("resolve %q: %v", raw, err)
	}
	r := ColumnResolution{Column: "c", Source: source, SourceNullableKnown: nullableKnown, RequestedRaw: raw, Probe: connectors.ColumnProbe{Column: "c"}}
	r.setRequested(requested)
	return r
}

func rat(v int64) *big.Rat { return new(big.Rat).SetInt64(v) }

func finalize(r ColumnResolution, st *connectors.ColumnProbeResult, probeErr error) (string, []typesystem.TypeWarning) {
	stats := map[string]connectors.ColumnProbeResult{}
	if st != nil {
		st.Column = r.Column
		stats[r.Column] = *st
	}
	effective, warnings := FinalizeColumnResolutions([]ColumnResolution{r}, stats, probeErr)
	return effective[r.Column], warnings
}

func TestOverrideNarrowingFallsBackWhenValuesDoNotFit(t *testing.T) {
	r := resolutionFor(t, typesystem.LogicalType{Kind: typesystem.KindInt64}, true, "int32")
	if !r.Probe.Range || r.Probe.Fraction {
		t.Fatalf("probe = %+v, want range only", r.Probe)
	}
	got, warnings := finalize(r, &connectors.ColumnProbeResult{Min: rat(1), Max: rat(3_000_000_000)}, nil)
	if got != "int64" {
		t.Fatalf("effective = %q, want int64", got)
	}
	if len(warnings) != 1 || warnings[0].Class != typesystem.MappingOverrideFallback {
		t.Fatalf("warnings = %+v", warnings)
	}
	for _, want := range []string{`Column "c"`, "int32", "3000000000", "2147483647", "Using int64"} {
		if !strings.Contains(warnings[0].Reason, want) {
			t.Fatalf("reason %q missing %q", warnings[0].Reason, want)
		}
	}
}

func TestOverrideNarrowingKeptWhenValuesFit(t *testing.T) {
	r := resolutionFor(t, typesystem.LogicalType{Kind: typesystem.KindInt64}, true, "int32")
	got, warnings := finalize(r, &connectors.ColumnProbeResult{Min: rat(-5), Max: rat(1000)}, nil)
	if got != "int32" || len(warnings) != 0 {
		t.Fatalf("effective = %q warnings = %+v", got, warnings)
	}
}

func TestOverrideWideningNeedsNoProbe(t *testing.T) {
	r := resolutionFor(t, typesystem.LogicalType{Kind: typesystem.KindInt32}, true, "int64")
	if r.NeedsProbe() {
		t.Fatalf("widening should not probe: %+v", r.Probe)
	}
	got, warnings := finalize(r, nil, nil)
	if got != "int64" || len(warnings) != 0 {
		t.Fatalf("effective = %q warnings = %+v", got, warnings)
	}
}

func TestOverrideNotNullFallsBackToNullableWhenNullsExist(t *testing.T) {
	r := resolutionFor(t, typesystem.LogicalType{Kind: typesystem.KindInt32, Nullable: true}, true, "int32")
	if !r.Probe.Nulls {
		t.Fatalf("expected NULL probe")
	}
	got, warnings := finalize(r, &connectors.ColumnProbeResult{NullCount: 7}, nil)
	if got != "nullable<int32>" {
		t.Fatalf("effective = %q", got)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0].Reason, "7 row(s) contain NULL") {
		t.Fatalf("warnings = %+v", warnings)
	}
}

func TestOverrideNotNullKeptWhenNoNulls(t *testing.T) {
	r := resolutionFor(t, typesystem.LogicalType{Kind: typesystem.KindInt32, Nullable: true}, false, "source")
	got, warnings := finalize(r, &connectors.ColumnProbeResult{}, nil)
	if got != "int32" || len(warnings) != 0 {
		t.Fatalf("effective = %q warnings = %+v", got, warnings)
	}
}

func TestOverrideUnverifiableFallsBackToSourceType(t *testing.T) {
	r := resolutionFor(t, typesystem.LogicalType{Kind: typesystem.KindInt64, Nullable: true}, true, "int16")
	got, warnings := finalize(r, nil, errors.New("timeout"))
	if got != "nullable<source>" {
		t.Fatalf("effective = %q", got)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0].Reason, "timeout") {
		t.Fatalf("warnings = %+v", warnings)
	}
}

func TestOverrideFractionalValuesKeepSourceType(t *testing.T) {
	source := typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: "NUMERIC"}
	r := resolutionFor(t, source, true, "int64")
	if !r.Probe.Range || !r.Probe.Fraction {
		t.Fatalf("probe = %+v", r.Probe)
	}
	got, warnings := finalize(r, &connectors.ColumnProbeResult{Min: rat(0), Max: rat(10), FractionCount: 3}, nil)
	if got != "source" || len(warnings) != 1 || !strings.Contains(warnings[0].Reason, "fractional") {
		t.Fatalf("effective = %q warnings = %+v", got, warnings)
	}
}

func TestOverrideDecimalScaleIsWidened(t *testing.T) {
	r := resolutionFor(t, typesystem.Decimal(20, 4), true, "decimal(12,2)")
	got, warnings := finalize(r, &connectors.ColumnProbeResult{Min: rat(0), Max: rat(100)}, nil)
	if got != "decimal(14,4)" {
		t.Fatalf("effective = %q", got)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0].Reason, "4 decimal places") {
		t.Fatalf("warnings = %+v", warnings)
	}
}

func TestCatalogNotNullWithoutOverrideUsesSourceKeyword(t *testing.T) {
	r := ColumnResolution{Column: "id", Source: typesystem.LogicalType{Kind: typesystem.KindInt64, Nullable: true}, KnownNotNull: true}
	effective, warnings := FinalizeColumnResolutions([]ColumnResolution{r}, nil, nil)
	if effective["id"] != "source" || len(warnings) != 0 {
		t.Fatalf("effective = %+v warnings = %+v", effective, warnings)
	}
}

func TestEffectiveTypesRoundTrip(t *testing.T) {
	source := typesystem.LogicalType{Kind: typesystem.KindString}
	for _, raw := range []string{"int32", "nullable<int64>", "decimal(14,4)", "nullable<decimal(38,0)>", "timestamp_tz[UTC]", "array<nullable<int32>>", "source", "nullable<source>", "Nullable(source)"} {
		lt, err := typesystem.ResolveOverride(raw, source)
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if ok, _ := typesystem.IsSourceOverride(raw); ok {
			continue
		}
		again, err := typesystem.ParseType(lt.String())
		if err != nil || !again.Equal(lt) {
			t.Fatalf("%q -> %q does not round-trip: %v", raw, lt.String(), err)
		}
	}
}

func TestUInt64StoredAsLosslessDecimal(t *testing.T) {
	plan, mapping, err := PlanForLogicalType("u", typesystem.LogicalType{Kind: typesystem.KindUInt64})
	if err != nil {
		t.Fatal(err)
	}
	if mapping.Fallback {
		t.Fatalf("uint64 should not be a fallback: %+v", mapping)
	}
	dec, ok := plan.DataType.(*arrow.Decimal128Type)
	if !ok || dec.Precision != 20 || dec.Scale != 0 {
		t.Fatalf("data type = %v", plan.DataType)
	}
	b := plan.Builder(memory.NewGoAllocator())
	defer b.Release()
	if err := plan.Append(b, uint64(math.MaxUint64)); err != nil {
		t.Fatal(err)
	}
	arr := b.NewArray().(*array.Decimal128)
	defer arr.Release()
	if got := arr.Value(0).ToString(0); got != "18446744073709551615" {
		t.Fatalf("value = %s", got)
	}
}

func TestDefaultUInt64NarrowsToInt64WhenValuesFit(t *testing.T) {
	r := ColumnResolution{Column: "id", Source: typesystem.LogicalType{Kind: typesystem.KindUInt64}, SourceNullableKnown: true, Probe: connectors.ColumnProbe{Column: "id", Range: true}}
	effective, warnings := FinalizeColumnResolutions([]ColumnResolution{r}, map[string]connectors.ColumnProbeResult{"id": {Column: "id", Min: rat(101), Max: rat(104)}}, nil)
	if effective["id"] != "int64" || len(warnings) != 0 {
		t.Fatalf("effective = %+v warnings = %+v", effective, warnings)
	}
	huge := new(big.Rat).SetUint64(math.MaxUint64)
	effective, _ = FinalizeColumnResolutions([]ColumnResolution{r}, map[string]connectors.ColumnProbeResult{"id": {Column: "id", Min: rat(0), Max: huge}}, nil)
	if _, ok := effective["id"]; ok {
		t.Fatalf("values beyond int64 must keep decimal(20,0): %+v", effective)
	}
	effective, _ = FinalizeColumnResolutions([]ColumnResolution{r}, nil, errors.New("down"))
	if _, ok := effective["id"]; ok {
		t.Fatalf("unverified uint64 must keep decimal(20,0): %+v", effective)
	}
}

func TestRequestedUInt64StoredAsInt64WhenValuesFit(t *testing.T) {
	r := resolutionFor(t, typesystem.LogicalType{Kind: typesystem.KindUInt64}, true, "nullable<uint64>")
	if !r.Probe.Range {
		t.Fatalf("uint64 request must probe range: %+v", r.Probe)
	}
	got, warnings := finalize(r, &connectors.ColumnProbeResult{Min: rat(1), Max: rat(99)}, nil)
	if got != "nullable<int64>" || len(warnings) != 0 {
		t.Fatalf("effective = %q warnings = %+v", got, warnings)
	}
	got, _ = finalize(r, &connectors.ColumnProbeResult{Min: rat(0), Max: new(big.Rat).SetUint64(math.MaxUint64)}, nil)
	if got != "nullable<uint64>" {
		t.Fatalf("values beyond int64 keep uint64 (decimal storage): %q", got)
	}
}
