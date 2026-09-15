package typesystem

import (
	"errors"
	"strings"
	"testing"
)

func TestConvertRejectsPostgreSQLInfinityForNativeTemporalTypes(t *testing.T) {
	for _, kind := range []Kind{KindDate, KindTimestamp, KindTimestampTZ} {
		for _, value := range []string{"infinity", "-infinity"} {
			got, err := Convert(value, LogicalType{Kind: kind})
			var conversionErr *ConversionError
			if !errors.As(err, &conversionErr) || !strings.Contains(conversionErr.Reason, "not representable") || !strings.Contains(conversionErr.Reason, "override this column to string") {
				t.Fatalf("Convert(%q, %s) = %#v, %T %v", value, kind, got, err, err)
			}
		}
	}
}

func TestConvertPreservesPostgreSQLInfinityForString(t *testing.T) {
	for _, value := range []string{"infinity", "-infinity"} {
		got, err := Convert(value, LogicalType{Kind: KindString})
		if err != nil || got != value {
			t.Fatalf("Convert(%q, string) = %#v, %v", value, got, err)
		}
	}
}
