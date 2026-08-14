package connectors

import (
	"os"
	"sync"
	"testing"
)

func TestMariaDBOrderedRangeReadsEnabled(t *testing.T) {
	prev, hadPrev := os.LookupEnv(envMariaDBOrderedReads)
	defer func() {
		if hadPrev {
			_ = os.Setenv(envMariaDBOrderedReads, prev)
		} else {
			_ = os.Unsetenv(envMariaDBOrderedReads)
		}
		mariadbOrderedReadsOnce = sync.Once{}
		mariadbOrderedReads = true
	}()

	tests := []struct {
		raw  *string
		want bool
	}{
		{raw: nil, want: true},
		{raw: strPtr("off"), want: false},
		{raw: strPtr("On"), want: true},
	}

	for _, tc := range tests {
		mariadbOrderedReadsOnce = sync.Once{}
		mariadbOrderedReads = true
		if tc.raw == nil {
			_ = os.Unsetenv(envMariaDBOrderedReads)
		} else {
			_ = os.Setenv(envMariaDBOrderedReads, *tc.raw)
		}
		if got := mariadbOrderedRangeReadsEnabled(); got != tc.want {
			val := "<unset>"
			if tc.raw != nil {
				val = *tc.raw
			}
			t.Fatalf("mariadbOrderedRangeReadsEnabled(%q)=%v want %v", val, got, tc.want)
		}
	}
}
