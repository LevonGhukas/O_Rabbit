package connectors

import (
	"strings"
	"testing"
)

func TestCursorLookbackStart(t *testing.T) {
	for _, tc := range []struct {
		domain        CursorDomain
		hwm, lookback string
		want, wantErr string
	}{
		{domain: CursorDomainInt64, hwm: "1000", lookback: "250", want: "750"},
		{domain: CursorDomainInt64, hwm: "-9223372036854775800", lookback: "100", want: "-9223372036854775808"},
		{domain: CursorDomainUInt64, hwm: "10", lookback: "25", want: "0"},
		{domain: CursorDomainTimestamp, hwm: "2026-09-29T12:00:00Z", lookback: "90m", want: "2026-09-29T10:30:00Z"},
		{domain: CursorDomainDate, hwm: "2026-09-29", lookback: "48h", want: "2026-09-27"},
		{domain: CursorDomainInt64, hwm: "1000", lookback: "1h", wantErr: "positive integer"},
		{domain: CursorDomainTimestamp, hwm: "2026-09-29T12:00:00Z", lookback: "-5m", wantErr: "positive duration"},
		{domain: CursorDomainString, hwm: "abc", lookback: "10", wantErr: "not supported"},
		{domain: CursorDomainUUID, hwm: "x", lookback: "10", wantErr: "not supported"},
	} {
		got, err := CursorLookbackStart(tc.domain, tc.hwm, tc.lookback)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("%s %s-%s: err=%v, want %q", tc.domain, tc.hwm, tc.lookback, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Fatalf("%s %s-%s = %q, %v; want %q", tc.domain, tc.hwm, tc.lookback, got, err, tc.want)
		}
	}
}
