package envutil

import "testing"

func TestParseBoolEnvTrueValues(t *testing.T) {
	tests := []string{"1", "true", "y", "yes", "on", " True ", "YES", " On "}
	for _, tc := range tests {
		got, ok := ParseBoolEnv(tc)
		if !ok || !got {
			t.Fatalf("ParseBoolEnv(%q)=(%v,%v), want (true,true)", tc, got, ok)
		}
	}
}

func TestParseBoolEnvFalseValues(t *testing.T) {
	tests := []string{"0", "false", "n", "no", "off", " False ", "NO", " Off "}
	for _, tc := range tests {
		got, ok := ParseBoolEnv(tc)
		if !ok || got {
			t.Fatalf("ParseBoolEnv(%q)=(%v,%v), want (false,true)", tc, got, ok)
		}
	}
}

func TestParseBoolEnvUnknown(t *testing.T) {
	tests := []string{"", "maybe", "2", "enabled"}
	for _, tc := range tests {
		got, ok := ParseBoolEnv(tc)
		if ok {
			t.Fatalf("ParseBoolEnv(%q)=(%v,%v), want ok=false", tc, got, ok)
		}
	}
}
