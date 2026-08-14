package envutil

import "testing"

func TestParsePositiveIntValid(t *testing.T) {
	tests := map[string]int{
		"1":     1,
		"42":    42,
		"  7  ": 7,
	}
	for raw, want := range tests {
		got, ok := ParsePositiveInt(raw)
		if !ok || got != want {
			t.Fatalf("ParsePositiveInt(%q)=(%d,%v), want (%d,true)", raw, got, ok, want)
		}
	}
}

func TestParsePositiveIntRejectsInvalid(t *testing.T) {
	tests := []string{"", "0", "-1", "abc", "  "}
	for _, raw := range tests {
		got, ok := ParsePositiveInt(raw)
		if ok {
			t.Fatalf("ParsePositiveInt(%q)=(%d,%v), want ok=false", raw, got, ok)
		}
	}
}
