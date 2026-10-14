package jobopts

import "testing"

func TestValidateWriteMode(t *testing.T) {
	tests := []struct {
		mode        string
		incremental bool
		valid       bool
	}{
		{"", false, true},
		{"", true, true},
		{"overwrite", false, true},
		{" Overwrite ", false, true},
		{"append", true, true},
		{"overwrite", true, false}, // would replace the table with only new rows
		{"append", false, false},   // full-run append is not supported yet
		{"replace", false, false},
		{"merge", true, false},
	}
	for _, tt := range tests {
		if err := ValidateWriteMode(tt.mode, tt.incremental); (err == nil) != tt.valid {
			t.Errorf("ValidateWriteMode(%q, %t) err=%v, want valid=%t", tt.mode, tt.incremental, err, tt.valid)
		}
	}
}

func TestEffectiveWriteMode(t *testing.T) {
	tests := []struct {
		mode        string
		incremental bool
		want        string
	}{
		{"", false, WriteModeOverwrite},
		{"", true, WriteModeAppend},
		{" APPEND ", true, WriteModeAppend},
		{"overwrite", false, WriteModeOverwrite},
		{"typo", true, WriteModeAppend},
	}
	for _, tt := range tests {
		if got := EffectiveWriteMode(tt.mode, tt.incremental); got != tt.want {
			t.Errorf("EffectiveWriteMode(%q, %t)=%q, want %q", tt.mode, tt.incremental, got, tt.want)
		}
	}
}
