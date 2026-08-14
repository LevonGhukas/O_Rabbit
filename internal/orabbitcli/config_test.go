package orabbitcli

import "testing"

func TestOracleSourcePromptHelpers(t *testing.T) {
	if got := sourceDSNPromptLabel("oracle"); got != "Oracle connection URL" {
		t.Fatalf("sourceDSNPromptLabel(oracle)=%q", got)
	}
	if got := defaultSourceDSN("oracle", ""); got != "oracle://user:password@localhost:1521/ORCLCDB" {
		t.Fatalf("defaultSourceDSN(oracle)=%q", got)
	}
	note := sourceDSNPromptNote("oracle")
	if note == "" {
		t.Fatal("sourceDSNPromptNote(oracle) should not be empty")
	}
	if got := normalizeSourceEngine("ORA"); got != "oracle" {
		t.Fatalf("normalizeSourceEngine(ORA)=%q want oracle", got)
	}
}
