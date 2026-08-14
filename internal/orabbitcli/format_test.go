package orabbitcli

import (
	"strings"
	"testing"
)

func TestWrapTextKeepsTokensIntact(t *testing.T) {
	lines := wrapText(
		"Use --master-bin/--worker-bin to override local daemon resolution cleanly.",
		40,
		"  - ",
		"    ",
	)
	got := strings.Join(lines, "\n")
	if !strings.Contains(got, "--master-bin/--worker-bin") {
		t.Fatalf("wrapped text lost combined flag token: %q", got)
	}
	if strings.Contains(got, "--master-bin/\n") {
		t.Fatalf("wrapped text split combined flag token: %q", got)
	}
}
