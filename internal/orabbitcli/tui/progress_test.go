package tui

import (
	"github.com/LevonGhukas/O_Rabbit/internal/orabbitcli"

	"strings"
	"testing"
)

func TestRenderProgressBar(t *testing.T) {
	tests := []struct {
		done, total, width int
		want               string
	}{
		{0, 10, 10, "░░░░░░░░░░"},
		{5, 10, 10, "█████░░░░░"},
		{10, 10, 10, "██████████"},
		{3, 0, 4, "░░░░"},   // no tasks planned yet: no division by zero
		{20, 10, 4, "████"}, // never overflows the width
		{1, 3, 0, ""},
	}
	for _, tt := range tests {
		if got := renderProgressBar(tt.done, tt.total, tt.width); got != tt.want {
			t.Errorf("renderProgressBar(%d, %d, %d) = %q, want %q", tt.done, tt.total, tt.width, got, tt.want)
		}
	}
}

func TestSummarizeRunProgress(t *testing.T) {
	d := orabbitcli.RunDetails{Tasks: []orabbitcli.TaskInfo{
		{Status: "SUCCEEDED", RowsRead: 100},
		{Status: "succeeded", RowsRead: 50},
		{Status: "FAILED"},
		{Status: "CANCELED"},
		{Status: "RUNNING", RowsRead: 7},
		{Status: "PENDING"},
	}}
	got := summarizeRunProgress(d)
	want := runProgress{Total: 6, Done: 4, Failed: 2, Running: 1, RowsRead: 157}
	if got != want {
		t.Fatalf("summarizeRunProgress = %+v, want %+v", got, want)
	}
}

func TestIsTerminalRunStatus(t *testing.T) {
	for _, s := range []string{"SUCCEEDED", "failed", " CANCELED "} {
		if !isTerminalRunStatus(s) {
			t.Errorf("%q should be terminal", s)
		}
	}
	for _, s := range []string{"RUNNING", "PENDING", "COMMITTING", ""} {
		if isTerminalRunStatus(s) {
			t.Errorf("%q should not be terminal", s)
		}
	}
}

func TestFormatRunProgressLine(t *testing.T) {
	line := formatRunProgressLine("running", runProgress{Total: 4, Done: 2, Running: 1, Failed: 1, RowsRead: 12_400}, 4)
	for _, part := range []string{"RUNNING", "██░░", " 50%", "2/4 tasks", "1 running", "1 failed", "12.4k rows"} {
		if !strings.Contains(line, part) {
			t.Errorf("line %q is missing %q", line, part)
		}
	}
	// Optional parts are left out when they are zero.
	line = formatRunProgressLine("PENDING", runProgress{}, 4)
	if strings.Contains(line, "running") || strings.Contains(line, "failed") {
		t.Errorf("line %q shows zero counters", line)
	}
}

func TestHumanCount(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 999: "999", 1_000: "1.0k", 12_400: "12.4k", 1_250_000: "1.2M", 3_100_000_000: "3.1B"} {
		if got := humanCount(n); got != want {
			t.Errorf("humanCount(%d) = %q, want %q", n, got, want)
		}
	}
}
