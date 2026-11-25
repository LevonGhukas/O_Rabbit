package orabbitcli

import (
	"fmt"
	"strings"
)

// runProgress is a task-level summary of one run, used by /watch.
type runProgress struct {
	Total    int
	Done     int // tasks in a terminal state (succeeded, failed or canceled)
	Failed   int
	Running  int
	RowsRead int64
}

func summarizeRunProgress(d runDetails) runProgress {
	p := runProgress{Total: len(d.Tasks)}
	for _, t := range d.Tasks {
		p.RowsRead += t.RowsRead
		switch strings.ToUpper(strings.TrimSpace(t.Status)) {
		case "SUCCEEDED":
			p.Done++
		case "FAILED", "CANCELED":
			p.Done++
			p.Failed++
		case "RUNNING":
			p.Running++
		}
	}
	return p
}

// isTerminalRunStatus matches the terminal run states `run watch` stops on.
func isTerminalRunStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "SUCCEEDED", "FAILED", "CANCELED":
		return true
	}
	return false
}

// renderProgressBar draws a fixed-width bar such as ████████░░░░.
// With no total yet it draws an empty bar rather than dividing by zero.
func renderProgressBar(done, total, width int) string {
	if width <= 0 {
		return ""
	}
	filled := 0
	if total > 0 {
		filled = done * width / total
	}
	filled = max(0, min(filled, width))
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func progressPercent(done, total int) int {
	if total <= 0 {
		return 0
	}
	return done * 100 / total
}

// formatRunProgressLine renders the one-line /watch status, for example:
// RUNNING  ████████████░░░░ 74%  148/200 tasks · 3 running · 1.2M rows
func formatRunProgressLine(status string, p runProgress, barWidth int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-10s %s %3d%%  %d/%d tasks",
		strings.ToUpper(strings.TrimSpace(status)),
		renderProgressBar(p.Done, p.Total, barWidth),
		progressPercent(p.Done, p.Total),
		p.Done, p.Total)
	if p.Running > 0 {
		fmt.Fprintf(&b, " · %d running", p.Running)
	}
	if p.Failed > 0 {
		fmt.Fprintf(&b, " · %d failed", p.Failed)
	}
	fmt.Fprintf(&b, " · %s rows", humanCount(p.RowsRead))
	return b.String()
}

// humanCount abbreviates large counts: 950, 12.4k, 1.2M, 3.1B.
func humanCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	}
	return fmt.Sprintf("%d", n)
}
