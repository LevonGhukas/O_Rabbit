package tui

import (
	"github.com/LevonGhukas/O_Rabbit/internal/orabbitcli"

	"fmt"
	"strings"
	"time"
)

// runProgress is a task-level summary of one run, used by /watch.
type runProgress struct {
	Total    int
	Done     int // tasks in a terminal state (succeeded, failed or canceled)
	Failed   int
	Canceled int
	Running  int
	RowsRead int64
}

func summarizeRunProgress(d orabbitcli.RunDetails) runProgress {
	p := runProgress{Total: len(d.Tasks)}
	for _, t := range d.Tasks {
		p.RowsRead += t.RowsRead
		switch strings.ToUpper(strings.TrimSpace(t.Status)) {
		case "SUCCEEDED":
			p.Done++
		case "FAILED":
			p.Done++
			p.Failed++
		case "CANCELED":
			p.Done++
			p.Canceled++
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
	if p.Canceled > 0 {
		fmt.Fprintf(&b, " · %d canceled", p.Canceled)
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

// Iceberg registration ------------------------------------------------------

// registrationTimeout bounds how long /watch waits for a registration to be
// recorded after a successful export, so a master that never queues one
// does not keep the shell watching forever.
const registrationTimeout = 60 * time.Second

// registrationFinal says whether a catalog status is final for /watch.
// RETRY_REQUIRED and RECONCILING keep the master working, so the watch
// continues; BLOCKED waits on an earlier run and is reported as such.
func registrationFinal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "REGISTERED", "FAILED", "QUARANTINED", "CANCELED", "BLOCKED":
		return true
	}
	return false
}

// formatRegistrationLine is the Iceberg line under a finished run.
func formatRegistrationLine(r orabbitcli.RunState, table string) string {
	status := strings.ToUpper(strings.TrimSpace(r.CatalogStatus))
	target := ""
	if table != "" {
		target = " " + table
	}
	switch status {
	case "REGISTERED":
		return styleOK.Render("✓ Iceberg: registered" + target)
	case "":
		return styleWarn.Render("! Iceberg: no registration was recorded for this run — check the master log for \"iceberg registration\"")
	}
	line := "Iceberg: registration " + status + target
	if r.RegistrationLastErrorClass != "" {
		line += " (" + r.RegistrationLastErrorClass + ")"
	}
	if r.RegistrationBlockedBy != "" {
		line += ", waiting on registration " + r.RegistrationBlockedBy
	}
	style := styleErr
	if status == "BLOCKED" {
		style = styleWarn
	}
	return style.Render("✗ "+line) + "\n" + styleDim.Render(
		"  the Parquet files are in S3; /diagnose "+r.ID+" explains it, and /recover "+r.ID+
			" --action registration_retry --reason \"…\" retries it after the cause is fixed")
}
