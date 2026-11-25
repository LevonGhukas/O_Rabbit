package orabbitcli

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func menuLabels(items []tuiMenuItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Label
	}
	return out
}

func menuInserts(items []tuiMenuItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Insert
	}
	return out
}

func TestTUIMenuItemsCommands(t *testing.T) {
	if items := tuiMenuItems(""); items != nil {
		t.Fatal("empty line should have no menu (arrows browse history)")
	}
	if items := tuiMenuItems("runs"); items != nil {
		t.Fatal("menu appears only after a leading slash")
	}
	all := tuiMenuItems("/")
	if len(all) != len(tuiCommands) {
		t.Fatalf("'/' lists %d commands, want %d", len(all), len(tuiCommands))
	}
	got := menuInserts(tuiMenuItems("/wa"))
	if len(got) != 1 || got[0] != "/watch " {
		t.Fatalf("/wa inserts %v", got)
	}
}

func TestTUIMenuItemsRunFlags(t *testing.T) {
	items := tuiMenuItems("/run ")
	if len(items) != len(tuiCompletionSpecs["run"].Flags) {
		t.Fatalf("/run lists %d flags, want all %d", len(items), len(tuiCompletionSpecs["run"].Flags))
	}
	// Prefix filtering works with one or two dashes.
	for _, line := range []string{"/run --ice", "/run -ice"} {
		labels := menuLabels(tuiMenuItems(line))
		for _, l := range labels {
			if !strings.HasPrefix(l, "--ice") {
				t.Fatalf("%q offered %q", line, l)
			}
		}
		if len(labels) != 5 { // iceberg-engine, ice-uri, ice-table, ice-config, ice-bin
			t.Fatalf("%q offered %v", line, labels)
		}
	}
	// Choosing keeps earlier words and adds the flag.
	got := menuInserts(tuiMenuItems("/run --advanced --loc"))
	if len(got) != 1 || got[0] != "/run --advanced --local-workers " {
		t.Fatalf("inserts = %v", got)
	}
	// Flags already used are not offered again.
	for _, l := range menuLabels(tuiMenuItems("/run --advanced ")) {
		if l == "--advanced" {
			t.Fatal("--advanced offered twice")
		}
	}
}

func TestTUIMenuItemsFlagValues(t *testing.T) {
	got := menuLabels(tuiMenuItems("/run --local-workers "))
	if strings.Join(got, ",") != "true,false" {
		t.Fatalf("--local-workers values = %v", got)
	}
	got = menuInserts(tuiMenuItems("/run --iceberg-engine i"))
	if len(got) != 1 || got[0] != "/run --iceberg-engine ice " {
		t.Fatalf("--iceberg-engine i inserts = %v", got)
	}
	got = menuLabels(tuiMenuItems("/recover r1 --action "))
	if len(got) != 4 {
		t.Fatalf("--action values = %v", got)
	}
	// Free-form values (URLs, paths) get no menu: the user types them.
	if items := tuiMenuItems("/run --ice-uri "); items != nil {
		t.Fatalf("--ice-uri offered %v", menuLabels(items))
	}
	// With --flag=value the value is done; the next flags are offered.
	if items := tuiMenuItems("/run --iceberg-engine=ice "); len(items) == 0 {
		t.Fatal("no flags offered after --flag=value")
	}
	// After the value, flags come back.
	if items := tuiMenuItems("/run --iceberg-engine ice "); len(items) == 0 {
		t.Fatal("no flags offered after a flag value")
	}
}

func TestTUIMenuItemsSubcommands(t *testing.T) {
	got := menuLabels(tuiMenuItems("/stack "))
	if strings.Join(got, ",") != "start,stop,status" {
		t.Fatalf("/stack subcommands = %v", got)
	}
	got = menuLabels(tuiMenuItems("/stack st"))
	if len(got) != 3 {
		t.Fatalf("/stack st = %v", got)
	}
	// Positional components come before flags, and are not repeated.
	got = menuLabels(tuiMenuItems("/stack start "))
	if got[0] != "master" || got[1] != "worker" {
		t.Fatalf("/stack start = %v", got[:2])
	}
	got = menuLabels(tuiMenuItems("/stack start master "))
	if got[0] != "worker" {
		t.Fatalf("/stack start master = %v", got[:1])
	}
	got = menuLabels(tuiMenuItems("/maintenance submit --operation "))
	if strings.Join(got, ",") != "orphan_cleanup,compaction" {
		t.Fatalf("--operation values = %v", got)
	}
	if items := tuiMenuItems("/stack nope "); items != nil {
		t.Fatal("unknown subcommand should have no menu")
	}
}

func TestTUIMenuItemsNoSpec(t *testing.T) {
	for _, line := range []string{"/watch ", "/runs ", "/zzz ", `/recover r1 --reason "open`} {
		if items := tuiMenuItems(line); items != nil {
			t.Errorf("%q offered %v", line, menuLabels(items))
		}
	}
}

func TestRebuildTUILineRoundTrips(t *testing.T) {
	words := []string{"/recover", "r1", "--reason", `stale "commit" \ now`, ""}
	line := rebuildTUILine(words)
	got, err := splitTUIArgs(line)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if strings.Join(got, "|") != strings.Join(words, "|") {
		t.Fatalf("round trip = %q, want %q", got, words)
	}
}

// --- key handling ----------------------------------------------------------

func TestTUIArrowsNavigateMenu(t *testing.T) {
	m := newTestTUIModel("http://127.0.0.1:1")
	m.pushHistory("/status")
	m.input.SetValue("/")
	up := tea.KeyMsg{Type: tea.KeyUp}
	down := tea.KeyMsg{Type: tea.KeyDown}

	m, _ = update(t, m, down)
	m, _ = update(t, m, down)
	if m.menuIndex != 2 || m.input.Value() != "/" {
		t.Fatalf("after two downs: index=%d input=%q (history must not be used)", m.menuIndex, m.input.Value())
	}
	m, _ = update(t, m, up)
	m, _ = update(t, m, up)
	m, _ = update(t, m, up) // wraps to the last command
	if m.menuIndex != len(tuiCommands)-1 {
		t.Fatalf("wrap-around index = %d", m.menuIndex)
	}

	// Enter picks the highlighted command instead of running "/".
	m, cmd := update(t, m, tea.KeyMsg{Type: tea.KeyEnter})
	if want := "/" + tuiCommands[len(tuiCommands)-1].Name + " "; m.input.Value() != want || cmd != nil {
		t.Fatalf("enter inserted %q, want %q", m.input.Value(), want)
	}
}

func TestTUITabAndEnterPickFlag(t *testing.T) {
	m := newTestTUIModel("http://127.0.0.1:1")
	m.input.SetValue("/run --adv")
	m, _ = update(t, m, tea.KeyMsg{Type: tea.KeyTab})
	if m.input.Value() != "/run --advanced " {
		t.Fatalf("tab inserted %q", m.input.Value())
	}
	// A complete line runs on Enter even though the flag menu is open.
	if !strings.Contains(m.View(), "--count") {
		t.Fatal("flag menu should be visible after /run --advanced ")
	}
	if m.enterAppliesMenu() {
		t.Fatal("enter on a complete line should run it, not pick a flag")
	}
}

func TestTUIEnterCompletesPartialCommand(t *testing.T) {
	m := newTestTUIModel("http://127.0.0.1:1")
	m.input.SetValue("/sta")
	m, cmd := update(t, m, tea.KeyMsg{Type: tea.KeyEnter})
	if m.input.Value() != "/status " || cmd != nil {
		t.Fatalf("enter on /sta gave %q", m.input.Value())
	}
	// An exact command name runs right away.
	m.input.SetValue("/runs")
	if m.enterAppliesMenu() {
		t.Fatal("/runs should run, not complete")
	}
}

func TestTUIHistoryHidesMenu(t *testing.T) {
	m := newTestTUIModel("http://127.0.0.1:1")
	m.pushHistory("/runs")
	m.pushHistory("/status")
	up := tea.KeyMsg{Type: tea.KeyUp}

	m, _ = update(t, m, up)
	if m.input.Value() != "/status" || m.renderSuggestions() != "" {
		t.Fatal("a recalled entry should not open the menu")
	}
	m, _ = update(t, m, up) // keeps walking history, not the menu
	if m.input.Value() != "/runs" {
		t.Fatalf("second up = %q", m.input.Value())
	}
	// Typing turns the menu back on.
	m, _ = update(t, m, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(" ")})
	if m.browsingHistory {
		t.Fatal("editing should end history browsing")
	}
}

func TestTUIMenuScrollWindow(t *testing.T) {
	m := newTestTUIModel("http://127.0.0.1:1")
	m.input.SetValue("/run ")
	n := len(tuiMenuItems("/run "))
	for range n - 1 { // move to the last flag
		m, _ = update(t, m, tea.KeyMsg{Type: tea.KeyDown})
	}
	out := m.renderSuggestions()
	if rows := strings.Count(out, "\n"); rows > tuiMenuMaxRows+1 {
		t.Fatalf("menu has %d rows, cap is %d plus a footer", rows, tuiMenuMaxRows)
	}
	last := tuiCompletionSpecs["run"].Flags[n-1].Name
	if !strings.Contains(out, "--"+last) {
		t.Fatalf("highlighted last flag --%s is not visible:\n%s", last, out)
	}
}

// --- drift guard -------------------------------------------------------------

var helpFlagLine = regexp.MustCompile(`(?m)^  -([A-Za-z0-9-]+)`)

// helpFlags runs a CLI command with --help and returns the flags it lists.
func helpFlags(t *testing.T, run func(context.Context, []string) int) []string {
	t.Helper()
	_, stdout, _ := captureCommandOutput(t, func() int { return run(context.Background(), []string{"--help"}) })
	var names []string
	for _, m := range helpFlagLine.FindAllStringSubmatch(stdout, -1) {
		names = append(names, m[1])
	}
	sort.Strings(names)
	return names
}

func specFlagNames(flags []tuiFlag) []string {
	names := make([]string, len(flags))
	for i, f := range flags {
		names[i] = f.Name
	}
	sort.Strings(names)
	return names
}

// TestTUICompletionMatchesRealFlags fails when a CLI flag is added, removed
// or renamed without updating tuiCompletionSpecs, so the menu never offers a
// flag the command would reject, or misses a new one.
func TestTUICompletionMatchesRealFlags(t *testing.T) {
	sub := func(cmd, name string) []tuiFlag {
		s, ok := findTUISubcommand(tuiCompletionSpecs[cmd].Subcommands, name)
		if !ok {
			t.Fatalf("no spec for %s %s", cmd, name)
		}
		return s.Flags
	}
	cases := []struct {
		name  string
		run   func(context.Context, []string) int
		flags []tuiFlag
	}{
		{"run interactive", cmdRunInteractive, tuiCompletionSpecs["run"].Flags},
		{"run submit", cmdRunSubmit, tuiCompletionSpecs["submit"].Flags},
		{"run recover", cmdRunRecover, tuiCompletionSpecs["recover"].Flags},
		{"stack start", cmdStart, sub("stack", "start")},
		{"stack stop", cmdStop, sub("stack", "stop")},
		{"stack status", cmdStackStatus, sub("stack", "status")},
		{"maintenance submit", cmdMaintenanceSubmit, sub("maintenance", "submit")},
		{"maintenance show", cmdMaintenanceShow, sub("maintenance", "show")},
		{"maintenance list", cmdMaintenanceList, sub("maintenance", "list")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			real := helpFlags(t, tc.run)
			if len(real) == 0 {
				t.Fatal("help printed no flags; did the help format change?")
			}
			if got := specFlagNames(tc.flags); strings.Join(got, ",") != strings.Join(real, ",") {
				t.Fatalf("completion flags differ from the CLI\n spec: %v\n real: %v", got, real)
			}
		})
	}
}
