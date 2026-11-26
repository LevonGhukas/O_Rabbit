package tui

import (
	"io"
	"os"

	"github.com/LevonGhukas/O_Rabbit/internal/orabbitcli"

	"regexp"
	"sort"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func menuLabels(items []menuItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Label
	}
	return out
}

func menuInserts(items []menuItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Insert
	}
	return out
}

func TestMenuItemsCommands(t *testing.T) {
	if items := menuItemsFor(""); items != nil {
		t.Fatal("empty line should have no menu (arrows browse history)")
	}
	if items := menuItemsFor("runs"); items != nil {
		t.Fatal("menu appears only after a leading slash")
	}
	all := menuItemsFor("/")
	if len(all) != len(commands) {
		t.Fatalf("'/' lists %d commands, want %d", len(all), len(commands))
	}
	got := menuInserts(menuItemsFor("/wa"))
	if len(got) != 1 || got[0] != "/watch " {
		t.Fatalf("/wa inserts %v", got)
	}
}

func TestMenuItemsSubcommandFlags(t *testing.T) {
	items := menuItemsFor("/stack start --")
	if want := len(subFlags(t, "stack", "start")); len(items) != want {
		t.Fatalf("/stack start -- lists %d flags, want %d", len(items), want)
	}
	// Prefix filtering works with one or two dashes.
	for _, line := range []string{"/stack start --worker-", "/stack start -worker-"} {
		labels := menuLabels(menuItemsFor(line))
		for _, l := range labels {
			if !strings.HasPrefix(l, "--worker-") {
				t.Fatalf("%q offered %q", line, l)
			}
		}
		if len(labels) != 4 { // worker-bin, worker-log-level, worker-log-format, worker-enrollment-token
			t.Fatalf("%q offered %v", line, labels)
		}
	}
	// Choosing keeps earlier words and adds the flag.
	got := menuInserts(menuItemsFor("/stack start --quiet --verb"))
	if len(got) != 1 || got[0] != "/stack start --quiet --verbose " {
		t.Fatalf("inserts = %v", got)
	}
	// Flags already used are not offered again.
	for _, l := range menuLabels(menuItemsFor("/stack start --quiet ")) {
		if l == "--quiet" {
			t.Fatal("--quiet offered twice")
		}
	}
}

func TestMenuItemsFlagValues(t *testing.T) {
	got := menuLabels(menuItemsFor("/stack start --insecure "))
	if strings.Join(got, ",") != "true,false" {
		t.Fatalf("--insecure values = %v", got)
	}
	got = menuInserts(menuItemsFor("/stack start --worker-log-format t"))
	if len(got) != 1 || got[0] != "/stack start --worker-log-format text " {
		t.Fatalf("--worker-log-format t inserts = %v", got)
	}
	got = menuLabels(menuItemsFor("/recover r1 --action "))
	if len(got) != 4 {
		t.Fatalf("--action values = %v", got)
	}
	// Free-form values (URLs, paths) get no menu: the user types them.
	if items := menuItemsFor("/stack start --db "); items != nil {
		t.Fatalf("--db offered %v", menuLabels(items))
	}
	// With --flag=value the value is done; the next flags are offered.
	if items := menuItemsFor("/stack start --insecure=true "); len(items) == 0 {
		t.Fatal("no flags offered after --flag=value")
	}
	// After the value, flags come back.
	if items := menuItemsFor("/stack start --insecure true "); len(items) == 0 {
		t.Fatal("no flags offered after a flag value")
	}
}

func TestMenuItemsSubcommands(t *testing.T) {
	got := menuLabels(menuItemsFor("/stack "))
	if strings.Join(got, ",") != "start,stop,status" {
		t.Fatalf("/stack subcommands = %v", got)
	}
	got = menuLabels(menuItemsFor("/stack st"))
	if len(got) != 3 {
		t.Fatalf("/stack st = %v", got)
	}
	// Positional components come before flags, and are not repeated.
	got = menuLabels(menuItemsFor("/stack start "))
	if got[0] != "master" || got[1] != "worker" {
		t.Fatalf("/stack start = %v", got[:2])
	}
	got = menuLabels(menuItemsFor("/stack start master "))
	if got[0] != "worker" {
		t.Fatalf("/stack start master = %v", got[:1])
	}
	got = menuLabels(menuItemsFor("/maintenance submit --operation "))
	if strings.Join(got, ",") != "orphan_cleanup,compaction" {
		t.Fatalf("--operation values = %v", got)
	}
	if items := menuItemsFor("/stack nope "); items != nil {
		t.Fatal("unknown subcommand should have no menu")
	}
}

func TestMenuItemsNoSpec(t *testing.T) {
	for _, line := range []string{"/watch ", "/show ", "/zzz ", `/recover r1 --reason "open`} {
		if items := menuItemsFor(line); items != nil {
			t.Errorf("%q offered %v", line, menuLabels(items))
		}
	}
}

func TestRebuildLineRoundTrips(t *testing.T) {
	words := []string{"/recover", "r1", "--reason", `stale "commit" \ now`, ""}
	line := rebuildLine(words)
	got, err := splitArgs(line)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if strings.Join(got, "|") != strings.Join(words, "|") {
		t.Fatalf("round trip = %q, want %q", got, words)
	}
}

// --- key handling ----------------------------------------------------------

func TestArrowsNavigateMenu(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
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
	if m.menuIndex != len(commands)-1 {
		t.Fatalf("wrap-around index = %d", m.menuIndex)
	}

	// Enter picks the highlighted command instead of running "/".
	m, cmd := update(t, m, tea.KeyMsg{Type: tea.KeyEnter})
	if want := "/" + commands[len(commands)-1].Name + " "; m.input.Value() != want || cmd != nil {
		t.Fatalf("enter inserted %q, want %q", m.input.Value(), want)
	}
}

func TestTabAndEnterPickFlag(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m.input.SetValue("/stack start --verb")
	m, _ = update(t, m, tea.KeyMsg{Type: tea.KeyTab})
	if m.input.Value() != "/stack start --verbose " {
		t.Fatalf("tab inserted %q", m.input.Value())
	}
	// A complete line runs on Enter even though the flag menu is open.
	if !strings.Contains(m.View(), "--count") {
		t.Fatal("flag menu should be visible after /stack start --verbose ")
	}
	if m.enterAppliesMenu() {
		t.Fatal("enter on a complete line should run it, not pick a flag")
	}
}

func TestEnterCompletesPartialCommand(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
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

func TestHistoryHidesMenu(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
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

func TestMenuScrollWindow(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m.input.SetValue("/stack start --")
	n := len(menuItemsFor("/stack start --"))
	for range n - 1 { // move to the last flag
		m, _ = update(t, m, tea.KeyMsg{Type: tea.KeyDown})
	}
	out := m.renderSuggestions()
	if rows := strings.Count(out, "\n"); rows > menuMaxRows+1 {
		t.Fatalf("menu has %d rows, cap is %d plus a footer", rows, menuMaxRows)
	}
	flags := subFlags(t, "stack", "start")
	last := flags[len(flags)-1].Name
	if !strings.Contains(out, "--"+last) {
		t.Fatalf("highlighted last flag --%s is not visible:\n%s", last, out)
	}
}

// --- drift guard -------------------------------------------------------------

var helpFlagLine = regexp.MustCompile(`(?m)^  -([A-Za-z0-9-]+)`)

// helpFlags runs `orabbit-client <command...> --help` through the real CLI
// and returns the flags it lists.
func helpFlags(t *testing.T, command string) []string {
	t.Helper()
	stdout := captureStdout(t, func() {
		orabbitcli.Main(append(strings.Fields(command), "--help"))
	})
	var names []string
	for _, m := range helpFlagLine.FindAllStringSubmatch(stdout, -1) {
		names = append(names, m[1])
	}
	sort.Strings(names)
	return names
}

func specFlagNames(flags []flagSpec) []string {
	names := make([]string, len(flags))
	for i, f := range flags {
		names[i] = f.Name
	}
	sort.Strings(names)
	return names
}

// TestCompletionMatchesRealFlags fails when a CLI flag is added, removed
// or renamed without updating completionSpecs, so the menu never offers a
// flag the command would reject, or misses a new one.
func TestCompletionMatchesRealFlags(t *testing.T) {
	sub := func(cmd, name string) []flagSpec {
		s, ok := findSubcommand(completionSpecs[cmd].Subcommands, name)
		if !ok {
			t.Fatalf("no spec for %s %s", cmd, name)
		}
		return s.Flags
	}
	cases := []struct {
		name  string
		flags []flagSpec
	}{
		{"run submit", completionSpecs["submit"].Flags},
		{"run recover", completionSpecs["recover"].Flags},
		{"stack start", sub("stack", "start")},
		{"stack stop", sub("stack", "stop")},
		{"stack status", sub("stack", "status")},
		{"maintenance submit", sub("maintenance", "submit")},
		{"maintenance show", sub("maintenance", "show")},
		{"maintenance list", sub("maintenance", "list")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			real := helpFlags(t, tc.name)
			if len(real) == 0 {
				t.Fatal("help printed no flags; did the help format change?")
			}
			if got := specFlagNames(tc.flags); strings.Join(got, ",") != strings.Join(real, ",") {
				t.Fatalf("completion flags differ from the CLI\n spec: %v\n real: %v", got, real)
			}
		})
	}
}

func TestRunInteractiveChoice(t *testing.T) {
	// Only "interactive" is offered; the old "classic" also opens the wizard.
	got := menuLabels(menuItemsFor("/run "))
	if strings.Join(got, ",") != "interactive" {
		t.Fatalf("/run menu = %v, want only interactive", got)
	}
	got = menuInserts(menuItemsFor("/run int"))
	if len(got) != 1 || got[0] != "/run interactive " {
		t.Fatalf("/run int inserts %v", got)
	}
	if items := menuItemsFor("/run interactive "); items != nil {
		t.Fatalf("the wizard takes no flags, menu offered %v", menuLabels(items))
	}
	if items := menuItemsFor("/run classic "); items != nil {
		t.Fatalf("/run classic should offer no flags, got %v", menuLabels(items))
	}
}

func TestGhostSuggestion(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m, _ = update(t, m, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/wa")})
	if got := m.input.AvailableSuggestions(); len(got) != 1 || got[0] != "/watch " {
		t.Fatalf("ghost suggestion = %v, want /watch", got)
	}
	m, _ = update(t, m, tea.KeyMsg{Type: tea.KeyDown})
	m, _ = update(t, m, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("z")})
	if got := m.input.AvailableSuggestions(); len(got) != 0 {
		t.Fatalf("no match should clear the ghost, got %v", got)
	}
}

// subFlags returns the completion flags of a subcommand such as `stack start`.
func subFlags(t *testing.T, cmd, name string) []flagSpec {
	t.Helper()
	s, ok := findSubcommand(completionSpecs[cmd].Subcommands, name)
	if !ok {
		t.Fatalf("no subcommand %s %s", cmd, name)
	}
	return s.Flags
}

// captureStdout returns what fn writes to os.Stdout.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	defer func() { os.Stdout = old }()
	fn()
	_ = w.Close()
	return <-done
}
