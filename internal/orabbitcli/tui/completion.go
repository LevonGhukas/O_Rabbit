package tui

import (
	"strings"
)

// flagSpec describes one flag of a command the shell forwards to the CLI.
// Value is a placeholder for flags that take a value ("" for booleans), and
// Values lists the accepted choices when the flag is an enum.
type flagSpec struct {
	Name    string
	Value   string
	Summary string
	Values  []string
}

// subcommandSpec is a second command word, such as `stack start`.
type subcommandSpec struct {
	Name    string
	Summary string
	// Hidden subcommands work and complete their flags, but are not listed.
	Hidden bool
	Flags  []flagSpec
	// Choices are positional words the subcommand accepts, e.g. master/worker.
	Choices []string
}

// commandSpec is the completion data for one slash command. The flag lists
// mirror the real flag sets in main.go, process.go and maintenance.go;
// tui_completion_test.go fails if they drift apart.
type commandSpec struct {
	Flags       []flagSpec
	Subcommands []subcommandSpec
}

var (
	logLevels  = []string{"DEBUG", "INFO", "WARN", "ERROR"}
	logFormats = []string{"json", "text"}
)

var completionSpecs = map[string]commandSpec{
	"runs":     {Flags: []flagSpec{{Name: "job", Value: "id|name", Summary: "Only this job's runs"}}},
	"diagnose": {Flags: []flagSpec{{Name: "json", Summary: "Print the master's raw diagnosis JSON"}}},
	"workers":  {Flags: []flagSpec{{Name: "all", Summary: "Include workers that stopped heartbeating"}}},
	// Every /run form opens the wizard, which takes no flags.
	"run": {Subcommands: []subcommandSpec{
		{Name: "interactive", Summary: "Step-by-step run wizard (same as plain /run)"},
	}},
	"submit": {Flags: []flagSpec{
		{Name: "file", Value: "path", Summary: "YAML/JSON run spec (a bare first argument works too)"},
		{Name: "master-http", Value: "url", Summary: "Master HTTP base URL (the shell passes its own)"},
		{Name: "quiet", Summary: "Suppress non-essential output"},
		{Name: "verbose", Summary: "Show additional progress details"},
	}},
	"recover": {Flags: []flagSpec{
		{Name: "action", Value: "action", Summary: "Recovery action", Values: []string{"reconcile_commit", "registration_retry", "reconciliation_retry", "acknowledge_quarantine"}},
		{Name: "reason", Value: "text", Summary: "Operator reason recorded in the audit log (quote it)"},
		{Name: "master-http", Value: "url", Summary: "Master HTTP base URL (the shell passes its own)"},
	}},
	"stack": {Subcommands: []subcommandSpec{
		{Name: "start", Summary: "Start local master and/or workers", Choices: []string{"master", "worker"}, Flags: []flagSpec{
			{Name: "count", Value: "n", Summary: "Worker process count"},
			{Name: "master-addr", Value: "addr", Summary: "Master gRPC address for workers"},
			{Name: "insecure", Value: "bool", Summary: "Disable gRPC TLS (dev)", Values: []string{"true", "false"}},
			{Name: "http-addr", Value: "addr", Summary: "HTTP listen address"},
			{Name: "grpc-addr", Value: "addr", Summary: "gRPC listen address"},
			{Name: "db", Value: "path", Summary: "SQLite DB path"},
			{Name: "gocache", Value: "dir", Summary: "Managed local daemon runtime dir"},
			{Name: "master-bin", Value: "path", Summary: "Path to the orabbit-master binary"},
			{Name: "worker-bin", Value: "path", Summary: "Path to the orabbit-worker binary"},
			{Name: "worker-log-level", Value: "level", Summary: "Log level for local workers", Values: logLevels},
			{Name: "worker-log-format", Value: "format", Summary: "Log format for local workers", Values: logFormats},
			{Name: "tls-cert", Value: "file", Summary: "Master gRPC TLS cert"},
			{Name: "tls-key", Value: "file", Summary: "Master gRPC TLS key"},
			{Name: "tls-ca", Value: "file", Summary: "Worker CA cert for master gRPC TLS"},
			{Name: "tls-server-name", Value: "name", Summary: "Expected server name for worker gRPC TLS"},
			{Name: "worker-enrollment-token", Value: "token", Summary: "Enrollment token (prefer ORABBIT_WORKER_ENROLLMENT_TOKEN)"},
			{Name: "quiet", Summary: "Suppress non-essential output"},
			{Name: "verbose", Summary: "Show additional progress details"},
		}},
		{Name: "stop", Summary: "Stop local master/worker processes", Flags: []flagSpec{
			{Name: "dry-run", Summary: "Preview the matched targets; don't signal"},
			{Name: "force", Summary: "Send SIGKILL (immediate)"},
			{Name: "all", Summary: "Also stop processes not started with -gocache (Unix only)"},
			{Name: "gocache", Value: "dir", Summary: "Managed runtime dir"},
			{Name: "grpc-addr", Value: "addr", Summary: "Master gRPC address to match with -all"},
			{Name: "http-addr", Value: "addr", Summary: "Master HTTP addr to match with -all"},
		}},
		{Name: "status", Summary: "List local/remote master and worker status", Flags: []flagSpec{
			{Name: "http-base", Value: "url", Summary: "Master HTTP base to query"},
			{Name: "json", Summary: "Print JSON"},
			{Name: "v", Summary: "Verbose output"},
			{Name: "gocache", Value: "dir", Summary: "Managed runtime dir to inspect first"},
		}},
	}},
	"maintenance": {Subcommands: []subcommandSpec{
		{Name: "submit", Summary: "Start orphan cleanup or compaction", Flags: []flagSpec{
			{Name: "job", Value: "job-id", Summary: "Job whose Iceberg table to maintain (required)"},
			{Name: "operation", Value: "op", Summary: "Operation (required)", Values: []string{"orphan_cleanup", "compaction"}},
			{Name: "execute", Summary: "Really delete/compact; without it this is a dry run"},
			{Name: "min-age-hours", Value: "hours", Summary: "orphan_cleanup: keep files younger than this"},
			{Name: "target-file-bytes", Value: "bytes", Summary: "compaction: output file size"},
			{Name: "small-file-bytes", Value: "bytes", Summary: "compaction: rewrite files below this size"},
			{Name: "min-input-files", Value: "n", Summary: "compaction: files a partition needs first"},
			{Name: "max-input-files", Value: "n", Summary: "compaction: most files one operation rewrites"},
			{Name: "master-http", Value: "url", Summary: "Master HTTP base URL (the shell passes its own)"},
		}},
		{Name: "show", Summary: "Show one operation and its report", Flags: []flagSpec{
			{Name: "json", Summary: "Print the full operation and report as JSON"},
			{Name: "master-http", Value: "url", Summary: "Master HTTP base URL (the shell passes its own)"},
		}},
		{Name: "list", Summary: "List maintenance operations", Flags: []flagSpec{
			{Name: "job", Value: "job-id", Summary: "Only this job's operations"},
			{Name: "master-http", Value: "url", Summary: "Master HTTP base URL (the shell passes its own)"},
		}},
	}},
}

// menuItem is one row of the completion menu. Insert is the whole input
// line after choosing the item, so applying it is a single SetValue.
type menuItem struct {
	Label   string
	Summary string
	Insert  string
}

// menuItemsFor computes the completion menu for the current input line:
// commands while the first word is typed, then subcommands, positional
// choices, flags, and finally the accepted values of an enum flag.
func menuItemsFor(line string) []menuItem {
	trimmed := strings.TrimLeft(line, " ")
	if !strings.HasPrefix(trimmed, "/") {
		return nil
	}
	words, err := splitArgs(trimmed)
	if err != nil || len(words) == 0 {
		return nil // an open quote: the user is typing a value
	}
	endsWithSpace := strings.HasSuffix(trimmed, " ") || strings.HasSuffix(trimmed, "\t")

	// The word being typed ("" right after a space) and everything before it.
	partial := ""
	done := words
	if !endsWithSpace {
		partial = words[len(words)-1]
		done = words[:len(words)-1]
	}
	prefix := rebuildLine(done)

	if len(done) == 0 {
		var items []menuItem
		for _, c := range matchCommands(strings.TrimPrefix(partial, "/")) {
			items = append(items, menuItem{Label: c.Usage, Summary: c.Summary, Insert: "/" + c.Name + " "})
		}
		return items
	}

	cmd, ok := lookupCommand(strings.TrimPrefix(done[0], "/"))
	if !ok {
		return nil
	}
	spec, ok := completionSpecs[cmd.Name]
	if !ok {
		return nil
	}

	flags := spec.Flags
	rest := done[1:]
	var choices []string
	if len(spec.Subcommands) > 0 {
		if len(rest) == 0 {
			var items []menuItem
			for _, s := range spec.Subcommands {
				if !s.Hidden && strings.HasPrefix(s.Name, partial) {
					items = append(items, menuItem{Label: s.Name, Summary: s.Summary, Insert: prefix + s.Name + " "})
				}
			}
			return items
		}
		sub, ok := findSubcommand(spec.Subcommands, rest[0])
		if !ok {
			return nil
		}
		flags, choices, rest = sub.Flags, sub.Choices, rest[1:]
	}

	// A value flag that is still waiting for its value: offer its choices,
	// or nothing at all for free-form values like paths and URLs.
	if n := len(rest); n > 0 {
		if f, ok := findFlag(flags, rest[n-1]); ok && f.Value != "" && !strings.Contains(rest[n-1], "=") {
			var items []menuItem
			for _, v := range f.Values {
				if strings.HasPrefix(v, partial) {
					items = append(items, menuItem{Label: v, Summary: "value for --" + f.Name, Insert: prefix + v + " "})
				}
			}
			return items
		}
	}

	var items []menuItem
	if !strings.HasPrefix(partial, "-") {
		for _, c := range choices {
			if strings.HasPrefix(c, partial) && !containsWord(rest, c) {
				items = append(items, menuItem{Label: c, Summary: "component", Insert: prefix + c + " "})
			}
		}
	}
	name := strings.TrimLeft(partial, "-")
	if partial == "" || strings.HasPrefix(partial, "-") {
		for _, f := range flags {
			if !strings.HasPrefix(f.Name, name) || hasFlag(rest, f.Name) {
				continue
			}
			label := "--" + f.Name
			if f.Value != "" {
				label += " <" + f.Value + ">"
			}
			items = append(items, menuItem{Label: label, Summary: f.Summary, Insert: prefix + "--" + f.Name + " "})
		}
	}
	return items
}

func findSubcommand(subs []subcommandSpec, name string) (subcommandSpec, bool) {
	for _, s := range subs {
		if s.Name == name {
			return s, true
		}
	}
	return subcommandSpec{}, false
}

// findFlag resolves a typed word such as "--action" or "-action=x".
func findFlag(flags []flagSpec, word string) (flagSpec, bool) {
	if !strings.HasPrefix(word, "-") {
		return flagSpec{}, false
	}
	name, _, _ := strings.Cut(strings.TrimLeft(word, "-"), "=")
	for _, f := range flags {
		if f.Name == name {
			return f, true
		}
	}
	return flagSpec{}, false
}

func containsWord(words []string, w string) bool {
	for _, x := range words {
		if x == w {
			return true
		}
	}
	return false
}

// rebuildLine joins finished words back into a line ending in a space,
// quoting any word that needs it so the line parses to the same words.
func rebuildLine(words []string) string {
	if len(words) == 0 {
		return ""
	}
	var b strings.Builder
	for _, w := range words {
		if w == "" || strings.ContainsAny(w, " \t\"'\\") {
			w = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(w) + `"`
		}
		b.WriteString(w)
		b.WriteByte(' ')
	}
	return b.String()
}
