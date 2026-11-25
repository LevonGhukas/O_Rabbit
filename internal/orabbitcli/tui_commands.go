package orabbitcli

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// tuiCommand describes one slash command of the interactive shell.
// The registry below is plain data so help, completion and "did you mean"
// suggestions all stay in sync automatically.
type tuiCommand struct {
	Name    string
	Usage   string
	Summary string
	Aliases []string
	// Group sorts /help into sections.
	Group string
	// Details and Examples are shown by `/help <cmd>` and `/<cmd> --help`.
	Details  string
	Examples []string
}

const (
	groupRuns    = "Runs"
	groupInspect = "Inspect"
	groupOps     = "Operations"
	groupShell   = "Shell"
)

var tuiHelpGroups = []string{groupRuns, groupInspect, groupOps, groupShell}

var tuiCommands = []tuiCommand{
	{Name: "run", Group: groupRuns, Usage: "/run", Summary: "Start a run with the step-by-step wizard",
		Details:  "Asks for the source, data, storage, destination and optional advanced settings, prints the configuration, submits it and follows the run. Esc closes the wizard and keeps your answers for the next /run.",
		Examples: []string{"/run"}},
	{Name: "submit", Group: groupRuns, Usage: "/submit <spec-file>", Summary: "Submit a run from a YAML/JSON spec",
		Details:  "Submits a spec file, such as one the wizard saved. Relative paths are resolved from the directory the shell was started in.",
		Examples: []string{"/submit orders.run.yaml"}},
	{Name: "watch", Group: groupRuns, Usage: "/watch <run-id>", Summary: "Follow a run with a progress bar",
		Details: "Polls the run until it finishes. Esc stops watching; the run keeps going.", Examples: []string{"/watch 9b5adc20334ecbfbef769f9a3fc55769"}},
	{Name: "cancel", Group: groupRuns, Usage: "/cancel <run-id>", Summary: "Cancel a run (asks for confirmation)"},
	{Name: "runs", Group: groupInspect, Usage: "/runs [limit] [--job <id|name>]", Summary: "List recent runs (default 10)",
		Details:  "Newest first, with the job name and a one-line error for failed runs. --job keeps only one job's runs (searched among the last 200).",
		Examples: []string{"/runs", "/runs 25", "/runs --job orders"}},
	{Name: "show", Group: groupInspect, Usage: "/show <run-id>", Summary: "Show a run's status, tasks and errors"},
	{Name: "diagnose", Group: groupInspect, Usage: "/diagnose <run-id|job> [--json]", Summary: "Explain why a run failed or is stuck",
		Details:  "Accepts a run ID, a job ID or a job name; for a job it diagnoses the job's latest run. Shows task counts, blocking tasks, Iceberg registration and the suggested next action. --json prints the master's raw diagnosis.",
		Examples: []string{"/diagnose 0067ecf311344cb9ac2de6bdebde4c07", "/diagnose 5d0e122da046882bbfbd6f5c95632892", "/diagnose orders --json"}},
	{Name: "jobs", Group: groupInspect, Usage: "/jobs", Summary: "List jobs (each run belongs to a job)"},
	{Name: "workers", Group: groupInspect, Usage: "/workers [--all]", Summary: "List active workers (--all includes stale ones)"},
	{Name: "status", Group: groupInspect, Usage: "/status", Summary: "Check master health and active workers"},
	{Name: "recover", Group: groupOps, Usage: "/recover <run-id> --action <a> --reason <r>", Summary: "Run an operator recovery action",
		Details:  "Use the action /diagnose suggests. The reason is stored in the audit log; quote it when it has spaces.",
		Examples: []string{`/recover <run-id> --action registration_retry --reason "catalog was down"`}},
	{Name: "maintenance", Group: groupOps, Usage: "/maintenance <submit|show|list> [flags]", Summary: "Iceberg orphan cleanup and compaction",
		Details:  "submit is a dry run unless --execute is given. list prints nothing but its header when the job has no operations yet.",
		Examples: []string{"/maintenance submit --job <job-id> --operation orphan_cleanup", "/maintenance list --job <job-id>", "/maintenance show <operation-id>"}},
	{Name: "stack", Group: groupOps, Usage: "/stack <start|stop|status> [flags]", Summary: "Manage local master/worker processes",
		Examples: []string{"/stack start master worker --count 2", "/stack status", "/stack stop --dry-run"}},
	{Name: "connect", Group: groupShell, Usage: "/connect <master-http-url>", Summary: "Switch to another master", Examples: []string{"/connect http://10.0.0.5:9100"}},
	{Name: "help", Group: groupShell, Usage: "/help [command]", Summary: "Show commands, or one command's details", Aliases: []string{"?"}},
	{Name: "clear", Group: groupShell, Usage: "/clear", Summary: "Clear the screen"},
	{Name: "exit", Group: groupShell, Usage: "/exit", Summary: "Leave the shell (Ctrl+D also works)", Aliases: []string{"quit", "q"}},
}

// wantsHelp reports whether the arguments ask for help instead of running.
func wantsHelp(args []string) bool {
	for _, a := range args {
		switch a {
		case "-h", "--help", "-help":
			return true
		}
	}
	return len(args) == 1 && args[0] == "help"
}

// parseRunsArgs reads `/runs [limit] [--job <id|name>]`.
func parseRunsArgs(args []string) (limit int, job string, err error) {
	limit = tuiDefaultRuns
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--job" || a == "-job":
			if i+1 >= len(args) {
				return 0, "", errors.New("--job needs a job ID or name")
			}
			i++
			job = args[i]
		case strings.HasPrefix(a, "--job=") || strings.HasPrefix(a, "-job="):
			_, job, _ = strings.Cut(a, "=")
		default:
			n, convErr := strconv.Atoi(a)
			if convErr != nil {
				return 0, "", fmt.Errorf("unexpected %q — usage: /runs [limit] [--job <id|name>]", a)
			}
			if n <= 0 {
				return 0, "", errors.New("limit must be a positive number")
			}
			limit = n
		}
	}
	return limit, job, nil
}

func renderTUIHelp() string {
	var b strings.Builder
	for gi, g := range tuiHelpGroups {
		if gi > 0 {
			b.WriteString("\n")
		}
		b.WriteString(tuiStyleTitle.Render(g) + "\n")
		for _, c := range tuiCommands {
			if c.Group == g {
				fmt.Fprintf(&b, "  %s %s\n", tuiStyleCmd.Render(fmt.Sprintf("%-42s", c.Usage)), tuiStyleDim.Render(c.Summary))
			}
		}
	}
	b.WriteString("\n" + tuiStyleDim.Render("/help <command> or /<command> --help shows flags and examples"))
	b.WriteString("\n" + tuiStyleDim.Render("Keys: Tab completes · ↑↓ menu/history · Esc stops the current activity · Ctrl+D exits"))
	return b.String()
}

// renderTUICommandHelp shows one command: what it does, its subcommands and
// flags (from the completion specs, so they match the real CLI flags) and
// examples.
func renderTUICommandHelp(c tuiCommand) string {
	var b strings.Builder
	b.WriteString(tuiStyleTitle.Render(c.Usage) + "\n")
	b.WriteString("  " + c.Summary + "\n")
	if c.Details != "" {
		b.WriteString("  " + tuiStyleDim.Render(c.Details) + "\n")
	}
	if len(c.Aliases) > 0 {
		b.WriteString("  " + tuiStyleDim.Render("aliases: /"+strings.Join(c.Aliases, ", /")) + "\n")
	}
	spec := tuiCompletionSpecs[c.Name]
	writeFlags := func(indent string, flags []tuiFlag) {
		for _, f := range flags {
			label := "--" + f.Name
			if f.Value != "" {
				label += " <" + f.Value + ">"
			}
			summary := f.Summary
			if len(f.Values) > 0 {
				summary += " [" + strings.Join(f.Values, "|") + "]"
			}
			fmt.Fprintf(&b, "%s%s %s\n", indent, tuiStyleCmd.Render(fmt.Sprintf("%-30s", label)), tuiStyleDim.Render(summary))
		}
	}
	if len(spec.Flags) > 0 {
		b.WriteString("\n" + tuiStyleTitle.Render("Flags") + "\n")
		writeFlags("  ", spec.Flags)
	}
	for _, s := range spec.Subcommands {
		if s.Hidden {
			continue
		}
		head := "/" + c.Name + " " + s.Name
		if len(s.Choices) > 0 {
			head += " [" + strings.Join(s.Choices, "] [") + "]"
		}
		fmt.Fprintf(&b, "\n%s  %s\n", tuiStyleTitle.Render(head), tuiStyleDim.Render(s.Summary))
		writeFlags("  ", s.Flags)
	}
	if len(c.Examples) > 0 {
		b.WriteString("\n" + tuiStyleTitle.Render("Examples") + "\n")
		for _, e := range c.Examples {
			b.WriteString("  " + tuiStyleCmd.Render(e) + "\n")
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

// lookupTUICommand resolves a command name or alias.
func lookupTUICommand(name string) (tuiCommand, bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	for _, c := range tuiCommands {
		if c.Name == name {
			return c, true
		}
		for _, a := range c.Aliases {
			if a == name {
				return c, true
			}
		}
	}
	return tuiCommand{}, false
}

// matchTUICommands returns the commands whose name starts with prefix,
// in registry order. An empty prefix matches everything.
func matchTUICommands(prefix string) []tuiCommand {
	prefix = strings.ToLower(strings.TrimSpace(prefix))
	var out []tuiCommand
	for _, c := range tuiCommands {
		if strings.HasPrefix(c.Name, prefix) {
			out = append(out, c)
		}
	}
	return out
}

// suggestTUICommand returns the closest known command for a typo, if any is
// close enough (edit distance <= 2) to be a plausible intent.
func suggestTUICommand(name string) (string, bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "", false
	}
	type candidate struct {
		name string
		dist int
	}
	var cands []candidate
	for _, c := range tuiCommands {
		for _, n := range append([]string{c.Name}, c.Aliases...) {
			if d := levenshtein(name, n); d <= 2 && d < len(n) {
				cands = append(cands, candidate{name: c.Name, dist: d})
			}
		}
	}
	if len(cands) == 0 {
		return "", false
	}
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].dist < cands[j].dist })
	return cands[0].name, true
}

// levenshtein computes the edit distance between two strings (insertions,
// deletions and substitutions each cost 1), using two rolling rows.
func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	curr := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		curr[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			curr[j] = min(prev[j]+1, curr[j-1]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}
	return prev[len(rb)]
}

var errUnterminatedQuote = errors.New("unterminated quote")

// splitTUIArgs splits a command line into words like a minimal shell:
// whitespace separates words, and single or double quotes group words so
// `--reason "stale commit"` stays one argument. Backslash escapes the next
// character outside single quotes.
func splitTUIArgs(line string) ([]string, error) {
	var (
		args    []string
		cur     strings.Builder
		inWord  bool
		quote   rune
		escaped bool
	)
	for _, r := range line {
		switch {
		case escaped:
			cur.WriteRune(r)
			escaped = false
		case r == '\\' && quote != '\'':
			escaped = true
			inWord = true
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '"' || r == '\'':
			quote = r
			inWord = true
		case r == ' ' || r == '\t':
			if inWord {
				args = append(args, cur.String())
				cur.Reset()
				inWord = false
			}
		default:
			cur.WriteRune(r)
			inWord = true
		}
	}
	if quote != 0 || escaped {
		return nil, errUnterminatedQuote
	}
	if inWord {
		args = append(args, cur.String())
	}
	return args, nil
}

// parseTUIInput turns a shell line into a command name and its arguments.
// The leading "/" is optional so `watch abc` works as well as `/watch abc`.
func parseTUIInput(line string) (name string, args []string, err error) {
	words, err := splitTUIArgs(strings.TrimSpace(line))
	if err != nil {
		return "", nil, err
	}
	if len(words) == 0 {
		return "", nil, nil
	}
	return strings.ToLower(strings.TrimPrefix(words[0], "/")), words[1:], nil
}

// hasFlag reports whether args already contain --name or --name=value
// (single-dash forms too, since Go's flag package accepts both).
func hasFlag(args []string, name string) bool {
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			continue
		}
		a = strings.TrimLeft(a, "-")
		if a == name || strings.HasPrefix(a, name+"=") {
			return true
		}
	}
	return false
}
