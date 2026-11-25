package orabbitcli

import (
	"errors"
	"sort"
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
}

var tuiCommands = []tuiCommand{
	{Name: "help", Usage: "/help", Summary: "Show available commands", Aliases: []string{"?"}},
	{Name: "status", Usage: "/status", Summary: "Check master health and active workers"},
	{Name: "connect", Usage: "/connect <master-http-url>", Summary: "Switch to another master"},
	{Name: "runs", Usage: "/runs [limit]", Summary: "List recent runs (default 10)"},
	{Name: "run", Usage: "/run [classic] [flags]", Summary: "Start a run with the step-by-step wizard"},
	{Name: "submit", Usage: "/submit <spec-file>", Summary: "Submit a run from a YAML/JSON spec"},
	{Name: "watch", Usage: "/watch <run-id>", Summary: "Follow a run with a progress bar"},
	{Name: "cancel", Usage: "/cancel <run-id>", Summary: "Cancel a run (asks for confirmation)"},
	{Name: "diagnose", Usage: "/diagnose <run-id>", Summary: "Show the master's diagnosis of a run"},
	{Name: "recover", Usage: "/recover <run-id> --action <a> --reason <r>", Summary: "Run an operator recovery action"},
	{Name: "stack", Usage: "/stack <start|stop|status> [flags]", Summary: "Manage local master/worker processes"},
	{Name: "maintenance", Usage: "/maintenance <submit|show|list> [flags]", Summary: "Iceberg orphan cleanup and compaction"},
	{Name: "clear", Usage: "/clear", Summary: "Clear the screen"},
	{Name: "exit", Usage: "/exit", Summary: "Leave the shell (Ctrl+D also works)", Aliases: []string{"quit", "q"}},
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
