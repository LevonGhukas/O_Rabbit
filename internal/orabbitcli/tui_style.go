package orabbitcli

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Boxes give the shell the look of Claude Code's terminal UI: rounded
// panels for the welcome banner, the prompt, the wizard, summaries and errors.

const (
	tuiDefaultWidth = 80
	tuiMaxBoxWidth  = 110
)

var (
	tuiColorAccent = lipgloss.Color("212")
	tuiColorBorder = lipgloss.Color("240")
	tuiColorInfo   = lipgloss.Color("81")
	tuiColorErr    = lipgloss.Color("203")
	tuiColorWarn   = lipgloss.Color("214")
)

// boxWidth is the outer width of a panel for a terminal of the given width.
func boxWidth(termWidth int) int {
	if termWidth <= 0 {
		termWidth = tuiDefaultWidth
	}
	return max(20, min(termWidth, tuiMaxBoxWidth))
}

// renderBox draws body in a rounded border. An optional title is shown as
// the first line, bold in the border color.
func renderBox(termWidth int, color lipgloss.Color, title, body string) string {
	content := strings.TrimRight(body, "\n")
	if title != "" {
		head := lipgloss.NewStyle().Bold(true).Foreground(color).Render(title)
		if content != "" {
			content = head + "\n" + content
		} else {
			content = head
		}
	}
	// Width counts padding but not the border, hence the -2.
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(color).
		Padding(0, 1).
		Width(boxWidth(termWidth) - 2).
		Render(content)
}

func renderWelcome(termWidth int, base string) string {
	body := tuiStyleDim.Render("  /run starts an export · /runs lists runs · /help shows every command") + "\n" +
		tuiStyleDim.Render("  master: "+base)
	return renderBox(termWidth, tuiColorAccent, "✻ Welcome to O_Rabbit", "\n"+body)
}

// --- API errors ------------------------------------------------------------

// masterAPIError is the master's JSON error body.
type masterAPIError struct {
	Status       string
	Code         string         `json:"code"`
	Message      string         `json:"message"`
	Details      map[string]any `json:"details"`
	FailureClass string         `json:"failure_class"`
	Retryable    bool           `json:"retryable"`
}

var httpErrPrefix = regexp.MustCompile(`(?s)http (\d{3}): (\{.*\})`)

// parseMasterError extracts the master's structured error from an httpJSON
// error such as `http 503: {"error":{...}}`, wherever it sits in the chain.
func parseMasterError(err error) (masterAPIError, bool) {
	if err == nil {
		return masterAPIError{}, false
	}
	m := httpErrPrefix.FindStringSubmatch(err.Error())
	if m == nil {
		return masterAPIError{}, false
	}
	var body struct {
		Error masterAPIError `json:"error"`
	}
	if json.Unmarshal([]byte(m[2]), &body) != nil || body.Error.Message == "" {
		return masterAPIError{}, false
	}
	body.Error.Status = m[1]
	return body.Error, true
}

// errorHints turns known failure patterns into next steps. Many export
// failures are connectivity or configuration, not code.
func errorHints(e masterAPIError) []string {
	detail := strings.ToLower(fmt.Sprint(e.Details["error"]))
	var hints []string
	switch {
	case strings.Contains(detail, "lookup ") && (strings.Contains(detail, "no such host") || strings.Contains(detail, "server misbehaving")):
		host := ""
		if m := regexp.MustCompile(`lookup ([^ ]+)`).FindStringSubmatch(detail); m != nil {
			host = m[1]
		}
		hints = append(hints,
			fmt.Sprintf("The host %q in the DSN could not be resolved.", host),
			"Planning runs on the master and the export on the workers, so both must reach the source.",
			"On Docker, attach the database container to their network, e.g.:",
			"  docker network connect orabbit-control-plane "+host,
		)
	case strings.Contains(detail, "connection refused"):
		hints = append(hints,
			"The host resolved, but nothing listens on that port.",
			"Between containers use the container port (ClickHouse 9000), not the published host port.")
	case strings.Contains(detail, "i/o timeout") || strings.Contains(detail, "deadline exceeded"):
		hints = append(hints, "The connection timed out: check firewalls and that the host is reachable from the master and workers.")
	case strings.Contains(detail, "authentication") || strings.Contains(detail, "password") || strings.Contains(detail, "access denied"):
		hints = append(hints, "The source rejected the credentials: check the user and password in the DSN.")
	case strings.Contains(detail, "unknown table") || strings.Contains(detail, "doesn't exist") || strings.Contains(detail, "does not exist"):
		hints = append(hints, "The table was not found: check the source table name, including its database or schema.")
	}
	if e.Retryable {
		hints = append(hints, "This error is retryable: fix the cause, then /run again (your answers are kept).")
	}
	return hints
}

// renderErrorBlock shows err as a red panel. Structured master errors get
// their message, details and hints; other errors a single line.
func renderErrorBlock(termWidth int, title string, err error) string {
	e, ok := parseMasterError(err)
	if !ok {
		return tuiStyleErr.Render("✗ " + title + ": " + err.Error())
	}
	var b strings.Builder
	b.WriteString(e.Message)
	if e.FailureClass != "" || e.Code != "" {
		b.WriteString(tuiStyleDim.Render(fmt.Sprintf("  (%s, http %s)", firstNonEmpty(e.FailureClass, e.Code), e.Status)))
	}
	b.WriteString("\n")
	keys := make([]string, 0, len(e.Details))
	for k := range e.Details {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(tuiStyleDim.Render(k+": ") + redactDSN(fmt.Sprint(e.Details[k])) + "\n")
	}
	if hints := errorHints(e); len(hints) > 0 {
		b.WriteString("\n")
		for _, h := range hints {
			b.WriteString(lipgloss.NewStyle().Foreground(tuiColorWarn).Render("→ ") + h + "\n")
		}
	}
	return renderBox(termWidth, tuiColorErr, "✗ "+title, b.String())
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
