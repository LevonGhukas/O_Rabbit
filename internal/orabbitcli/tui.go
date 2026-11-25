package orabbitcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"golang.org/x/term"
)

// envMasterHTTP lets users point the interactive shell at a remote master
// without typing /connect every session.
const envMasterHTTP = "ORABBIT_MASTER_HTTP"

const (
	tuiRequestTimeout = 15 * time.Second
	tuiWatchInterval  = time.Second
	tuiProgressWidth  = 30
	tuiDefaultRuns    = 10
	tuiMaxHistory     = 200
)

var (
	tuiStyleTitle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("212"))
	tuiStyleDim      = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	tuiStyleOK       = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	tuiStyleErr      = lipgloss.NewStyle().Foreground(lipgloss.Color("203"))
	tuiStyleWarn     = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	tuiStyleCmd      = lipgloss.NewStyle().Foreground(lipgloss.Color("81"))
	tuiStylePrompt   = lipgloss.NewStyle().Foreground(lipgloss.Color("212")).Bold(true)
	tuiStyleSelected = lipgloss.NewStyle().Foreground(lipgloss.Color("212")).Bold(true)
)

// stdioIsTerminal reports whether both stdin and stdout are a terminal.
// The shell only starts in that case; pipes and CI keep the old behavior.
func stdioIsTerminal() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

// runTUI starts the interactive shell and returns the process exit code.
func runTUI() int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := strings.TrimSpace(os.Getenv(envMasterHTTP))
	explicit := base != ""
	if !explicit {
		base = localHTTPBase(defaultHTTPAddr)
	}

	p := tea.NewProgram(newTUIModel(ctx, normalizeHTTPBase(base), explicit), tea.WithoutSignalHandler())

	// Bubble Tea's own signal handler is disabled on purpose. Commands such as
	// /run hand the terminal to a child orabbit-client process; Ctrl+C there
	// sends SIGINT to the whole foreground process group, and it must stop the
	// child only, not close the shell. Catching (not ignoring) SIGINT here
	// matters: ignored signals are inherited across exec, caught ones are not.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		for sig := range sigCh {
			if sig == syscall.SIGTERM {
				p.Quit()
				return
			}
		}
	}()

	if _, err := p.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "interactive shell:", err)
		return exitOperational
	}
	return exitSuccess
}

// Messages produced by background commands. Bubble Tea delivers them to
// Update, which is the only place model state changes.
type (
	tuiResultMsg struct {
		text string
		err  error
	}
	tuiStatusMsg struct {
		healthy bool
		workers int
		err     error // error listing workers; health is reported separately
	}
	tuiWatchMsg struct {
		runID   string
		details runDetails
		err     error
	}
	tuiWatchTickMsg struct{ runID string }
	tuiExecDoneMsg  struct {
		label string
		err   error
	}
)

type tuiWatch struct {
	runID   string
	status  string
	prog    runProgress
	loaded  bool
	lastErr error
}

type tuiModel struct {
	ctx      context.Context
	base     string
	explicit bool // master URL came from env or /connect, so pass it to child commands

	input   textinput.Model
	spinner spinner.Model

	history    []string
	historyPos int // == len(history) when not browsing

	busy          string // label of the running request; empty when idle
	watch         *tuiWatch
	pendingCancel string // run ID waiting for y/N confirmation

	menuIndex       int  // highlighted completion menu row
	menuNavigated   bool // the user moved the highlight with the arrows
	browsingHistory bool // ↑/↓ recalled a history entry; the menu stays hidden

	masterHealthy *bool
	workers       int
}

func newTUIModel(ctx context.Context, base string, explicit bool) tuiModel {
	in := textinput.New()
	in.Prompt = tuiStylePrompt.Render("› ")
	in.Placeholder = "type / for commands"
	in.Focus()

	sp := spinner.New()
	sp.Spinner = spinner.Dot

	return tuiModel{ctx: ctx, base: base, explicit: explicit, input: in, spinner: sp}
}

func (m tuiModel) Init() tea.Cmd {
	banner := tuiStyleTitle.Render("O_Rabbit") + tuiStyleDim.Render(" interactive shell · master "+m.base) +
		"\n" + tuiStyleDim.Render("Type /help for commands. Existing subcommands (orabbit-client run submit ...) still work for scripts.")
	return tea.Batch(textinput.Blink, m.spinner.Tick, tea.Println(banner), m.statusCmd())
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKey(msg)

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	case tuiResultMsg:
		m.busy = ""
		if msg.err != nil {
			return m, tea.Println(tuiStyleErr.Render("✗ " + msg.err.Error()))
		}
		return m, tea.Println(msg.text)

	case tuiStatusMsg:
		m.busy = ""
		healthy := msg.healthy
		m.masterHealthy = &healthy
		m.workers = msg.workers
		return m, tea.Println(m.renderStatus(msg))

	case tuiExecDoneMsg:
		if msg.err != nil {
			var exitErr *exec.ExitError
			if errors.As(msg.err, &exitErr) {
				return m, tea.Println(tuiStyleWarn.Render(fmt.Sprintf("%s exited with code %d", msg.label, exitErr.ExitCode())))
			}
			return m, tea.Println(tuiStyleErr.Render(msg.label + ": " + msg.err.Error()))
		}
		return m, tea.Println(tuiStyleOK.Render("✓ " + msg.label + " finished"))

	case tuiWatchMsg:
		return m.handleWatchUpdate(msg)

	case tuiWatchTickMsg:
		if m.watch == nil || m.watch.runID != msg.runID {
			return m, nil // the watch was stopped meanwhile
		}
		return m, m.fetchWatchCmd(msg.runID)
	}

	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m tuiModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlD:
		return m, tea.Quit

	case tea.KeyCtrlC, tea.KeyEsc:
		// Ctrl+C escalates like in most shells: stop the current activity
		// first, clear the line next, and only quit from an empty prompt.
		switch {
		case m.watch != nil:
			id := m.watch.runID
			m.watch = nil
			return m, tea.Println(tuiStyleDim.Render("stopped watching " + id + " (the run keeps going)"))
		case m.pendingCancel != "":
			m.pendingCancel = ""
			m.input.SetValue("")
			return m, tea.Println(tuiStyleDim.Render("cancel aborted"))
		case m.input.Value() != "":
			m.input.SetValue("")
			m.resetMenu()
			m.browsingHistory = false
			return m, nil
		case msg.Type == tea.KeyCtrlC:
			return m, tea.Quit
		}
		return m, nil

	case tea.KeyTab:
		if items := m.menuItems(); len(items) > 0 {
			m.applyMenuItem(items)
		}
		return m, nil

	case tea.KeyUp, tea.KeyDown:
		// Arrows move through the menu while it is open, and through history
		// otherwise (or once the user has started browsing history).
		if items := m.menuItems(); len(items) > 0 {
			m.moveMenu(len(items), msg.Type == tea.KeyUp)
			return m, nil
		}
		m.browseHistory(msg.Type == tea.KeyUp)
		return m, nil

	case tea.KeyEnter:
		if items := m.menuItems(); len(items) > 0 && m.enterAppliesMenu() {
			m.applyMenuItem(items)
			return m, nil
		}
		line := strings.TrimSpace(m.input.Value())
		m.input.SetValue("")
		m.resetMenu()
		m.browsingHistory = false
		if m.pendingCancel != "" {
			return m.confirmCancel(line)
		}
		if line == "" {
			return m, nil
		}
		if m.busy != "" || m.watch != nil {
			m.input.SetValue(line) // keep what the user typed
			return m, tea.Println(tuiStyleDim.Render("busy — press Esc to stop the current activity first"))
		}
		m.pushHistory(line)
		echo := tea.Println(tuiStyleDim.Render("› " + line))
		model, cmd := m.dispatch(line)
		return model, tea.Sequence(echo, cmd)
	}

	// Any other key edits the line: the menu starts over at the top and the
	// line stops being a history entry.
	before := m.input.Value()
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	if m.input.Value() != before {
		m.resetMenu()
		m.browsingHistory = false
	}
	return m, cmd
}

// menuItems returns the completion menu for the current line, or nil when
// it is hidden (confirming a cancel, or browsing history).
func (m tuiModel) menuItems() []tuiMenuItem {
	if m.pendingCancel != "" || m.browsingHistory {
		return nil
	}
	return tuiMenuItems(m.input.Value())
}

func (m *tuiModel) moveMenu(n int, up bool) {
	if up {
		m.menuIndex = (m.menuIndex - 1 + n) % n
	} else {
		m.menuIndex = (m.menuIndex + 1) % n
	}
	m.menuNavigated = true
}

func (m *tuiModel) applyMenuItem(items []tuiMenuItem) {
	m.input.SetValue(items[min(m.menuIndex, len(items)-1)].Insert)
	m.input.CursorEnd()
	m.resetMenu()
}

func (m *tuiModel) resetMenu() {
	m.menuIndex = 0
	m.menuNavigated = false
}

// enterAppliesMenu decides whether Enter picks the highlighted menu item or
// runs the line. It picks when the user moved through the menu, or when the
// command word is still unfinished (/wa); a complete line such as `/runs` or
// `/run --advanced ` runs as typed.
func (m tuiModel) enterAppliesMenu() bool {
	if m.menuNavigated {
		return true
	}
	v := strings.TrimSpace(m.input.Value())
	if strings.ContainsAny(v, " \t") {
		return false
	}
	_, exact := lookupTUICommand(strings.TrimPrefix(v, "/"))
	return !exact
}

func (m *tuiModel) pushHistory(line string) {
	if n := len(m.history); n == 0 || m.history[n-1] != line {
		m.history = append(m.history, line)
		if len(m.history) > tuiMaxHistory {
			m.history = m.history[len(m.history)-tuiMaxHistory:]
		}
	}
	m.historyPos = len(m.history)
}

func (m *tuiModel) browseHistory(up bool) {
	if len(m.history) == 0 || m.pendingCancel != "" {
		return
	}
	if up && m.historyPos > 0 {
		m.historyPos--
	} else if !up && m.historyPos < len(m.history) {
		m.historyPos++
	}
	if m.historyPos == len(m.history) {
		m.input.SetValue("")
		m.browsingHistory = false
	} else {
		m.input.SetValue(m.history[m.historyPos])
		m.browsingHistory = true
	}
	m.input.CursorEnd()
	m.resetMenu()
}

// dispatch runs one shell line. Native commands talk to the master API
// directly; the rest re-run this binary with the matching subcommand so the
// existing, tested implementations are reused instead of duplicated.
func (m tuiModel) dispatch(line string) (tea.Model, tea.Cmd) {
	name, args, err := parseTUIInput(line)
	if err != nil {
		return m, printErr(err)
	}
	cmd, ok := lookupTUICommand(name)
	if !ok {
		hint := fmt.Sprintf("unknown command %q", name)
		if s, ok := suggestTUICommand(name); ok {
			hint += fmt.Sprintf(" — did you mean /%s?", s)
		}
		return m, printErr(errors.New(hint + " (type /help)"))
	}

	switch cmd.Name {
	case "help":
		return m, tea.Println(renderTUIHelp())
	case "exit":
		return m, tea.Quit
	case "clear":
		return m, tea.ClearScreen
	case "status":
		m.busy = "checking master"
		return m, m.statusCmd()
	case "connect":
		if len(args) != 1 {
			return m, usageErr(cmd)
		}
		m.base = normalizeHTTPBase(args[0])
		m.explicit = true
		m.masterHealthy = nil
		m.busy = "connecting"
		return m, m.statusCmd()
	case "runs":
		limit := tuiDefaultRuns
		if len(args) > 1 {
			return m, usageErr(cmd)
		}
		if len(args) == 1 {
			n, err := strconv.Atoi(args[0])
			if err != nil || n <= 0 {
				return m, printErr(errors.New("limit must be a positive number"))
			}
			limit = n
		}
		m.busy = "loading runs"
		return m, m.listRunsCmd(limit)
	case "watch":
		if len(args) != 1 {
			return m, usageErr(cmd)
		}
		m.watch = &tuiWatch{runID: args[0]}
		return m, m.fetchWatchCmd(args[0])
	case "cancel":
		if len(args) != 1 {
			return m, usageErr(cmd)
		}
		m.pendingCancel = args[0]
		return m, nil
	case "diagnose":
		if len(args) != 1 {
			return m, usageErr(cmd)
		}
		m.busy = "diagnosing " + args[0]
		return m, m.diagnoseCmd(args[0])
	case "run":
		return m, m.execCLI("run interactive", append([]string{"run", "interactive"}, args...), m.explicit)
	case "submit":
		if len(args) == 0 {
			return m, usageErr(cmd)
		}
		// A bare file name is the common case; flags pass through unchanged.
		if !strings.HasPrefix(args[0], "-") {
			args = append([]string{"--file", args[0]}, args[1:]...)
		}
		return m, m.execCLI("run submit", append([]string{"run", "submit"}, args...), true)
	case "recover":
		if len(args) == 0 {
			return m, usageErr(cmd)
		}
		return m, m.execCLI("run recover", append([]string{"run", "recover"}, args...), true)
	case "stack":
		if len(args) == 0 {
			return m, usageErr(cmd)
		}
		// stack commands manage local processes and have no --master-http flag.
		return m, m.execCLI("stack "+args[0], append([]string{"stack"}, args...), false)
	case "maintenance":
		if len(args) == 0 {
			return m, usageErr(cmd)
		}
		return m, m.execCLI("maintenance "+args[0], append([]string{"maintenance"}, args...), true)
	}
	return m, printErr(fmt.Errorf("command /%s is not implemented", cmd.Name))
}

func (m tuiModel) confirmCancel(answer string) (tea.Model, tea.Cmd) {
	runID := m.pendingCancel
	m.pendingCancel = ""
	switch strings.ToLower(answer) {
	case "y", "yes":
		m.busy = "canceling " + runID
		return m, m.cancelCmd(runID)
	}
	return m, tea.Println(tuiStyleDim.Render("run " + runID + " was not canceled"))
}

func (m tuiModel) handleWatchUpdate(msg tuiWatchMsg) (tea.Model, tea.Cmd) {
	if m.watch == nil || m.watch.runID != msg.runID {
		return m, nil
	}
	w := m.watch
	if msg.err != nil {
		if !w.loaded {
			// The first fetch failed (typo in the ID, master down): give up.
			m.watch = nil
			return m, printErr(fmt.Errorf("watch %s: %w", msg.runID, msg.err))
		}
		// Transient error mid-watch: keep the last progress and retry.
		w.lastErr = msg.err
		return m, m.watchTickCmd(msg.runID)
	}
	w.loaded = true
	w.lastErr = nil
	w.status = msg.details.Run.Status
	w.prog = summarizeRunProgress(msg.details)

	if isTerminalRunStatus(w.status) {
		m.watch = nil
		line := "Run " + msg.runID + "  " + formatRunProgressLine(w.status, w.prog, tuiProgressWidth)
		style := tuiStyleOK
		if !strings.EqualFold(w.status, "SUCCEEDED") {
			style = tuiStyleErr
		}
		out := style.Render(line)
		if s := msg.details.Run.ErrorSummary; s != nil && strings.TrimSpace(*s) != "" {
			out += "\n" + tuiStyleErr.Render("  error: "+strings.TrimSpace(*s))
		}
		return m, tea.Println(out)
	}
	return m, m.watchTickCmd(msg.runID)
}

func (m tuiModel) View() string {
	var b strings.Builder

	switch {
	case m.watch != nil:
		w := m.watch
		b.WriteString(m.spinner.View() + " Run " + w.runID + "  ")
		if w.loaded {
			b.WriteString(formatRunProgressLine(w.status, w.prog, tuiProgressWidth))
		} else {
			b.WriteString(tuiStyleDim.Render("loading…"))
		}
		if w.lastErr != nil {
			b.WriteString(tuiStyleWarn.Render("  (retrying: " + w.lastErr.Error() + ")"))
		}
		b.WriteString("\n" + tuiStyleDim.Render("  Esc stops watching · the run keeps going") + "\n")
	case m.busy != "":
		b.WriteString(m.spinner.View() + " " + tuiStyleDim.Render(m.busy+"…") + "\n")
	}

	if m.pendingCancel != "" {
		b.WriteString(tuiStyleWarn.Render("Cancel run "+m.pendingCancel+"? [y/N] ") + m.input.Value() + "\n")
	} else {
		b.WriteString(m.input.View() + "\n")
		b.WriteString(m.renderSuggestions())
	}
	b.WriteString(m.renderFooter())
	return b.String()
}

// tuiMenuMaxRows caps the menu height; the visible window follows the
// highlight so long flag lists stay usable on small terminals.
const tuiMenuMaxRows = 8

// renderSuggestions draws the completion menu under the prompt, like the
// slash-command menu in Claude Code, with the highlighted row marked.
func (m tuiModel) renderSuggestions() string {
	items := m.menuItems()
	if len(items) == 0 {
		return ""
	}
	sel := min(m.menuIndex, len(items)-1)
	start := 0
	if sel >= tuiMenuMaxRows {
		start = sel - tuiMenuMaxRows + 1
	}
	end := min(start+tuiMenuMaxRows, len(items))

	var b strings.Builder
	for i := start; i < end; i++ {
		it := items[i]
		label := fmt.Sprintf("%-44s", it.Label)
		if i == sel {
			fmt.Fprintf(&b, "%s %s %s\n", tuiStylePrompt.Render("›"), tuiStyleSelected.Render(label), tuiStyleDim.Render(it.Summary))
		} else {
			fmt.Fprintf(&b, "  %s %s\n", tuiStyleCmd.Render(label), tuiStyleDim.Render(it.Summary))
		}
	}
	if len(items) > end-start {
		b.WriteString(tuiStyleDim.Render(fmt.Sprintf("  %d/%d · ↑↓ select · Tab/Enter choose", sel+1, len(items))) + "\n")
	}
	return b.String()
}

func (m tuiModel) renderFooter() string {
	state := tuiStyleDim.Render("●")
	if m.masterHealthy != nil {
		if *m.masterHealthy {
			state = tuiStyleOK.Render("●")
		} else {
			state = tuiStyleErr.Render("●")
		}
	}
	return state + tuiStyleDim.Render(" "+m.base+" · Tab complete · ↑↓ menu/history · /help · Ctrl+D exit")
}

func (m tuiModel) renderStatus(msg tuiStatusMsg) string {
	if !msg.healthy {
		return tuiStyleErr.Render("✗ master "+m.base+" is not reachable") + "\n" +
			tuiStyleDim.Render("  start one with /stack start master worker, or /connect <url>")
	}
	out := tuiStyleOK.Render("✓ master "+m.base+" is healthy") + "\n"
	if msg.err != nil {
		return out + tuiStyleWarn.Render("  workers: unknown ("+msg.err.Error()+")")
	}
	return out + tuiStyleDim.Render(fmt.Sprintf("  active workers: %d", msg.workers))
}

func renderTUIHelp() string {
	var b strings.Builder
	b.WriteString(tuiStyleTitle.Render("Commands") + "\n")
	for _, c := range tuiCommands {
		fmt.Fprintf(&b, "  %s %s\n", tuiStyleCmd.Render(fmt.Sprintf("%-44s", c.Usage)), tuiStyleDim.Render(c.Summary))
	}
	b.WriteString(tuiStyleDim.Render("Keys: Tab completes · ↑↓ history · Esc/Ctrl+C stops the current activity · Ctrl+D exits"))
	return b.String()
}

// --- background commands -------------------------------------------------
// Each returns a tea.Cmd: a function Bubble Tea runs off the UI goroutine,
// whose return value comes back to Update as a message.

func (m tuiModel) requestCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(m.ctx, tuiRequestTimeout)
}

func (m tuiModel) statusCmd() tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		if !checkHealth(ctx, base) {
			return tuiStatusMsg{healthy: false}
		}
		workers, err := listActiveWorkers(ctx, base)
		return tuiStatusMsg{healthy: true, workers: len(workers), err: err}
	}
}

// tuiRunSummary is the subset of the master's run list entry the shell shows.
type tuiRunSummary struct {
	ID           string  `json:"id"`
	JobID        string  `json:"job_id"`
	Status       string  `json:"status"`
	StartedAt    string  `json:"started_at"`
	ErrorSummary *string `json:"error_summary"`
}

func (m tuiModel) listRunsCmd(limit int) tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		var runs []tuiRunSummary
		url := fmt.Sprintf("%s%s/runs?limit=%d", base, apiV1, limit)
		if err := httpJSON(ctx, http.MethodGet, url, nil, &runs); err != nil {
			return tuiResultMsg{err: fmt.Errorf("list runs: %w", err)}
		}
		return tuiResultMsg{text: formatRunsTable(runs)}
	}
}

func formatRunsTable(runs []tuiRunSummary) string {
	if len(runs) == 0 {
		return tuiStyleDim.Render("no runs yet")
	}
	var b strings.Builder
	b.WriteString(tuiStyleDim.Render(fmt.Sprintf("%-38s %-12s %-22s %s", "RUN ID", "STATUS", "STARTED", "JOB")))
	for _, r := range runs {
		status := fmt.Sprintf("%-12s", r.Status)
		switch strings.ToUpper(r.Status) {
		case "SUCCEEDED":
			status = tuiStyleOK.Render(status)
		case "FAILED", "CANCELED":
			status = tuiStyleErr.Render(status)
		default:
			status = tuiStyleWarn.Render(status)
		}
		fmt.Fprintf(&b, "\n%-38s %s %-22s %s", r.ID, status, r.StartedAt, r.JobID)
	}
	return b.String()
}

func (m tuiModel) fetchWatchCmd(runID string) tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		d, err := getRunDetails(ctx, base, runID)
		return tuiWatchMsg{runID: runID, details: d, err: err}
	}
}

func (m tuiModel) watchTickCmd(runID string) tea.Cmd {
	return tea.Tick(tuiWatchInterval, func(time.Time) tea.Msg { return tuiWatchTickMsg{runID: runID} })
}

func (m tuiModel) cancelCmd(runID string) tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		resp, err := cancelRun(ctx, base, runID)
		if err != nil {
			return tuiResultMsg{err: fmt.Errorf("cancel %s: %w", runID, err)}
		}
		return tuiResultMsg{text: tuiStyleOK.Render(fmt.Sprintf("✓ run %s: status=%s canceled=%t pending tasks canceled=%d",
			runID, resp.Run.Status, resp.Canceled, resp.PendingTasksCanceled))}
	}
}

func (m tuiModel) diagnoseCmd(runID string) tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		d, err := diagnoseRun(ctx, base, runID)
		if err != nil {
			return tuiResultMsg{err: fmt.Errorf("diagnose %s: %w", runID, err)}
		}
		b, err := json.MarshalIndent(d, "", "  ")
		if err != nil {
			return tuiResultMsg{err: fmt.Errorf("format diagnosis: %w", err)}
		}
		return tuiResultMsg{text: string(b)}
	}
}

// execCLI suspends the shell and runs this same binary with args, giving it
// the full terminal (needed for the guided prompts of `run interactive`).
// When withMaster is true and the user did not pass --master-http, the
// shell's current master URL is appended so both stay pointed at one master.
func (m tuiModel) execCLI(label string, args []string, withMaster bool) tea.Cmd {
	exe, err := os.Executable()
	if err != nil {
		return printErr(fmt.Errorf("locate %s binary: %w", CLIName, err))
	}
	if withMaster && !hasFlag(args, "master-http") {
		args = append(args, "--master-http", m.base)
	}
	c := exec.Command(exe, args...)
	return tea.ExecProcess(c, func(err error) tea.Msg { return tuiExecDoneMsg{label: label, err: err} })
}

func printErr(err error) tea.Cmd {
	return tea.Println(tuiStyleErr.Render("✗ " + err.Error()))
}

func usageErr(c tuiCommand) tea.Cmd {
	return tea.Println(tuiStyleWarn.Render("usage: " + c.Usage))
}
