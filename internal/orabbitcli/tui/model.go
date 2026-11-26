// Package tui is the interactive shell of orabbit-client: the slash-command
// prompt with completion, the /run wizard, run progress and readable master
// errors. It opens when orabbit-client runs with no arguments in a terminal
// (see cmd/orabbit-client) and talks to the master through the small API in
// internal/orabbitcli/shell_api.go.
package tui

import (
	"github.com/LevonGhukas/O_Rabbit/internal/orabbitcli"

	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
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
	requestTimeout = 15 * time.Second
	watchInterval  = time.Second
	progressWidth  = 30
	defaultRuns    = 10
	maxHistory     = 200
)

var (
	styleTitle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("212"))
	styleDim      = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	styleOK       = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	styleErr      = lipgloss.NewStyle().Foreground(lipgloss.Color("203"))
	styleWarn     = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	styleCmd      = lipgloss.NewStyle().Foreground(lipgloss.Color("81"))
	stylePrompt   = lipgloss.NewStyle().Foreground(lipgloss.Color("212")).Bold(true)
	styleSelected = lipgloss.NewStyle().Foreground(lipgloss.Color("212")).Bold(true)
)

// IsTerminal reports whether both stdin and stdout are a terminal. The
// shell only starts in that case; pipes and CI keep the plain CLI.
func IsTerminal() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

// Run starts the interactive shell and blocks until the user leaves it.
// The master URL comes from ORABBIT_MASTER_HTTP, else the local default.
func Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := strings.TrimSpace(os.Getenv(envMasterHTTP))
	explicit := base != ""
	if !explicit {
		base = orabbitcli.DefaultMasterHTTP()
	}

	p := tea.NewProgram(newModel(ctx, orabbitcli.NormalizeHTTPBase(base), explicit), tea.WithoutSignalHandler())

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

	_, err := p.Run()
	return err
}

// Messages produced by background commands. Bubble Tea delivers them to
// Update, which is the only place model state changes.
type (
	resultMsg struct {
		text string
		err  error
	}
	statusMsg struct {
		healthy bool
		workers int
		err     error // error listing workers; health is reported separately
	}
	watchMsg struct {
		runID   string
		details orabbitcli.RunDetails
		err     error
	}
	watchTickMsg struct{ runID string }
	execDoneMsg  struct {
		label string
		err   error
	}
)

type watchState struct {
	runID   string
	status  string
	prog    runProgress
	loaded  bool
	lastErr error
}

type model struct {
	ctx      context.Context
	base     string
	explicit bool // master URL came from env or /connect, so pass it to child commands

	input   textinput.Model
	spinner spinner.Model

	history    []string
	historyPos int // == len(history) when not browsing

	busy          string // label of the running request; empty when idle
	watch         *watchState
	pendingCancel string // run ID waiting for y/N confirmation

	menuIndex       int  // highlighted completion menu row
	menuNavigated   bool // the user moved the highlight with the arrows
	browsingHistory bool // ↑/↓ recalled a history entry; the menu stays hidden
	execing         bool // a child CLI process owns the terminal

	width    int  // terminal width, from WindowSizeMsg
	welcomed bool // the welcome banner was printed

	wizard     *runWizard        // the guided /run wizard, while it is open
	lastWizard map[string]string // answers of the last wizard, to prefill the next

	masterHealthy *bool
	workers       int
}

func newModel(ctx context.Context, base string, explicit bool) model {
	in := textinput.New()
	in.Prompt = stylePrompt.Render("› ")
	in.Placeholder = "type / for commands"
	in.Focus()

	sp := spinner.New()
	sp.Spinner = spinner.Dot

	return model{ctx: ctx, base: base, explicit: explicit, input: in, spinner: sp}
}

// Init starts the cursor and spinner and checks the master. The welcome
// banner waits for the first WindowSizeMsg so it can be drawn full width.
func (m model) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, m.spinner.Tick, m.statusCmd())
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.input.Width = max(10, boxWidth(m.width)-8)
		if m.wizard != nil {
			m.wizard.input.Width = m.input.Width
		}
		if !m.welcomed {
			m.welcomed = true
			return m, tea.Println(renderWelcome(m.width, m.base))
		}
		return m, nil

	case tea.KeyMsg:
		if m.wizard != nil {
			return m.handleWizardKey(msg)
		}
		next, cmd := m.handleKey(msg)
		if tm, ok := next.(model); ok {
			tm.syncSuggestion()
			return tm, cmd
		}
		return next, cmd

	case wizardSubmittedMsg:
		m.busy = ""
		if msg.err != nil {
			return m, tea.Println(renderErrorBlock(m.width, "Submit failed", msg.err) + "\n" +
				styleDim.Render("  /run reopens the wizard with your answers"))
		}
		m.watch = &watchState{runID: msg.runID}
		note := styleOK.Render(fmt.Sprintf("✓ submitted run %s (%d tasks) — watching it, Esc stops watching", msg.runID, msg.tasks))
		return m, tea.Sequence(tea.Println(note), m.fetchWatchCmd(msg.runID))

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	case resultMsg:
		m.busy = ""
		if msg.err != nil {
			return m, tea.Println(renderErrorBlock(m.width, "Request failed", msg.err))
		}
		return m, tea.Println(fitWidth(msg.text, m.width))

	case statusMsg:
		m.busy = ""
		healthy := msg.healthy
		m.masterHealthy = &healthy
		m.workers = msg.workers
		return m, tea.Println(m.renderStatus(msg))

	case execDoneMsg:
		m.execing = false
		if msg.err != nil {
			var exitErr *exec.ExitError
			if errors.As(msg.err, &exitErr) {
				return m, tea.Println(styleWarn.Render(fmt.Sprintf("%s exited with code %d", msg.label, exitErr.ExitCode())))
			}
			return m, tea.Println(styleErr.Render(msg.label + ": " + msg.err.Error()))
		}
		return m, tea.Println(styleOK.Render("✓ " + msg.label + " finished"))

	case watchMsg:
		return m.handleWatchUpdate(msg)

	case watchTickMsg:
		if m.watch == nil || m.watch.runID != msg.runID {
			return m, nil // the watch was stopped meanwhile
		}
		return m, m.fetchWatchCmd(msg.runID)
	}

	if m.wizard != nil { // e.g. cursor blink messages for the wizard's input
		var cmd tea.Cmd
		m.wizard.input, cmd = m.wizard.input.Update(msg)
		return m, cmd
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

// handleWizardKey forwards keys to the open wizard and acts on its result.
func (m model) handleWizardKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	action, cmd := m.wizard.handleKey(msg)
	m.lastWizard = m.wizard.answers
	switch action {
	case wizClose:
		m.wizard = nil
		return m, tea.Println(styleDim.Render("run wizard closed — /run reopens it with your answers"))
	case wizSubmit, wizSave:
		spec, err := buildWizardSpec(m.wizard.answers, m.base)
		if err != nil {
			m.wizard.err = strings.TrimPrefix(err.Error(), orabbitcli.ErrRunSubmitSpecInvalid.Error()+": ")
			return m, nil
		}
		if action == wizSave {
			path, err := saveWizardSpec(spec)
			if err != nil {
				m.wizard.err = err.Error()
				return m, nil
			}
			m.wizard.notice = "saved " + path + " (contains secrets, owner-only) — run it later with /submit " + path
			summary := renderBox(m.width, colorInfo, "Saved run spec · "+path, m.wizard.reviewSummary(wizardFields(m.wizard.answers)))
			return m, tea.Println(summary)
		}
		m.busy = "submitting run"
		// Print the answers first, so the scrollback shows what was submitted
		// even when the submit fails.
		summary := renderBox(m.width, colorInfo, "Run configuration · "+spec.Job.Name, m.wizard.reviewSummary(wizardFields(m.wizard.answers)))
		m.wizard = nil
		return m, tea.Sequence(tea.Println(summary), tea.Println(styleDim.Render("submitting to "+m.base+"…")), m.submitWizardCmd(spec))
	}
	return m, cmd
}

func (m model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
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
			return m, tea.Println(styleDim.Render("stopped watching " + id + " (the run keeps going)"))
		case m.pendingCancel != "":
			m.pendingCancel = ""
			m.input.SetValue("")
			return m, tea.Println(styleDim.Render("cancel aborted"))
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
			return m, tea.Println(styleDim.Render("busy — press Esc to stop the current activity first"))
		}
		m.pushHistory(line)
		echo := tea.Println(styleDim.Render("› " + line))
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

// syncSuggestion shows the rest of the highlighted menu item as dim text
// after the cursor (ghost text); Tab accepts it.
func (m *model) syncSuggestion() {
	m.input.ShowSuggestions = true
	items := m.menuItems()
	if len(items) == 0 {
		m.input.SetSuggestions(nil)
		return
	}
	m.input.SetSuggestions([]string{items[min(m.menuIndex, len(items)-1)].Insert})
}

// menuItems returns the completion menu for the current line, or nil when
// it is hidden (confirming a cancel, or browsing history).
func (m model) menuItems() []menuItem {
	if m.pendingCancel != "" || m.browsingHistory {
		return nil
	}
	return menuItemsFor(m.input.Value())
}

func (m *model) moveMenu(n int, up bool) {
	if up {
		m.menuIndex = (m.menuIndex - 1 + n) % n
	} else {
		m.menuIndex = (m.menuIndex + 1) % n
	}
	m.menuNavigated = true
}

func (m *model) applyMenuItem(items []menuItem) {
	m.input.SetValue(items[min(m.menuIndex, len(items)-1)].Insert)
	m.input.CursorEnd()
	m.resetMenu()
}

func (m *model) resetMenu() {
	m.menuIndex = 0
	m.menuNavigated = false
}

// enterAppliesMenu decides whether Enter picks the highlighted menu item or
// runs the line. It picks when the user moved through the menu, or when the
// command word is still unfinished (/wa); a complete line such as `/runs` or
// `/run --advanced ` runs as typed.
func (m model) enterAppliesMenu() bool {
	if m.menuNavigated {
		return true
	}
	v := strings.TrimSpace(m.input.Value())
	if strings.ContainsAny(v, " \t") {
		return false
	}
	_, exact := lookupCommand(strings.TrimPrefix(v, "/"))
	return !exact
}

func (m *model) pushHistory(line string) {
	if n := len(m.history); n == 0 || m.history[n-1] != line {
		m.history = append(m.history, line)
		if len(m.history) > maxHistory {
			m.history = m.history[len(m.history)-maxHistory:]
		}
	}
	m.historyPos = len(m.history)
}

func (m *model) browseHistory(up bool) {
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
func (m model) dispatch(line string) (tea.Model, tea.Cmd) {
	name, args, err := parseInput(line)
	if err != nil {
		return m, printErr(err)
	}
	cmd, ok := lookupCommand(name)
	if !ok {
		hint := fmt.Sprintf("unknown command %q", name)
		if s, ok := suggestCommand(name); ok {
			hint += fmt.Sprintf(" — did you mean /%s?", s)
		}
		return m, printErr(errors.New(hint + " (type /help)"))
	}

	// `/<cmd> --help`, `-h` or `help` shows that command's own help. Commands
	// that run the CLI used to get this from its flag parser; now every
	// command answers the same way, with the flags the menu knows about.
	if cmd.Name != "help" && wantsHelp(args) {
		return m, tea.Println(fitWidth(renderCommandHelp(cmd), m.width))
	}

	switch cmd.Name {
	case "help":
		if len(args) == 1 {
			c, ok := lookupCommand(strings.TrimPrefix(args[0], "/"))
			if !ok {
				return m, printErr(fmt.Errorf("unknown command %q (type /help)", args[0]))
			}
			return m, tea.Println(fitWidth(renderCommandHelp(c), m.width))
		}
		return m, tea.Println(fitWidth(renderHelp(), m.width))
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
		m.base = orabbitcli.NormalizeHTTPBase(args[0])
		m.explicit = true
		m.masterHealthy = nil
		m.busy = "connecting"
		return m, m.statusCmd()
	case "runs":
		limit, job, err := parseRunsArgs(args)
		if err != nil {
			return m, printErr(err)
		}
		m.busy = "loading runs"
		return m, m.listRunsCmd(limit, job)
	case "jobs":
		if len(args) != 0 {
			return m, usageErr(cmd)
		}
		m.busy = "loading jobs"
		return m, m.listJobsCmd()
	case "workers":
		all := len(args) == 1 && (args[0] == "--all" || args[0] == "-all")
		if len(args) != 0 && !all {
			return m, usageErr(cmd)
		}
		m.busy = "loading workers"
		return m, m.listWorkersCmd(all)
	case "show":
		if len(args) != 1 {
			return m, usageErr(cmd)
		}
		m.busy = "loading run " + args[0]
		return m, m.showRunCmd(args[0])
	case "watch":
		if len(args) != 1 {
			return m, usageErr(cmd)
		}
		m.watch = &watchState{runID: args[0]}
		return m, m.fetchWatchCmd(args[0])
	case "cancel":
		if len(args) != 1 {
			return m, usageErr(cmd)
		}
		m.pendingCancel = args[0]
		return m, nil
	case "diagnose":
		rawJSON := hasFlag(args, "json")
		var refs []string
		for _, a := range args {
			if !strings.HasPrefix(a, "-") {
				refs = append(refs, a)
			}
		}
		if len(refs) != 1 {
			return m, usageErr(cmd)
		}
		m.busy = "diagnosing " + refs[0]
		return m, m.diagnoseCmd(refs[0], rawJSON)
	case "run":
		// Every form of /run opens the wizard: plain, `interactive`, and the
		// old `classic`. Its Advanced step covers what the CLI's flags set, so
		// extra words are not run through the old line-by-line prompts.
		var notes []tea.Cmd
		if extra := wizardIgnoredArgs(args); len(extra) > 0 {
			notes = append(notes, tea.Println(styleWarn.Render(
				"note: the wizard takes no flags, ignoring "+strings.Join(extra, " ")+
					" (its Advanced step sets prefix, tuning and fetch size)")))
		}
		m.wizard = newRunWizard(m.lastWizard)
		// Without a width the input wraps long DSNs over several lines.
		m.wizard.input.Width = max(10, boxWidth(m.width)-8)
		return m, tea.Batch(append(notes, textinput.Blink)...)
	case "submit":
		if len(args) == 0 {
			return m, usageErr(cmd)
		}
		// A bare file name is the common case; flags pass through unchanged.
		if !strings.HasPrefix(args[0], "-") {
			args = append([]string{"--file", args[0]}, args[1:]...)
		}
		return m, m.startExec("run submit", append([]string{"run", "submit"}, args...), true)
	case "recover":
		if len(args) == 0 {
			return m, usageErr(cmd)
		}
		return m, m.startExec("run recover", append([]string{"run", "recover"}, args...), true)
	case "stack":
		if len(args) == 0 {
			return m, usageErr(cmd)
		}
		// stack commands manage local processes and have no --master-http flag.
		return m, m.startExec("stack "+args[0], append([]string{"stack"}, args...), false)
	case "maintenance":
		if len(args) == 0 {
			return m, usageErr(cmd)
		}
		return m, m.startExec("maintenance "+args[0], append([]string{"maintenance"}, args...), true)
	}
	return m, printErr(fmt.Errorf("command /%s is not implemented", cmd.Name))
}

func (m model) confirmCancel(answer string) (tea.Model, tea.Cmd) {
	runID := m.pendingCancel
	m.pendingCancel = ""
	switch strings.ToLower(answer) {
	case "y", "yes":
		m.busy = "canceling " + runID
		return m, m.cancelCmd(runID)
	}
	return m, tea.Println(styleDim.Render("run " + runID + " was not canceled"))
}

func (m model) handleWatchUpdate(msg watchMsg) (tea.Model, tea.Cmd) {
	if m.watch == nil || m.watch.runID != msg.runID {
		return m, nil
	}
	w := m.watch
	if msg.err != nil {
		if !w.loaded {
			// The first fetch failed (typo in the ID, master down): give up.
			m.watch = nil
			return m, tea.Println(renderErrorBlock(m.width, "Watch "+msg.runID+" failed", msg.err))
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
		line := "Run " + msg.runID + "  " + formatRunProgressLine(w.status, w.prog, progressWidth)
		style := styleOK
		if !strings.EqualFold(w.status, "SUCCEEDED") {
			style = styleErr
		}
		out := style.Render(line)
		if s := msg.details.Run.ErrorSummary; s != nil && strings.TrimSpace(*s) != "" {
			out += "\n" + styleErr.Render("  error: "+strings.TrimSpace(*s))
		}
		return m, tea.Println(fitWidth(out, m.width))
	}
	return m, m.watchTickCmd(msg.runID)
}

func (m model) View() string {
	if m.execing {
		return ""
	}
	if m.wizard != nil {
		return fitWidth(renderBox(m.width, colorAccent, "", m.wizard.view())+"\n"+m.renderFooter(), m.width)
	}
	var b strings.Builder

	switch {
	case m.watch != nil:
		w := m.watch
		b.WriteString(m.spinner.View() + " Run " + w.runID + "  ")
		if w.loaded {
			b.WriteString(formatRunProgressLine(w.status, w.prog, progressWidth))
		} else {
			b.WriteString(styleDim.Render("loading…"))
		}
		if w.lastErr != nil {
			b.WriteString(styleWarn.Render("  (retrying: " + w.lastErr.Error() + ")"))
		}
		b.WriteString("\n" + styleDim.Render("  Esc stops watching · the run keeps going") + "\n")
	case m.busy != "":
		b.WriteString(m.spinner.View() + " " + styleDim.Render(m.busy+"…") + "\n")
	}

	if m.pendingCancel != "" {
		b.WriteString(renderBox(m.width, colorWarn, "", styleWarn.Render("Cancel run "+m.pendingCancel+"? [y/N] ")+m.input.Value()) + "\n")
	} else {
		b.WriteString(renderBox(m.width, colorBorder, "", m.input.View()) + "\n")
		b.WriteString(m.renderSuggestions())
	}
	b.WriteString(m.renderFooter())
	// Every line must be narrower than the terminal: Bubble Tea erases the
	// previous frame by counting lines, and a line the terminal wraps by
	// itself leaves old text behind (e.g. "rows" + "s watching").
	return fitWidth(b.String(), m.width)
}

// menuMaxRows caps the menu height; the visible window follows the
// highlight so long flag lists stay usable on small terminals.
const menuMaxRows = 8

// renderSuggestions draws the completion menu under the prompt, like the
// slash-command menu in Claude Code, with the highlighted row marked.
func (m model) renderSuggestions() string {
	items := m.menuItems()
	if len(items) == 0 {
		return ""
	}
	sel := min(m.menuIndex, len(items)-1)
	start := 0
	if sel >= menuMaxRows {
		start = sel - menuMaxRows + 1
	}
	end := min(start+menuMaxRows, len(items))

	var b strings.Builder
	for i := start; i < end; i++ {
		it := items[i]
		label := fmt.Sprintf("%-44s", it.Label)
		if i == sel {
			fmt.Fprintf(&b, "%s %s %s\n", stylePrompt.Render("›"), styleSelected.Render(label), styleDim.Render(it.Summary))
		} else {
			fmt.Fprintf(&b, "  %s %s\n", styleCmd.Render(label), styleDim.Render(it.Summary))
		}
	}
	if len(items) > end-start {
		b.WriteString(styleDim.Render(fmt.Sprintf("  %d/%d · ↑↓ select · Tab/Enter choose", sel+1, len(items))) + "\n")
	}
	return b.String()
}

func (m model) renderFooter() string {
	state := styleDim.Render("●")
	if m.masterHealthy != nil {
		if *m.masterHealthy {
			state = styleOK.Render("●")
		} else {
			state = styleErr.Render("●")
		}
	}
	return state + styleDim.Render(" "+m.base+" · Tab complete · ↑↓ menu/history · /help · Ctrl+D exit")
}

func (m model) renderStatus(msg statusMsg) string {
	if !msg.healthy {
		return styleErr.Render("✗ master "+m.base+" is not reachable") + "\n" +
			styleDim.Render("  start one with /stack start master worker, or /connect <url>")
	}
	out := styleOK.Render("✓ master "+m.base+" is healthy") + "\n"
	if msg.err != nil {
		return out + styleWarn.Render("  workers: unknown ("+msg.err.Error()+")")
	}
	return out + styleDim.Render(fmt.Sprintf("  active workers: %d", msg.workers))
}

// --- background commands -------------------------------------------------
// Each returns a tea.Cmd: a function Bubble Tea runs off the UI goroutine,
// whose return value comes back to Update as a message.

func (m model) requestCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(m.ctx, requestTimeout)
}

func (m model) statusCmd() tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		if !orabbitcli.CheckHealth(ctx, base) {
			return statusMsg{healthy: false}
		}
		workers, err := orabbitcli.ActiveWorkerCount(ctx, base)
		return statusMsg{healthy: true, workers: workers, err: err}
	}
}

// runSummary is the subset of the master's run list entry the shell shows.
type runSummary struct {
	ID           string  `json:"id"`
	JobID        string  `json:"job_id"`
	Status       string  `json:"status"`
	StartedAt    string  `json:"started_at"`
	ErrorSummary *string `json:"error_summary"`
}

func (m model) fetchWatchCmd(runID string) tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		d, err := orabbitcli.GetRunDetails(ctx, base, runID)
		return watchMsg{runID: runID, details: d, err: err}
	}
}

func (m model) watchTickCmd(runID string) tea.Cmd {
	return tea.Tick(watchInterval, func(time.Time) tea.Msg { return watchTickMsg{runID: runID} })
}

func (m model) cancelCmd(runID string) tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		resp, err := orabbitcli.CancelRun(ctx, base, runID)
		if err != nil {
			return resultMsg{err: fmt.Errorf("cancel %s: %w", runID, err)}
		}
		return resultMsg{text: styleOK.Render(fmt.Sprintf("✓ run %s: status=%s canceled=%t pending tasks canceled=%d",
			runID, resp.Run.Status, resp.Canceled, resp.PendingTasksCanceled))}
	}
}

// execCLI suspends the shell and runs this same binary with args, giving it
// the full terminal (needed for the guided prompts of `run interactive`).
// When withMaster is true and the user did not pass --master-http, the
// shell's current master URL is appended so both stay pointed at one master.
func (m model) execCLI(label string, args []string, withMaster bool) tea.Cmd {
	exe, err := os.Executable()
	if err != nil {
		// Report through execDoneMsg so Update also clears m.execing.
		err = fmt.Errorf("locate %s binary: %w", orabbitcli.CLIName, err)
		return func() tea.Msg { return execDoneMsg{label: label, err: err} }
	}
	if withMaster && !hasFlag(args, "master-http") {
		args = append(args, "--master-http", m.base)
	}
	c := exec.Command(exe, args...)
	return tea.ExecProcess(c, func(err error) tea.Msg { return execDoneMsg{label: label, err: err} })
}

// startExec hides the shell's prompt before handing the terminal over.
// Bubble Tea leaves the last frame on screen in inline mode, so without this
// a stale "› type / for commands" line would sit above the child's output.
func (m *model) startExec(label string, args []string, withMaster bool) tea.Cmd {
	m.execing = true
	return m.execCLI(label, args, withMaster)
}

func printErr(err error) tea.Cmd {
	return tea.Println(styleErr.Render("✗ " + err.Error()))
}

func usageErr(c command) tea.Cmd {
	return tea.Println(styleWarn.Render("usage: " + c.Usage))
}

// wizardIgnoredArgs drops the accepted aliases and returns what is left.
func wizardIgnoredArgs(args []string) []string {
	var extra []string
	for i, a := range args {
		if i == 0 && (a == "interactive" || a == "classic") {
			continue
		}
		extra = append(extra, a)
	}
	return extra
}
