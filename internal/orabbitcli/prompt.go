package orabbitcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/term"
)

var (
	// ErrInterrupted reports that interactive input was canceled by context/signal.
	ErrInterrupted = errors.New("interrupted")
	// ErrPromptAborted reports that the user intentionally declined to continue.
	ErrPromptAborted = errors.New("prompt aborted")
	// ErrPromptTTYRequired reports that interactive prompting was attempted without a real stdin TTY.
	ErrPromptTTYRequired = errors.New("interactive prompts require a TTY on stdin")
	// ErrPromptEOF reports that stdin closed while interactive input was still required.
	ErrPromptEOF = errors.New("stdin closed while waiting for interactive input")
	// ErrPromptInvalid reports that a prompt received syntactically invalid input.
	ErrPromptInvalid = errors.New("invalid prompt input")
)

const (
	keyCtrlC = byte(3)
)

// splitPositionals keeps positional component parsing separate from flag parsing.
func splitPositionals(args []string) (pos []string, rest []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			return pos, args[i:]
		}
		pos = append(pos, a)
	}
	return pos, nil
}

// has reports whether xs contains x.
func has(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func stdinIsTTY() bool {
	return term.IsTerminal(int(os.Stdin.Fd()))
}

func requireInteractiveTTY() error {
	if stdinIsTTY() {
		return nil
	}
	return fmt.Errorf("%w: `%s run interactive` prompts cannot read defaults safely from non-interactive stdin", ErrPromptTTYRequired, CLIName)
}

type controlTrackingReadWriter struct {
	in  io.Reader
	out io.Writer

	mu       sync.Mutex
	sawCtrlC bool
}

func newControlTrackingReadWriter(in io.Reader, out io.Writer) *controlTrackingReadWriter {
	return &controlTrackingReadWriter{in: in, out: out}
}

func (rw *controlTrackingReadWriter) Read(p []byte) (int, error) {
	n, err := rw.in.Read(p)
	if n > 0 {
		rw.mu.Lock()
		for _, b := range p[:n] {
			if b == keyCtrlC {
				rw.sawCtrlC = true
			}
		}
		rw.mu.Unlock()
	}
	return n, err
}

func (rw *controlTrackingReadWriter) Write(p []byte) (int, error) {
	return rw.out.Write(p)
}

func (rw *controlTrackingReadWriter) resetControlState() {
	rw.mu.Lock()
	rw.sawCtrlC = false
	rw.mu.Unlock()
}

func (rw *controlTrackingReadWriter) sawInterruptKey() bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.sawCtrlC
}

type promptReadResult struct {
	line string
	err  error
}

type promptSession struct {
	terminal *term.Terminal
	tracker  *controlTrackingReadWriter
	restore  func() error
	width    int
}

type promptFieldSpec struct {
	Label string
	Note  string
}

func newTTYPromptSession() (*promptSession, error) {
	fd := int(os.Stdin.Fd())
	state, err := term.MakeRaw(fd)
	if err != nil {
		return nil, fmt.Errorf("enable terminal line editing: %w", err)
	}
	tracker := newControlTrackingReadWriter(os.Stdin, os.Stdout)
	terminal := term.NewTerminal(tracker, "")
	terminal.SetBracketedPasteMode(true)
	return &promptSession{
		terminal: terminal,
		tracker:  tracker,
		width:    wrapWidthForWriter(os.Stdout),
		restore: func() error {
			return term.Restore(fd, state)
		},
	}, nil
}

func newPromptSessionFromReadWriter(rw io.ReadWriter) *promptSession {
	tracker := newControlTrackingReadWriter(rw, rw)
	return &promptSession{
		terminal: term.NewTerminal(tracker, ""),
		tracker:  tracker,
		width:    defaultWrapWidth,
	}
}

func (p *promptSession) Close() error {
	if p == nil {
		return nil
	}
	if p.terminal != nil {
		p.terminal.SetBracketedPasteMode(false)
		p.terminal = nil
	}
	restore := p.restore
	p.restore = nil
	p.tracker = nil
	if restore == nil {
		return nil
	}
	return restore()
}

func (p *promptSession) readLine(ctx context.Context, prompt string, hidden bool) (string, error) {
	if p == nil || p.terminal == nil || p.tracker == nil {
		return "", fmt.Errorf("prompt session is not initialized")
	}

	p.tracker.resetControlState()
	resCh := make(chan promptReadResult, 1)
	go func() {
		var (
			line string
			err  error
		)
		if hidden {
			line, err = p.terminal.ReadPassword(prompt)
		} else {
			p.terminal.SetPrompt(prompt)
			line, err = p.terminal.ReadLine()
		}
		if errors.Is(err, io.EOF) {
			if p.tracker.sawInterruptKey() {
				_, _ = p.terminal.Write([]byte("\r\n"))
				err = ErrInterrupted
			} else {
				err = ErrPromptEOF
			}
		}
		resCh <- promptReadResult{line: strings.TrimSpace(line), err: err}
	}()

	select {
	case <-ctx.Done():
		_, _ = p.terminal.Write([]byte("\r\n"))
		return "", ErrInterrupted
	case res := <-resCh:
		return res.line, res.err
	}
}

func promptString(ctx context.Context, p *promptSession, label, def string) (string, error) {
	line, err := p.readLine(ctx, promptLabel(label, def), false)
	if err != nil {
		if errors.Is(err, ErrInterrupted) {
			return "", err
		}
		return "", fmt.Errorf("%s: %w", label, err)
	}
	if line == "" {
		return def, nil
	}
	return line, nil
}

func promptStringField(ctx context.Context, p *promptSession, spec promptFieldSpec, def string) (string, error) {
	if err := p.writePromptPrelude(spec, promptDefaultValue(def)); err != nil {
		return "", fmt.Errorf("%s: %w", spec.Label, err)
	}
	line, err := p.readLine(ctx, "> ", false)
	if err != nil {
		if errors.Is(err, ErrInterrupted) {
			return "", err
		}
		return "", fmt.Errorf("%s: %w", spec.Label, err)
	}
	if line == "" {
		return def, nil
	}
	return line, nil
}

func promptSecretString(ctx context.Context, p *promptSession, label, def string) (string, error) {
	line, err := p.readLine(ctx, secretPromptLabel(label, def), true)
	if err != nil {
		if errors.Is(err, ErrInterrupted) {
			return "", err
		}
		return "", fmt.Errorf("%s: %w", label, err)
	}
	if line == "" {
		return def, nil
	}
	return line, nil
}

func promptSecretStringField(ctx context.Context, p *promptSession, spec promptFieldSpec, def string) (string, error) {
	defaultLine := ""
	if strings.TrimSpace(def) != "" {
		defaultLine = "Press Enter to keep the current hidden value."
	}
	if err := p.writePromptPrelude(spec, defaultLine); err != nil {
		return "", fmt.Errorf("%s: %w", spec.Label, err)
	}
	line, err := p.readLine(ctx, "> ", true)
	if err != nil {
		if errors.Is(err, ErrInterrupted) {
			return "", err
		}
		return "", fmt.Errorf("%s: %w", spec.Label, err)
	}
	if line == "" {
		return def, nil
	}
	return line, nil
}

func promptBool(ctx context.Context, p *promptSession, label string, def bool) (bool, error) {
	defS := "n"
	if def {
		defS = "y"
	}
	line, err := p.readLine(ctx, fmt.Sprintf("%s [y/n] (default %s): ", label, defS), false)
	if err != nil {
		if errors.Is(err, ErrInterrupted) {
			return false, err
		}
		return false, fmt.Errorf("%s: %w", label, err)
	}
	line = strings.ToLower(line)
	if line == "" {
		return def, nil
	}
	switch line {
	case "y", "yes", "1", "true":
		return true, nil
	case "n", "no", "0", "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s: %w: expected y/n, got %q", label, ErrPromptInvalid, line)
	}
}

func promptBoolField(ctx context.Context, p *promptSession, spec promptFieldSpec, def bool) (bool, error) {
	note := appendPromptNote(spec.Note, fmt.Sprintf("Enter y or n. Default: %s.", boolPromptValue(def)))
	if err := p.writePromptPrelude(promptFieldSpec{Label: spec.Label, Note: note}, ""); err != nil {
		return false, fmt.Errorf("%s: %w", spec.Label, err)
	}
	line, err := p.readLine(ctx, "> ", false)
	if err != nil {
		if errors.Is(err, ErrInterrupted) {
			return false, err
		}
		return false, fmt.Errorf("%s: %w", spec.Label, err)
	}
	line = strings.ToLower(line)
	if line == "" {
		return def, nil
	}
	switch line {
	case "y", "yes", "1", "true":
		return true, nil
	case "n", "no", "0", "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s: %w: expected y/n, got %q", spec.Label, ErrPromptInvalid, line)
	}
}

func promptInt(ctx context.Context, p *promptSession, label string, def int) (int, error) {
	line, err := p.readLine(ctx, fmt.Sprintf("%s [%d]: ", label, def), false)
	if err != nil {
		if errors.Is(err, ErrInterrupted) {
			return 0, err
		}
		return 0, fmt.Errorf("%s: %w", label, err)
	}
	if line == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w: expected integer, got %q", label, ErrPromptInvalid, line)
	}
	return int(v), nil
}

func promptIntField(ctx context.Context, p *promptSession, spec promptFieldSpec, def int) (int, error) {
	note := appendPromptNote(spec.Note, fmt.Sprintf("Enter an integer. Default: %d.", def))
	if err := p.writePromptPrelude(promptFieldSpec{Label: spec.Label, Note: note}, ""); err != nil {
		return 0, fmt.Errorf("%s: %w", spec.Label, err)
	}
	line, err := p.readLine(ctx, "> ", false)
	if err != nil {
		if errors.Is(err, ErrInterrupted) {
			return 0, err
		}
		return 0, fmt.Errorf("%s: %w", spec.Label, err)
	}
	if line == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w: expected integer, got %q", spec.Label, ErrPromptInvalid, line)
	}
	return int(v), nil
}

func (p *promptSession) writePromptPrelude(spec promptFieldSpec, defaultLine string) error {
	if err := p.writeWrappedPrompt(spec.Label, "", ""); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Note) != "" {
		if err := p.writeWrappedPrompt(spec.Note, "  ", "  "); err != nil {
			return err
		}
	}
	if strings.TrimSpace(defaultLine) != "" {
		if err := p.writeWrappedPrompt("Default: "+defaultLine, "  ", "           "); err != nil {
			return err
		}
	}
	return nil
}

func (p *promptSession) writeWrappedPrompt(text, firstIndent, restIndent string) error {
	if p == nil || p.terminal == nil {
		return fmt.Errorf("prompt session is not initialized")
	}
	lines := wrapText(text, p.width, firstIndent, restIndent)
	if len(lines) == 0 {
		return nil
	}
	payload := strings.Join(lines, "\r\n") + "\r\n"
	_, err := p.terminal.Write([]byte(payload))
	return err
}

func (p *promptSession) writePromptSection(title string) error {
	if p == nil || p.terminal == nil {
		return fmt.Errorf("prompt session is not initialized")
	}
	if _, err := p.terminal.Write([]byte("\r\n")); err != nil {
		return err
	}
	return p.writeWrappedPrompt(title, "", "")
}

func promptDefaultValue(def string) string {
	return strings.TrimSpace(def)
}

func boolPromptValue(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}

func appendPromptNote(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		filtered = append(filtered, part)
	}
	return strings.Join(filtered, " ")
}

func promptLabel(label, def string) string {
	if strings.TrimSpace(def) == "" {
		return label + ": "
	}
	return fmt.Sprintf("%s [%s]: ", label, def)
}

func secretPromptLabel(label, def string) string {
	if strings.TrimSpace(def) == "" {
		return label + ": "
	}
	return fmt.Sprintf("%s [hidden current value]: ", label)
}
