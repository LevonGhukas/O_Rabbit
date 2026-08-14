package orabbitcli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type cliOutput struct {
	quiet   bool
	verbose bool
	stdout  io.Writer
	stderr  io.Writer

	mu sync.Mutex
	wg sync.WaitGroup
}

type commandLogAttachment struct {
	path     string
	writer   *os.File
	stopCh   chan struct{}
	stopOnce sync.Once
}

func newCLIOutput(quiet, verbose bool) *cliOutput {
	return &cliOutput{
		quiet:   quiet,
		verbose: verbose,
		stdout:  os.Stdout,
		stderr:  os.Stderr,
	}
}

func validateOutputMode(quiet, verbose bool) error {
	if quiet && verbose {
		return fmt.Errorf("--quiet and --verbose cannot be used together")
	}
	return nil
}

func (o *cliOutput) wait() {
	if o == nil {
		return
	}
	o.wg.Wait()
}

func (o *cliOutput) infof(format string, args ...any) {
	if o == nil || o.quiet {
		return
	}
	o.writeLine(o.stderr, "[client] "+fmt.Sprintf(format, args...))
}

func (o *cliOutput) debugf(format string, args ...any) {
	if o == nil || !o.verbose {
		return
	}
	o.writeLine(o.stderr, "[client] debug: "+fmt.Sprintf(format, args...))
}

func (o *cliOutput) eventln(msg string) {
	if o == nil {
		fmt.Fprintln(os.Stdout, msg)
		return
	}
	if o.quiet {
		return
	}
	o.writeLine(o.stdout, msg)
}

func (o *cliOutput) followCommandLog(ctx context.Context, label string, att *commandLogAttachment) <-chan struct{} {
	ready := make(chan struct{})
	if o == nil || o.quiet || att == nil {
		close(ready)
		return ready
	}
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()

		f, err := os.Open(att.path)
		close(ready)
		if err != nil {
			o.debugf("unable to follow %s log %q: %v", label, att.path, err)
			return
		}
		defer f.Close()

		buf := make([]byte, 4096)
		var pending strings.Builder
		drainAvailable := func() error {
			for {
				n, err := f.Read(buf)
				if n > 0 {
					pending.Write(buf[:n])
					for {
						s := pending.String()
						idx := strings.IndexByte(s, '\n')
						if idx < 0 {
							break
						}
						line := strings.TrimRight(s[:idx], "\r")
						o.writeLine(o.stderr, "[%s] %s", label, line)
						pending.Reset()
						pending.WriteString(s[idx+1:])
					}
				}
				if err != nil {
					return err
				}
			}
		}
		finalDrain := func() {
			for i := 0; i < 25; i++ {
				if err := drainAvailable(); err != io.EOF {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		for {
			err := drainAvailable()
			if err != io.EOF {
				o.debugf("stopped following %s log %q: %v", label, att.path, err)
				if pending.Len() > 0 {
					o.writeLine(o.stderr, "[%s] %s", label, strings.TrimRight(pending.String(), "\r"))
				}
				return
			}

			select {
			case <-ctx.Done():
				finalDrain()
				if pending.Len() > 0 {
					o.writeLine(o.stderr, "[%s] %s", label, strings.TrimRight(pending.String(), "\r"))
				}
				return
			case <-att.stopCh:
				finalDrain()
				if pending.Len() > 0 {
					o.writeLine(o.stderr, "[%s] %s", label, strings.TrimRight(pending.String(), "\r"))
				}
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	return ready
}

func (o *cliOutput) writeLine(w io.Writer, format string, args ...any) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(w, format+"\n", args...)
}

func (o *cliOutput) writeBlock(w io.Writer, text string) {
	if o == nil || strings.TrimSpace(text) == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	_, _ = io.WriteString(w, text)
}

func newCommandLogAttachment(runtimeDir, label string) (*commandLogAttachment, error) {
	runtimeDir = strings.TrimSpace(runtimeDir)
	if runtimeDir == "" {
		return nil, fmt.Errorf("missing managed runtime dir for %s log", label)
	}
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		return nil, fmt.Errorf("prepare managed runtime dir %q for %s log: %w", runtimeDir, label, err)
	}
	prefix := sanitizeLogLabel(label) + "-"
	writer, err := os.CreateTemp(runtimeDir, prefix+"*.log")
	if err != nil {
		return nil, fmt.Errorf("create %s log in %q: %w", label, runtimeDir, err)
	}
	return &commandLogAttachment{
		path:   writer.Name(),
		writer: writer,
		stopCh: make(chan struct{}),
	}, nil
}

func (a *commandLogAttachment) apply(cmd *exec.Cmd) {
	if a == nil || cmd == nil {
		return
	}
	cmd.Stdout = a.writer
	cmd.Stderr = a.writer
}

func (a *commandLogAttachment) closeParentWriter() {
	if a == nil || a.writer == nil {
		return
	}
	_ = a.writer.Close()
	a.writer = nil
}

func (a *commandLogAttachment) stopFollowing() {
	if a == nil {
		return
	}
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})
}

func sanitizeLogLabel(label string) string {
	replacer := strings.NewReplacer(
		"/", "-",
		"\\", "-",
		":", "-",
		" ", "-",
	)
	s := replacer.Replace(strings.TrimSpace(label))
	s = filepath.Clean(s)
	s = strings.Trim(s, ".-")
	if s == "" {
		return "daemon"
	}
	return s
}
