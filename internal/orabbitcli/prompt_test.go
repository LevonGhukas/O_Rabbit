package orabbitcli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type scriptedReadWriter struct {
	reader io.Reader
	out    bytes.Buffer
}

func newScriptedReadWriter(input string) *scriptedReadWriter {
	return &scriptedReadWriter{reader: strings.NewReader(input)}
}

func (rw *scriptedReadWriter) Read(p []byte) (int, error) {
	return rw.reader.Read(p)
}

func (rw *scriptedReadWriter) Write(p []byte) (int, error) {
	return rw.out.Write(p)
}

func (rw *scriptedReadWriter) Output() string {
	return rw.out.String()
}

func TestPromptStringUsesDefaultOnEmptyInput(t *testing.T) {
	rw := newScriptedReadWriter("\n")
	prompts := newPromptSessionFromReadWriter(rw)

	got, err := promptString(context.Background(), prompts, "Label", "default")
	if err != nil {
		t.Fatalf("promptString returned error: %v", err)
	}
	if got != "default" {
		t.Fatalf("promptString = %q, want %q", got, "default")
	}
}

func TestPromptStringSupportsArrowEditing(t *testing.T) {
	rw := newScriptedReadWriter("mssxl\x1b[D\x1b[D\x1b[3~q\n")
	prompts := newPromptSessionFromReadWriter(rw)

	got, err := promptString(context.Background(), prompts, "Source engine", "")
	if err != nil {
		t.Fatalf("promptString returned error: %v", err)
	}
	if got != "mssql" {
		t.Fatalf("promptString = %q, want %q", got, "mssql")
	}
	if strings.Contains(rw.Output(), "^[[D") {
		t.Fatalf("prompt output leaked literal escape sequence text: %q", rw.Output())
	}
}

func TestPromptStringSupportsRightArrowEditing(t *testing.T) {
	rw := newScriptedReadWriter("abc\x1b[D\x1b[D\x1b[CX\n")
	prompts := newPromptSessionFromReadWriter(rw)

	got, err := promptString(context.Background(), prompts, "Label", "")
	if err != nil {
		t.Fatalf("promptString returned error: %v", err)
	}
	if got != "abXc" {
		t.Fatalf("promptString = %q, want %q", got, "abXc")
	}
}

func TestPromptSecretStringUsesHiddenInput(t *testing.T) {
	rw := newScriptedReadWriter("supersecret\n")
	prompts := newPromptSessionFromReadWriter(rw)

	got, err := promptSecretString(context.Background(), prompts, "S3 secret access key", "")
	if err != nil {
		t.Fatalf("promptSecretString returned error: %v", err)
	}
	if got != "supersecret" {
		t.Fatalf("promptSecretString = %q, want %q", got, "supersecret")
	}
	if strings.Contains(rw.Output(), "supersecret") {
		t.Fatalf("secret value was echoed in terminal output: %q", rw.Output())
	}
}

func TestPromptStringRejectsEOF(t *testing.T) {
	rw := newScriptedReadWriter("")
	prompts := newPromptSessionFromReadWriter(rw)

	_, err := promptString(context.Background(), prompts, "Label", "default")
	if !errors.Is(err, ErrPromptEOF) {
		t.Fatalf("promptString error = %v, want ErrPromptEOF", err)
	}
}

func TestPromptIntRejectsInvalidInput(t *testing.T) {
	rw := newScriptedReadWriter("abc\n")
	prompts := newPromptSessionFromReadWriter(rw)

	_, err := promptInt(context.Background(), prompts, "Number", 42)
	if !errors.Is(err, ErrPromptInvalid) {
		t.Fatalf("promptInt error = %v, want ErrPromptInvalid", err)
	}
}

func TestPromptBoolRejectsInvalidInput(t *testing.T) {
	rw := newScriptedReadWriter("maybe\n")
	prompts := newPromptSessionFromReadWriter(rw)

	_, err := promptBool(context.Background(), prompts, "Bool", true)
	if !errors.Is(err, ErrPromptInvalid) {
		t.Fatalf("promptBool error = %v, want ErrPromptInvalid", err)
	}
}

func TestPromptStringReturnsInterruptedOnCtrlC(t *testing.T) {
	rw := newScriptedReadWriter(string([]byte{keyCtrlC}))
	prompts := newPromptSessionFromReadWriter(rw)

	_, err := promptString(context.Background(), prompts, "Label", "default")
	if !errors.Is(err, ErrInterrupted) {
		t.Fatalf("promptString error = %v, want ErrInterrupted", err)
	}
}

func TestPromptStringFieldWrapsLongNotesAndDefaults(t *testing.T) {
	rw := newScriptedReadWriter("\n")
	prompts := newPromptSessionFromReadWriter(rw)

	got, err := promptStringField(context.Background(), prompts, promptFieldSpec{
		Label: "Postgres DSN",
		Note:  "Must be reachable from all workers.",
	}, "postgresql://myuser:mypassword@localhost:5432/mydb?sslmode=disable")
	if err != nil {
		t.Fatalf("promptStringField returned error: %v", err)
	}
	if want := "postgresql://myuser:mypassword@localhost:5432/mydb?sslmode=disable"; got != want {
		t.Fatalf("promptStringField = %q, want %q", got, want)
	}

	output := rw.Output()
	if !strings.Contains(output, "Postgres DSN") {
		t.Fatalf("prompt output missing label: %q", output)
	}
	if !strings.Contains(output, "Must be reachable from all workers.") {
		t.Fatalf("prompt output missing note: %q", output)
	}
	if !strings.Contains(output, "Default: postgresql://") {
		t.Fatalf("prompt output missing wrapped default: %q", output)
	}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if runeLen(line) > defaultWrapWidth {
			t.Fatalf("prompt line exceeds width %d: %q", defaultWrapWidth, line)
		}
	}
	if strings.Contains(output, "postgre\r\ns") {
		t.Fatalf("prompt output split a word mid-line: %q", output)
	}
}

func TestPromptSessionCloseRestoresOnceAndDisablesBracketedPaste(t *testing.T) {
	rw := newScriptedReadWriter("")
	prompts := newPromptSessionFromReadWriter(rw)

	restoreCalls := 0
	prompts.restore = func() error {
		restoreCalls++
		return nil
	}
	prompts.terminal.SetBracketedPasteMode(true)
	rw.out.Reset()

	if err := prompts.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if restoreCalls != 1 {
		t.Fatalf("restore called %d times, want 1", restoreCalls)
	}
	if !strings.Contains(rw.Output(), "\x1b[?2004l") {
		t.Fatalf("Close did not disable bracketed paste: %q", rw.Output())
	}

	rw.out.Reset()
	if err := prompts.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if restoreCalls != 1 {
		t.Fatalf("restore called %d times after second close, want 1", restoreCalls)
	}
	if rw.Output() != "" {
		t.Fatalf("second Close wrote unexpected output: %q", rw.Output())
	}
}
