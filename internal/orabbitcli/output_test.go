package orabbitcli

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestValidateOutputModeRejectsQuietAndVerbose(t *testing.T) {
	if err := validateOutputMode(true, true); err == nil {
		t.Fatal("expected quiet+verbose to be rejected")
	}
}

func TestFollowCommandLogPrefixesDaemonLines(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout bytes.Buffer
	var stderr safeBuffer
	out := &cliOutput{stdout: &stdout, stderr: &stderr}

	att, err := newCommandLogAttachment(t.TempDir(), "master")
	if err != nil {
		t.Fatalf("newCommandLogAttachment: %v", err)
	}
	defer att.closeParentWriter()
	defer att.stopFollowing()

	out.followCommandLog(ctx, "master", att)
	if _, err := att.writer.WriteString("hello\nworld\n"); err != nil {
		t.Fatalf("write log: %v", err)
	}
	if err := att.writer.Sync(); err != nil {
		t.Fatalf("sync log: %v", err)
	}

	waitForOutput(t, &stderr, "[master] hello")
	waitForOutput(t, &stderr, "[master] world")

	cancel()
	att.stopFollowing()
	out.wait()
}

func TestFollowCommandLogRespectsQuietMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	out := &cliOutput{quiet: true, stdout: &stdout, stderr: &stderr}

	att, err := newCommandLogAttachment(t.TempDir(), "worker:local-01")
	if err != nil {
		t.Fatalf("newCommandLogAttachment: %v", err)
	}
	defer att.closeParentWriter()
	defer att.stopFollowing()

	out.followCommandLog(ctx, "worker:local-01", att)
	if _, err := att.writer.WriteString("should stay hidden\n"); err != nil {
		t.Fatalf("write log: %v", err)
	}
	if err := att.writer.Sync(); err != nil {
		t.Fatalf("sync log: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	cancel()
	att.stopFollowing()
	out.wait()

	if got := strings.TrimSpace(stderr.String()); got != "" {
		t.Fatalf("expected quiet mode to suppress daemon logs, got %q", got)
	}
}

func TestFollowCommandLogFlushesFinalLinesOnStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stderr bytes.Buffer
	out := &cliOutput{stderr: &stderr, stdout: io.Discard}

	att, err := newCommandLogAttachment(t.TempDir(), "worker:local-01")
	if err != nil {
		t.Fatalf("newCommandLogAttachment: %v", err)
	}
	defer att.closeParentWriter()

	out.followCommandLog(ctx, "worker:local-01", att)
	if _, err := att.writer.WriteString("fast-exit-line\n"); err != nil {
		t.Fatalf("write log: %v", err)
	}
	if err := att.writer.Sync(); err != nil {
		t.Fatalf("sync log: %v", err)
	}
	att.stopFollowing()
	out.wait()

	if !strings.Contains(stderr.String(), "[worker:local-01] fast-exit-line") {
		t.Fatalf("expected final line to flush on stop, got %q", stderr.String())
	}
}

func waitForOutput(t *testing.T, buf interface{ String() string }, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in %q", want, buf.String())
}
