//go:build unix

package icebergreg

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// A launcher that starts the JVM without exec leaves a grandchild behind; the
// deadline must kill the whole process group so a late catalog commit cannot
// land after the attempt was already recorded as failed.
func TestRunIceCLIRegisterKillsIceProcessGroupOnDeadline(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	// Write the PID atomically so the test never reads a half-written file.
	script := "#!/bin/sh\nsleep 600 &\necho $! > " + pidFile + ".tmp\nmv " + pidFile + ".tmp " + pidFile + "\nwait\n"
	// The attempt ends once the grandchild exists, not after a fixed time:
	// starting a process can take longer than any short fixed deadline on a
	// loaded machine or under the race detector. exec.CommandContext kills
	// through cmd.Cancel for a deadline and a cancel alike; the timeout is
	// only a safety net.
	ctx, cancel := context.WithTimeout(context.Background(), iceTestReturnTimeout)
	defer cancel()
	var canceledAt atomic.Int64
	go func() {
		for ctx.Err() == nil {
			if _, err := os.Stat(pidFile); err == nil {
				canceledAt.Store(time.Now().UnixNano())
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	err := runFakeIce(t, ctx, script, s3io.Config{Region: "us-east-1"}, iceTestRunConfig(), []icebergObj{{key: "a.parquet"}})
	returned := time.Now()

	if err == nil {
		t.Fatal("expected error")
	}
	if canceledAt.Load() == 0 {
		t.Fatal("fake ice never started its grandchild")
	}
	if elapsed := returned.Sub(time.Unix(0, canceledAt.Load())); elapsed >= iceWaitDelay {
		t.Fatalf("returned %v after the attempt ended; grandchild kept ice's pipes open until WaitDelay", elapsed)
	}
	raw, readErr := os.ReadFile(pidFile)
	if readErr != nil {
		t.Fatalf("read child pid: %v", readErr)
	}
	pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
	if convErr != nil {
		t.Fatalf("parse child pid: %v", convErr)
	}
	deadline := time.Now().Add(time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			t.Fatalf("ice grandchild %d survived the deadline", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
