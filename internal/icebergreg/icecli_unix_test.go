//go:build unix

package icebergreg

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	script := "#!/bin/sh\nsleep 600 &\necho $! > " + pidFile + "\nwait\n"
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := runFakeIce(t, ctx, script, s3io.Config{Region: "us-east-1"}, iceTestRunConfig(), []icebergObj{{key: "a.parquet"}})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error")
	}
	if elapsed >= iceWaitDelay {
		t.Fatalf("returned after %v; grandchild kept ice's pipes open until WaitDelay", elapsed)
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
