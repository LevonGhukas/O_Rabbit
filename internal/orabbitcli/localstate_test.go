package orabbitcli

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCmdStackStatusUsesManagedStateBeforeFallback(t *testing.T) {
	tempDir := t.TempDir()
	if err := registerManagedProcess(tempDir, procInfo{
		PID:        os.Getpid(),
		Kind:       "worker",
		Command:    "synthetic-managed-worker",
		MasterAddr: "127.0.0.1:9102",
		WorkerID:   "local-01",
		Insecure:   true,
	}, "/tmp/fake-worker"); err != nil {
		t.Fatalf("registerManagedProcess: %v", err)
	}

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdStackStatus(context.Background(), []string{
			"--gocache", tempDir,
			"--http-base", localHTTPBase(defaultHTTPAddr),
			"--json",
		})
	})
	if code != exitSuccess {
		t.Fatalf("cmdStackStatus exit=%d want=%d stderr=%q", code, exitSuccess, stderr)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var report stackStatusJSONReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal stdout: %v\nstdout=%s", err, stdout)
	}
	if report.Source != stackStatusJSONSourceManaged {
		t.Fatalf("source=%q want=%q", report.Source, stackStatusJSONSourceManaged)
	}
	if len(report.Items) != 1 {
		t.Fatalf("items=%d want=1", len(report.Items))
	}
	item := report.Items[0]
	if item.PID == nil || *item.PID != os.Getpid() {
		t.Fatalf("pid=%v want=%d", item.PID, os.Getpid())
	}
	if item.Command == nil || *item.Command != "synthetic-managed-worker" {
		t.Fatalf("command=%v", item.Command)
	}
	if item.WorkerID == nil || *item.WorkerID != "local-01" {
		t.Fatalf("worker_id=%v", item.WorkerID)
	}
}

func TestListManagedProcessesPrunesStaleEntries(t *testing.T) {
	tempDir := t.TempDir()
	supervisor := newLocalSupervisor(tempDir, nil)
	if err := registerManagedProcess(tempDir, procInfo{
		PID:        999999,
		Kind:       "worker",
		Command:    "stale-worker",
		MasterAddr: "127.0.0.1:9102",
		WorkerID:   "local-01",
		Insecure:   true,
	}, "/tmp/fake-worker"); err != nil {
		t.Fatalf("registerManagedProcess: %v", err)
	}

	procs, removed, err := supervisor.listManagedProcesses()
	if err != nil {
		t.Fatalf("supervisor.listManagedProcesses: %v", err)
	}
	if len(procs) != 0 {
		t.Fatalf("expected stale entry to be pruned, got %+v", procs)
	}
	if removed != 1 {
		t.Fatalf("removed=%d want=1", removed)
	}

	raw, err := os.ReadFile(managedProcessStatePath(tempDir))
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var state managedProcessState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}
	if len(state.Processes) != 0 {
		t.Fatalf("expected persisted stale entry cleanup, got %+v", state.Processes)
	}
}

func TestCmdStopUsesManagedStateBeforeFallback(t *testing.T) {
	tempDir := t.TempDir()
	scriptPath := filepath.Join(tempDir, "state-only-worker")
	script := `#!/bin/sh
trap 'exit 0' TERM INT
while :; do
  sleep 1
done
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	cmd := exec.Command(scriptPath)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start script: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGKILL)
		}
	}()

	if err := registerManagedProcess(tempDir, procInfo{
		PID:        cmd.Process.Pid,
		Kind:       "worker",
		Command:    scriptPath,
		MasterAddr: "127.0.0.1:9102",
		WorkerID:   "local-01",
		Insecure:   true,
	}, scriptPath); err != nil {
		t.Fatalf("registerManagedProcess: %v", err)
	}

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdStop(context.Background(), []string{"--gocache", tempDir})
	})
	if code != exitSuccess {
		t.Fatalf("cmdStop exit=%d want=%d stdout=%q stderr=%q", code, exitSuccess, stdout, stderr)
	}
	if !strings.Contains(stdout, "stopped 1 process") && !strings.Contains(stdout, "killed 1 process") {
		t.Fatalf("expected stop/kill output, got %q", stdout)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case err := <-waitDone:
		if err != nil && !strings.Contains(err.Error(), "terminated") {
			t.Fatalf("worker wait: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for managed process %d to exit", cmd.Process.Pid)
	}

	procs, _, err := listManagedProcesses(tempDir)
	if err != nil {
		t.Fatalf("listManagedProcesses: %v", err)
	}
	if len(procs) != 0 {
		t.Fatalf("expected state cleanup after stop, got %+v", procs)
	}
}
