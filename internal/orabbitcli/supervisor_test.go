package orabbitcli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLocalSupervisorStopPIDsCleansManagedState(t *testing.T) {
	tempDir := t.TempDir()
	scriptPath := filepath.Join(tempDir, "supervised-worker")
	script := `#!/bin/sh
trap 'exit 0' TERM INT
while :; do
  sleep 1
done
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	supervisor := newLocalSupervisor(tempDir, nil)
	proc, err := supervisor.startWorker(localWorkerLaunchSpec{
		BinaryPath: scriptPath,
		MasterAddr: "127.0.0.1:9102",
		WorkerID:   "local-01",
		Insecure:   true,
	})
	if err != nil {
		t.Fatalf("startWorker: %v", err)
	}
	defer func() {
		proc.signal(syscall.SIGKILL)
		_ = proc.wait()
	}()

	if err := waitForManagedProcessCount(tempDir, 1, 2*time.Second); err != nil {
		t.Fatalf("waitForManagedProcessCount: %v", err)
	}

	action, err := supervisor.stopPIDs(context.Background(), []int{proc.pid}, false, 2*time.Second)
	if err != nil {
		t.Fatalf("stopPIDs: %v", err)
	}
	if action != "stopped" && action != "killed" {
		t.Fatalf("stop action=%q", action)
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- proc.wait() }()
	select {
	case err := <-waitDone:
		if err != nil && !strings.Contains(err.Error(), "terminated") {
			t.Fatalf("proc wait: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for supervised process %d to exit", proc.pid)
	}

	procs, _, err := supervisor.listManagedProcesses()
	if err != nil {
		t.Fatalf("listManagedProcesses: %v", err)
	}
	if len(procs) != 0 {
		t.Fatalf("expected managed state cleanup, got %+v", procs)
	}
}

func TestCmdStartWorkerUsesSupervisorManagedState(t *testing.T) {
	tempDir := t.TempDir()
	argsPath := filepath.Join(tempDir, "worker-args.txt")
	scriptPath := filepath.Join(tempDir, "fake-worker")
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$@" > %q
trap 'exit 0' TERM INT
while :; do
  sleep 1
done
`, argsPath)
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake worker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan int, 1)
	go func() {
		done <- cmdStart(ctx, []string{
			"worker",
			"--master-addr", "127.0.0.1:9102",
			"--worker-bin", scriptPath,
			"--gocache", tempDir,
			"--quiet",
		})
	}()

	if err := waitForManagedProcessCount(tempDir, 1, 2*time.Second); err != nil {
		t.Fatalf("waitForManagedProcessCount: %v", err)
	}

	procs, _, err := listManagedProcesses(tempDir)
	if err != nil {
		t.Fatalf("listManagedProcesses: %v", err)
	}
	if len(procs) != 1 {
		t.Fatalf("managed processes=%d want=1", len(procs))
	}
	if procs[0].Kind != "worker" || procs[0].WorkerID != "local-01" {
		t.Fatalf("unexpected managed worker: %+v", procs[0])
	}
	if err := waitForFile(argsPath, 2*time.Second); err != nil {
		t.Fatalf("waitForFile(worker args): %v", err)
	}

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdStop(context.Background(), []string{"--gocache", tempDir})
	})
	if code != exitSuccess {
		t.Fatalf("cmdStop exit=%d want=%d stdout=%q stderr=%q", code, exitSuccess, stdout, stderr)
	}
	if !strings.Contains(stdout, "stopped 1 process") && !strings.Contains(stdout, "killed 1 process") {
		t.Fatalf("expected stop output, got %q", stdout)
	}
	if strings.TrimSpace(stderr) != "" && !strings.Contains(stderr, "worker exited:") {
		t.Fatalf("unexpected stderr %q", stderr)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for cmdStart to exit after stop")
	}

	argsRaw, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read worker args: %v", err)
	}
	got := string(argsRaw)
	for _, want := range []string{"-master", "127.0.0.1:9102", "-worker-id", "local-01"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected worker args to contain %q, got %q", want, got)
		}
	}
}

func waitForManagedProcessCount(runtimeDir string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		procs, _, err := listManagedProcesses(runtimeDir)
		if err != nil {
			return err
		}
		if len(procs) == want {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %d managed processes in %s", want, runtimeDir)
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, err := os.Stat(path)
		if err == nil && info.Size() > 0 {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for file %s", path)
}
