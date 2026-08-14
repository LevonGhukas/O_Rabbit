package orabbitcli

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestEnsureLocalWorkersPassesWorkerLogSettings(t *testing.T) {
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
		t.Fatalf("write fake worker script: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/workers":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `[]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pg procGroup
	supervisor := newLocalSupervisor(tempDir, nil)
	err := ensureLocalWorkers(ctx, localWorkerConfig{
		GOCache:         tempDir,
		HTTPBase:        server.URL,
		GRPCAddr:        "127.0.0.1:9102",
		WorkerBin:       scriptPath,
		WorkerLogLevel:  "debug",
		WorkerLogFormat: "text",
	}, 1, supervisor, &pg, true)
	if err != nil {
		t.Fatalf("ensureLocalWorkers: %v", err)
	}
	defer func() {
		pg.signal(syscall.SIGTERM)
		pg.wait()
	}()

	deadline := time.Now().Add(2 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(argsPath)
		if err == nil {
			got = string(b)
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if got == "" {
		t.Fatalf("timed out waiting for worker args file %s", argsPath)
	}

	for _, want := range []string{
		"-master",
		"127.0.0.1:9102",
		"-worker-id",
		"local-01",
		"-insecure=true",
		"-log-level",
		"DEBUG",
		"-log-format",
		"text",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected worker args to contain %q, got %q", want, got)
		}
	}

	procs, _, err := listManagedProcesses(tempDir)
	if err != nil {
		t.Fatalf("listManagedProcesses: %v", err)
	}
	if len(procs) != 1 {
		t.Fatalf("managed processes=%d want=1", len(procs))
	}
	if procs[0].Kind != "worker" || procs[0].WorkerID != "local-01" || procs[0].MasterAddr != "127.0.0.1:9102" {
		t.Fatalf("unexpected managed worker entry: %+v", procs[0])
	}
}
