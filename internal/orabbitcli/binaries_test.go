package orabbitcli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveDaemonBinaryFromClientPathPrefersSibling(t *testing.T) {
	dir := t.TempDir()
	clientPath := filepath.Join(dir, "orabbit-client"+exeSuffix())
	siblingPath := filepath.Join(dir, masterBinarySpec.SiblingName)

	if err := os.WriteFile(clientPath, []byte("client"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(siblingPath, []byte("master"), 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := resolveDaemonBinaryFromClientPath(clientPath, masterBinarySpec, "")
	if err != nil {
		t.Fatalf("resolveDaemonBinaryFromClientPath returned error: %v", err)
	}
	if got != siblingPath {
		t.Fatalf("resolveDaemonBinaryFromClientPath = %q, want %q", got, siblingPath)
	}
}

func TestResolveDaemonBinaryFromClientPathMissingSiblingExplainsFix(t *testing.T) {
	dir := t.TempDir()
	clientPath := filepath.Join(dir, "orabbit-client"+exeSuffix())
	if err := os.WriteFile(clientPath, []byte("client"), 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := resolveDaemonBinaryFromClientPath(clientPath, workerBinarySpec, "")
	if err == nil {
		t.Fatal("expected missing sibling error")
	}
	msg := err.Error()
	for _, want := range []string{"missing " + workerBinarySpec.DisplayName, workerBinarySpec.FlagName, workerBinarySpec.BuildTarget} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected error to contain %q, got %q", want, msg)
		}
	}
}

func TestStageManagedBinaryCopiesIntoRuntimeDir(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "orabbit-worker"+exeSuffix())
	runtimeDir := filepath.Join(dir, "runtime")
	if err := os.WriteFile(sourcePath, []byte("worker-binary"), 0o755); err != nil {
		t.Fatal(err)
	}

	stagedPath, err := stageManagedBinary(sourcePath, runtimeDir, "orabbit-worker"+exeSuffix())
	if err != nil {
		t.Fatalf("stageManagedBinary returned error: %v", err)
	}
	if filepath.Dir(stagedPath) != runtimeDir {
		t.Fatalf("stageManagedBinary staged into %q, want dir %q", stagedPath, runtimeDir)
	}
	got, err := os.ReadFile(stagedPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "worker-binary" {
		t.Fatalf("staged binary contents = %q, want %q", string(got), "worker-binary")
	}
}
