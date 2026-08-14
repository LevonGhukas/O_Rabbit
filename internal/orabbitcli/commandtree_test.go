package orabbitcli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func assertNoWrappedLineLongerThan(t *testing.T, output string, width int) {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if runeLen(line) > width {
			t.Fatalf("line exceeds width %d: %q", width, line)
		}
	}
}

func TestMainStackStatusDispatchesToManagedState(t *testing.T) {
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
		return Main([]string{
			"stack", "status",
			"--gocache", tempDir,
			"--http-base", localHTTPBase(defaultHTTPAddr),
			"--json",
		})
	})
	if code != exitSuccess {
		t.Fatalf("Main exit=%d want=%d stderr=%q", code, exitSuccess, stderr)
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
}

func TestMainRunInteractiveHelp(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return Main([]string{"run", "interactive", "--help"})
	})
	if code != exitSuccess {
		t.Fatalf("Main exit=%d want=%d stdout=%q stderr=%q", code, exitSuccess, stdout, stderr)
	}
	if !strings.Contains(stdout, "orabbit-client run interactive") {
		t.Fatalf("expected interactive help, got %q", stdout)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
}

func TestMainRunSubmitDispatches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/connections":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `[]`)
		case r.Method == http.MethodPost && r.URL.Path == "/connections":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"id":"conn-1"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/jobs":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `[]`)
		case r.Method == http.MethodPost && r.URL.Path == "/jobs":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"id":"job-1"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/jobs/job-1/runs":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"run":{"id":"run-1"},"tasks":[{}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	specPath := writeTempRunSubmitFile(t, "run-submit-main-*.yaml", fmt.Sprintf(`
master:
  http: %s
source:
  name: mssql-prod
  engine: mssql
  dsn: sqlserver://sa:pass@example:1433?database=SalesDB
target:
  name: minio-lake
  endpoint: http://localhost:9000
  region: us-east-1
  bucket: bucket1
  prefix: exports/orders
  force_path_style: true
  access_key_id: minioadmin
  secret_access_key: minioadmin
job:
  name: orders-export
  target_namespace: analytics
  target_table: orders
  write_mode: append
  incremental: false
  table: SalesDB.dbo.Orders
  id_column: RowId
  auto_tune: true
`, server.URL))

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return Main([]string{"run", "submit", "--file", specPath})
	})
	if code != exitSuccess {
		t.Fatalf("Main exit=%d want=%d stderr=%q stdout=%q", code, exitSuccess, stderr, stdout)
	}
	if !strings.Contains(stdout, "submitted run run-1") {
		t.Fatalf("expected stdout to contain submitted run line, got %q", stdout)
	}
}

func TestMainRunWatchDispatches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"run":{"id":"run-1","status":"RUNNING"},"tasks":[]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/sse":
			w.Header().Set("Content-Type", "text/event-stream")
			fl, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "streaming unsupported", http.StatusInternalServerError)
				return
			}
			fmt.Fprint(w, "data: {\"message\":\"task STARTED\",\"level\":\"INFO\",\"run_id\":\"run-1\",\"ts\":\"2026-01-01T00:00:00Z\",\"fields_json\":{}}\n\n")
			fmt.Fprint(w, "data: {\"message\":\"run committed\",\"level\":\"INFO\",\"run_id\":\"run-1\",\"ts\":\"2026-01-01T00:00:01Z\",\"fields_json\":{}}\n\n")
			fmt.Fprint(w, "data: {\"message\":\"run SUCCEEDED\",\"level\":\"INFO\",\"run_id\":\"run-1\",\"ts\":\"2026-01-01T00:00:02Z\",\"fields_json\":{}}\n\n")
			fl.Flush()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return Main([]string{"run", "watch", "run-1", "--master-http", server.URL})
	})
	if code != exitSuccess {
		t.Fatalf("Main exit=%d want=%d stderr=%q stdout=%q", code, exitSuccess, stderr, stdout)
	}
	if !strings.Contains(stderr, "[client] watching run run-1 on "+server.URL) {
		t.Fatalf("expected watch status line in stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "run SUCCEEDED") {
		t.Fatalf("expected stdout to contain terminal event, got %q", stdout)
	}
}

func TestMainRunWithoutSubcommandShowsGroupHelp(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return Main([]string{"run"})
	})
	if code != exitSuccess {
		t.Fatalf("Main exit=%d want=%d stdout=%q stderr=%q", code, exitSuccess, stdout, stderr)
	}
	if !strings.Contains(stdout, "orabbit-client run - interactive and non-interactive run operations") {
		t.Fatalf("expected run group help, got %q", stdout)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
}

func TestMainRemovedLegacyStartAliasErrors(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return Main([]string{"start"})
	})
	if code != exitUsage {
		t.Fatalf("Main exit=%d want=%d stdout=%q stderr=%q", code, exitUsage, stdout, stderr)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if !strings.Contains(stderr, `unknown command "start"`) {
		t.Fatalf("expected unknown command error, got %q", stderr)
	}
}

func TestHelpStackStartTopic(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdHelp(context.Background(), []string{"stack", "start"})
	})
	if code != exitSuccess {
		t.Fatalf("cmdHelp exit=%d want=%d stdout=%q stderr=%q", code, exitSuccess, stdout, stderr)
	}
	if !strings.Contains(stdout, "orabbit-client stack start") {
		t.Fatalf("expected stack start help, got %q", stdout)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
}

func TestHelpStackStartReadableAtDefaultWidth(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdHelp(context.Background(), []string{"stack", "start"})
	})
	if code != exitSuccess {
		t.Fatalf("cmdHelp exit=%d want=%d stdout=%q stderr=%q", code, exitSuccess, stdout, stderr)
	}
	assertNoWrappedLineLongerThan(t, stdout, defaultWrapWidth)
	if !strings.Contains(stdout, "--master-bin/--worker-bin") {
		t.Fatalf("expected combined flag token to stay intact, got %q", stdout)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
}
