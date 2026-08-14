package orabbitcli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCmdRunWatchStreamsExistingRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"run":{"id":"run-1","status":"RUNNING"},"tasks":[]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/sse":
			if got := strings.TrimSpace(r.URL.Query().Get("run_id")); got != "run-1" {
				http.Error(w, "unexpected run_id", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fl, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "streaming unsupported", http.StatusInternalServerError)
				return
			}
			fmt.Fprint(w, "data: {\"message\":\"task STARTED\",\"level\":\"INFO\",\"run_id\":\"run-1\",\"ts\":\"2026-01-01T00:00:00Z\",\"fields_json\":{}}\n\n")
			fl.Flush()
			fmt.Fprint(w, "data: {\"message\":\"run committed\",\"level\":\"INFO\",\"run_id\":\"run-1\",\"ts\":\"2026-01-01T00:00:01Z\",\"fields_json\":{}}\n\n")
			fmt.Fprint(w, "data: {\"message\":\"run SUCCEEDED\",\"level\":\"INFO\",\"run_id\":\"run-1\",\"ts\":\"2026-01-01T00:00:02Z\",\"fields_json\":{}}\n\n")
			fl.Flush()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdRun(context.Background(), []string{"watch", "run-1", "--master-http", server.URL})
	})

	if code != exitSuccess {
		t.Fatalf("exit code=%d want=%d stderr=%q stdout=%q", code, exitSuccess, stderr, stdout)
	}
	if !strings.Contains(stderr, "[client] watching run run-1 on "+server.URL) {
		t.Fatalf("expected watch status line in stderr, got %q", stderr)
	}
	for _, want := range []string{"task STARTED", "run committed", "run SUCCEEDED"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, stdout)
		}
	}
}

func TestCmdRunWatchInterruptStopsWatchingWithoutCancelingRun(t *testing.T) {
	var cancelCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"run":{"id":"run-1","status":"RUNNING"},"tasks":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/runs/run-1/cancel":
			cancelCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"run":{"id":"run-1","status":"CANCELED"},"canceled":true,"pending_tasks_canceled":1}`)
		case r.Method == http.MethodGet && r.URL.Path == "/sse":
			w.Header().Set("Content-Type", "text/event-stream")
			fl, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "streaming unsupported", http.StatusInternalServerError)
				return
			}
			fmt.Fprint(w, ": keepalive\n\n")
			fl.Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdRun(ctx, []string{"watch", "run-1", "--master-http", server.URL})
	})

	if code != exitInterrupted {
		t.Fatalf("exit code=%d want=%d stderr=%q stdout=%q", code, exitInterrupted, stderr, stdout)
	}
	if cancelCalls.Load() != 0 {
		t.Fatalf("watch interrupt should not cancel the run; cancel endpoint calls=%d", cancelCalls.Load())
	}
	if !strings.Contains(stderr, "[client] watching run run-1 on "+server.URL) {
		t.Fatalf("expected watch status line in stderr, got %q", stderr)
	}
}

func TestCmdRunCancelSendsExplicitCancelRequest(t *testing.T) {
	var cancelCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/runs/run-1/cancel":
			cancelCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"run":{"id":"run-1","status":"CANCELED"},"canceled":true,"pending_tasks_canceled":2}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdRun(context.Background(), []string{"cancel", "run-1", "--master-http", server.URL})
	})

	if code != exitSuccess {
		t.Fatalf("exit code=%d want=%d stderr=%q stdout=%q", code, exitSuccess, stderr, stdout)
	}
	if cancelCalls.Load() != 1 {
		t.Fatalf("cancel endpoint calls=%d want=1", cancelCalls.Load())
	}
	for _, want := range []string{"canceled run run-1", "pending tasks canceled: 2"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, stdout)
		}
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
}

func TestCmdRunDiagnosePrintsRedactedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/runs/run-1/diagnosis" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"run_id":"run-1","status":"COMMITTING","commit_phase":"INTENT","suggested_next_action":"request commit reconciliation","operator_review_required":false}`)
	}))
	defer server.Close()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdRun(context.Background(), []string{"diagnose", "run-1", "--master-http", server.URL})
	})
	if code != exitSuccess || strings.TrimSpace(stderr) != "" {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	for _, want := range []string{`"run_id": "run-1"`, `"commit_phase": "INTENT"`, `"suggested_next_action": "request commit reconciliation"`} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("missing %q in %s", want, stdout)
		}
	}
}

func TestCmdRunRecoverSendsActionAndReason(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/runs/run-1/recover" {
			http.NotFound(w, r)
			return
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if body["action"] != "registration_retry" || body["reason"] != "catalog recovered" {
			http.Error(w, "unexpected recovery body", http.StatusBadRequest)
			return
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"action":"registration_retry","changed":true,"status":"RETRY_REQUIRED","message":"registration retry is eligible"}`)
	}))
	defer server.Close()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdRun(context.Background(), []string{"recover", "run-1", "--master-http", server.URL, "--action", "registration_retry", "--reason", "catalog recovered"})
	})
	if code != exitSuccess || calls.Load() != 1 || strings.TrimSpace(stderr) != "" {
		t.Fatalf("code=%d calls=%d stdout=%q stderr=%q", code, calls.Load(), stdout, stderr)
	}
	if !strings.Contains(stdout, "registration_retry: status=RETRY_REQUIRED changed=true") {
		t.Fatalf("stdout=%q", stdout)
	}
}

func captureCommandOutput(t *testing.T, fn func() int) (code int, stdout string, stderr string) {
	t.Helper()

	oldStdout := os.Stdout
	oldStderr := os.Stderr

	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}

	os.Stdout = stdoutWriter
	os.Stderr = stderrWriter
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	stdoutDone := make(chan string, 1)
	stderrDone := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(stdoutReader)
		stdoutDone <- string(b)
	}()
	go func() {
		b, _ := io.ReadAll(stderrReader)
		stderrDone <- string(b)
	}()

	code = fn()

	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	stdout = <-stdoutDone
	stderr = <-stderrDone
	_ = stdoutReader.Close()
	_ = stderrReader.Close()
	return code, stdout, stderr
}
