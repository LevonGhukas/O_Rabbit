package tui

import (
	"github.com/LevonGhukas/O_Rabbit/internal/orabbitcli"

	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// These tests drive the model the way Bubble Tea would: feed a message to
// Update, then inspect the returned model and run the returned command.

func newTestModel(base string) model {
	return newModel(context.Background(), base, false)
}

func update(t *testing.T, m model, msg tea.Msg) (model, tea.Cmd) {
	t.Helper()
	next, cmd := m.Update(msg)
	tm, ok := next.(model)
	if !ok {
		t.Fatalf("Update returned %T, want model", next)
	}
	return tm, cmd
}

func typeLine(t *testing.T, m model, line string) (model, tea.Cmd) {
	t.Helper()
	m.input.SetValue(line)
	return update(t, m, tea.KeyMsg{Type: tea.KeyEnter})
}

// isQuit reports whether cmd (possibly a batch or sequence) asks to quit.
func isQuit(cmd tea.Cmd) bool {
	if cmd == nil {
		return false
	}
	_, ok := cmd().(tea.QuitMsg)
	return ok
}

func TestCtrlCEscalates(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	ctrlC := tea.KeyMsg{Type: tea.KeyCtrlC}

	m.watch = &watchState{runID: "r1"}
	m.input.SetValue("draft")
	m, cmd := update(t, m, ctrlC)
	if m.watch != nil || isQuit(cmd) {
		t.Fatal("first Ctrl+C should stop the watch, not quit")
	}
	if m.input.Value() != "draft" {
		t.Fatal("stopping a watch must keep the typed line")
	}

	m, cmd = update(t, m, ctrlC)
	if m.input.Value() != "" || isQuit(cmd) {
		t.Fatal("second Ctrl+C should clear the line, not quit")
	}

	_, cmd = update(t, m, ctrlC)
	if !isQuit(cmd) {
		t.Fatal("Ctrl+C on an empty prompt should quit")
	}
}

func TestEscDoesNotQuit(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	if _, cmd := update(t, m, tea.KeyMsg{Type: tea.KeyEsc}); isQuit(cmd) {
		t.Fatal("Esc on an empty prompt must not quit")
	}
}

func TestCancelRequiresConfirmation(t *testing.T) {
	var canceled int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/runs/r1/cancel" {
			canceled++
			_, _ = w.Write([]byte(`{"run":{"id":"r1","status":"CANCELED"},"canceled":true,"pending_tasks_canceled":2}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	m := newTestModel(srv.URL)
	m, _ = typeLine(t, m, "/cancel r1")
	if m.pendingCancel != "r1" {
		t.Fatalf("pendingCancel = %q, want r1", m.pendingCancel)
	}

	// Anything but y/yes aborts without calling the master.
	aborted, _ := typeLine(t, m, "n")
	if aborted.pendingCancel != "" || aborted.busy != "" {
		t.Fatal("'n' should abort the cancel")
	}
	if canceled != 0 {
		t.Fatal("master was called although the user declined")
	}

	m, cmd := typeLine(t, m, "yes")
	if m.busy == "" || cmd == nil {
		t.Fatal("'yes' should start the cancel request")
	}
	msg := cmd()
	res, ok := msg.(resultMsg)
	if !ok || res.err != nil || !strings.Contains(res.text, "canceled=true") {
		t.Fatalf("cancel result = %#v", msg)
	}
	if canceled != 1 {
		t.Fatalf("cancel calls = %d, want 1", canceled)
	}
}

func TestWatchFirstErrorStops(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m.watch = &watchState{runID: "r1"}
	m, _ = update(t, m, watchMsg{runID: "r1", err: errors.New("http 404: not found")})
	if m.watch != nil {
		t.Fatal("a failing first fetch should stop the watch")
	}
}

func TestWatchLaterErrorRetries(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m.watch = &watchState{runID: "r1", loaded: true, status: "RUNNING"}
	m, cmd := update(t, m, watchMsg{runID: "r1", err: errors.New("connection reset")})
	if m.watch == nil || m.watch.lastErr == nil || cmd == nil {
		t.Fatal("a mid-watch error should keep watching and schedule a retry")
	}
	if !strings.Contains(m.View(), "retrying") {
		t.Fatal("view should show the retry")
	}
}

func TestWatchProgressAndCompletion(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m.watch = &watchState{runID: "r1"}

	running := orabbitcli.RunDetails{Run: orabbitcli.RunState{ID: "r1", Status: "RUNNING"}, Tasks: []orabbitcli.TaskInfo{{Status: "SUCCEEDED"}, {Status: "RUNNING"}}}
	m, cmd := update(t, m, watchMsg{runID: "r1", details: running})
	if m.watch == nil || !m.watch.loaded || cmd == nil {
		t.Fatal("a running run should stay watched and schedule the next poll")
	}
	if v := m.View(); !strings.Contains(v, "1/2 tasks") || !strings.Contains(v, "█") {
		t.Fatalf("view lacks progress: %q", v)
	}

	done := orabbitcli.RunDetails{Run: orabbitcli.RunState{ID: "r1", Status: "SUCCEEDED"}, Tasks: []orabbitcli.TaskInfo{{Status: "SUCCEEDED"}, {Status: "SUCCEEDED"}}}
	m, _ = update(t, m, watchMsg{runID: "r1", details: done})
	if m.watch != nil {
		t.Fatal("a terminal status should end the watch")
	}
}

func TestWatchIgnoresStaleMessages(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m.watch = &watchState{runID: "new"}
	m, cmd := update(t, m, watchMsg{runID: "old", details: orabbitcli.RunDetails{Run: orabbitcli.RunState{Status: "SUCCEEDED"}}})
	if m.watch == nil || m.watch.runID != "new" || cmd != nil {
		t.Fatal("a message for a previous watch must not affect the current one")
	}
	if _, cmd := update(t, m, watchTickMsg{runID: "old"}); cmd != nil {
		t.Fatal("a tick for a previous watch must not poll")
	}
}

func TestBusyKeepsInput(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m.busy = "loading runs"
	m, _ = typeLine(t, m, "/status")
	if m.input.Value() != "/status" {
		t.Fatal("input typed while busy should be kept, not dropped")
	}
	if len(m.history) != 0 {
		t.Fatal("rejected input should not enter history")
	}
}

func TestUnknownCommandSuggests(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	_, cmd := m.dispatch("/wach r1")
	if cmd == nil {
		t.Fatal("expected an error message command")
	}
	// tea.Println returns an unexported message type; check its text via %v.
	if got := printed(cmd); !strings.Contains(got, "did you mean /watch?") {
		t.Fatalf("message = %q", got)
	}
}

func TestUsageErrors(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	for _, line := range []string{"/watch", "/watch a b", "/cancel", "/runs x y", "/connect", "/submit"} {
		next, cmd := m.dispatch(line)
		tm := next.(model)
		if tm.watch != nil || tm.pendingCancel != "" || tm.busy != "" {
			t.Errorf("%q changed state on a usage error", line)
		}
		if got := printed(cmd); !strings.Contains(got, "usage:") {
			t.Errorf("%q printed %q, want a usage line", line, got)
		}
	}
	if got := printed(func() tea.Cmd { _, c := m.dispatch("/runs 0"); return c }()); !strings.Contains(got, "positive") {
		t.Errorf("/runs 0 printed %q", got)
	}
}

func TestHistoryNavigation(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m.pushHistory("/runs")
	m.pushHistory("/status")
	m.pushHistory("/status") // consecutive duplicates are stored once
	if len(m.history) != 2 {
		t.Fatalf("history = %v", m.history)
	}
	up := tea.KeyMsg{Type: tea.KeyUp}
	down := tea.KeyMsg{Type: tea.KeyDown}
	m, _ = update(t, m, up)
	m, _ = update(t, m, up)
	if m.input.Value() != "/runs" {
		t.Fatalf("after two ups input = %q", m.input.Value())
	}
	m, _ = update(t, m, up) // stays at the oldest entry
	m, _ = update(t, m, down)
	m, _ = update(t, m, down)
	if m.input.Value() != "" {
		t.Fatalf("past the newest entry input = %q, want empty", m.input.Value())
	}
}

func TestListRuns(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs" || r.URL.Query().Get("limit") != "5" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`[{"id":"run-a","job_id":"job-1","status":"SUCCEEDED","started_at":"2026-11-25T10:00:00Z"}]`))
	}))
	defer srv.Close()

	m := newTestModel(srv.URL)
	m, cmd := m.dispatchModel(t, "/runs 5")
	if m.busy == "" {
		t.Fatal("/runs should mark the shell busy")
	}
	res, ok := cmd().(resultMsg)
	if !ok || res.err != nil || !strings.Contains(res.text, "run-a") || !strings.Contains(res.text, "job-1") {
		t.Fatalf("result = %#v", res)
	}
	m, _ = update(t, m, res)
	if m.busy != "" {
		t.Fatal("a result should clear the busy state")
	}
}

func TestStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			_, _ = w.Write([]byte("ok"))
		case "/api/v1/workers":
			_, _ = w.Write([]byte(`[{"id":"w1"},{"id":"w2"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	m := newTestModel(srv.URL)
	msg, ok := m.statusCmd()().(statusMsg)
	if !ok || !msg.healthy || msg.workers != 2 || msg.err != nil {
		t.Fatalf("status = %#v", msg)
	}
	m, _ = update(t, m, msg)
	if m.masterHealthy == nil || !*m.masterHealthy {
		t.Fatal("model should record a healthy master")
	}

	down := newTestModel("http://127.0.0.1:1") // nothing listens on port 1
	if msg := down.statusCmd()().(statusMsg); msg.healthy {
		t.Fatal("unreachable master reported healthy")
	}
}

func TestConnectMarksExplicit(t *testing.T) {
	m := newTestModel("http://127.0.0.1:9100")
	m, _ = m.dispatchModel(t, "/connect master.example.internal:9100")
	if !m.explicit || !strings.Contains(m.base, "master.example.internal:9100") {
		t.Fatalf("base=%q explicit=%v", m.base, m.explicit)
	}
}

func TestSuggestionsMenu(t *testing.T) {
	m := newTestModel("http://127.0.0.1:1")
	m.input.SetValue("/wa")
	if s := m.renderSuggestions(); !strings.Contains(s, "/watch") || strings.Contains(s, "/status") {
		t.Fatalf("suggestions for /wa = %q", s)
	}
	m.input.SetValue("/watch r1")
	if s := m.renderSuggestions(); s != "" {
		t.Fatal("menu should hide once arguments are typed")
	}
}

// dispatchModel is dispatch with the model type asserted, for brevity.
func (m model) dispatchModel(t *testing.T, line string) (model, tea.Cmd) {
	t.Helper()
	next, cmd := m.dispatch(line)
	return next.(model), cmd
}

// printed runs a tea.Println command and returns the text it would print.
func printed(cmd tea.Cmd) string {
	if cmd == nil {
		return ""
	}
	return strings.TrimSpace(fmtMsg(cmd()))
}

// fmtMsg renders a message with %v; tea.Println's message type is
// unexported, but its printed text shows up in the formatted struct.
func fmtMsg(msg tea.Msg) string {
	return fmt.Sprintf("%v", msg)
}
