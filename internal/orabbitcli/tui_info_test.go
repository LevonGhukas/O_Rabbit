package orabbitcli

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

// fakeInfoMaster serves two jobs, three runs and a diagnosis for r2.
func fakeInfoMaster(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/jobs":
			_, _ = w.Write([]byte(`[{"id":"j1","name":"orders","target_namespace":"shop","target_table":"orders"},{"id":"j2","name":"users"}]`))
		case "/api/v1/runs":
			_, _ = w.Write([]byte(`[{"id":"r3","job_id":"j2","status":"SUCCEEDED","started_at":"2026-11-25T14:02:14.816469347Z"},
				{"id":"r2","job_id":"j1","status":"FAILED","started_at":"2026-11-25T13:34:17Z","error_summary":"dial clickhouse://u:secret@db:9000 failed"},
				{"id":"r1","job_id":"j1","status":"SUCCEEDED","started_at":"2026-11-25T12:00:00Z"}]`))
		case "/api/v1/runs/r2":
			_, _ = w.Write([]byte(`{"run":{"id":"r2","status":"FAILED"},"tasks":[]}`))
		case "/api/v1/runs/r2/diagnosis":
			_, _ = w.Write([]byte(`{"run_id":"r2","status":"FAILED","task_counts":{"FAILED":1,"SUCCEEDED":3},
				"last_classified_error":"NETWORK_CONNECTION_FAILED","operator_review_required":false,
				"blocking_tasks":[{"task_index":2,"status":"FAILED","attempt_count":3,"max_attempts":3}],
				"suggested_next_action":"fix the source and start a new run"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestTUIDiagnoseAcceptsJobNameOrID(t *testing.T) {
	m := newTestTUIModel(fakeInfoMaster(t).URL)
	for _, ref := range []string{"r2", "j1", "orders"} {
		out := fmtMsg(m.diagnoseCmd(ref, false)())
		for _, want := range []string{"Diagnosis · run r2", "FAILED 1 · SUCCEEDED 3", "NETWORK_CONNECTION_FAILED", "task 2 FAILED, attempts 3/3", "fix the source"} {
			if !strings.Contains(out, want) {
				t.Errorf("/diagnose %s: missing %q in\n%s", ref, want, out)
			}
		}
		if ref != "r2" && !strings.Contains(out, "latest run of job orders") {
			t.Errorf("/diagnose %s should say which run was picked", ref)
		}
	}
	if res := m.diagnoseCmd("nope", false)().(tuiResultMsg); res.err == nil || !strings.Contains(res.err.Error(), "neither a run ID nor a job") {
		t.Errorf("unknown ref: %v", res.err)
	}
	if out := fmtMsg(m.diagnoseCmd("r2", true)()); !strings.Contains(out, `"run_id": "r2"`) {
		t.Errorf("--json should print raw JSON: %s", out)
	}
}

func TestTUIRunsShowsJobNamesAndFilters(t *testing.T) {
	m := newTestTUIModel(fakeInfoMaster(t).URL)
	out := fmtMsg(m.listRunsCmd(10, "")())
	for _, want := range []string{"orders", "users", "2026-11-25 14:02:14", "└ dial"} {
		if !strings.Contains(out, want) {
			t.Errorf("runs table missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "secret") {
		t.Error("runs table leaked a DSN password")
	}
	out = fmtMsg(m.listRunsCmd(10, "users")())
	if !strings.Contains(out, "r3") || strings.Contains(out, "r2 ") {
		t.Errorf("--job users kept the wrong runs:\n%s", out)
	}
}

func TestTUICommandHelp(t *testing.T) {
	m := newTestTUIModel("http://127.0.0.1:1")
	for line, want := range map[string]string{
		"/watch --help":       "/watch <run-id>",
		"/maintenance -h":     "--operation <op>",
		"/stack help":         "/stack start [master] [worker]",
		"/help diagnose":      "job name",
		"/recover r1 --help":  "--action <action>",
		"/help":               "Inspect",
		"/diagnose --help":    "--json",
		"/run --help":         "wizard",
		"/help /runs":         "--job",
		"/workers --help":     "--all",
		"/submit --help":      "orders.run.yaml",
		"/connect --help":     "http://",
		"/jobs --help":        "/jobs",
		"/show --help":        "tasks",
		"/cancel --help":      "confirmation",
		"/maintenance --help": "list prints nothing",
	} {
		_, cmd := m.dispatch(line)
		if got := printed(cmd); !strings.Contains(got, want) {
			t.Errorf("%q: missing %q in\n%s", line, want, got)
		}
	}
}

func TestParseRunsArgs(t *testing.T) {
	limit, job, err := parseRunsArgs([]string{"25", "--job", "orders"})
	if err != nil || limit != 25 || job != "orders" {
		t.Fatalf("got %d %q %v", limit, job, err)
	}
	if _, job, _ := parseRunsArgs([]string{"--job=j1"}); job != "j1" {
		t.Fatalf("--job= form: %q", job)
	}
	if _, _, err := parseRunsArgs([]string{"--job"}); err == nil {
		t.Fatal("--job without a value should fail")
	}
}

// Lines as wide as the terminal used to leave pieces of the prompt box on
// screen, so every printed line must stay narrower than the terminal.
func TestFitWidthKeepsLinesNarrower(t *testing.T) {
	long := strings.Repeat("x", 150) + "\nshort"
	for _, line := range strings.Split(fitWidth(long, 100), "\n") {
		if w := ansi.StringWidth(line); w >= 100 {
			t.Fatalf("line width %d >= terminal width", w)
		}
	}
}
