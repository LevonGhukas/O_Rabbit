package orabbitcli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

var (
	keyEnter = tea.KeyMsg{Type: tea.KeyEnter}
	keyEsc   = tea.KeyMsg{Type: tea.KeyEsc}
	keyDown  = tea.KeyMsg{Type: tea.KeyDown}
)

// clearWizardEnv keeps the machine's ORABBIT_DEFAULT_S3_* settings out of
// the tests' defaults.
func clearWizardEnv(t *testing.T) {
	for _, k := range []string{"ORABBIT_DEFAULT_S3_ENDPOINT", "ORABBIT_DEFAULT_S3_ACCESS_KEY_ID", "ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY", "ORABBIT_DEFAULT_S3_REGION"} {
		t.Setenv(k, "")
	}
}

// answer fills the current field: text is typed, a select option is
// highlighted by its text. Then Enter is pressed.
func answer(t *testing.T, w *runWizard, value string) {
	t.Helper()
	f := w.field()
	if f.Kind == wizSelect {
		found := false
		for i, o := range f.Options {
			if o == value {
				w.choice, found = i, true
			}
		}
		if !found {
			t.Fatalf("%s: no option %q in %v", f.Key, value, f.Options)
		}
	} else if value != "" {
		w.input.SetValue(value)
	}
	if action, _ := w.handleKey(keyEnter); action != wizContinue && f.Key != wkReview {
		t.Fatalf("%s: unexpected action %v", f.Key, action)
	}
	if w.err != "" {
		t.Fatalf("%s: %s", f.Key, w.err)
	}
}

// fillClickHouse answers every question for the ex-clickhouse example,
// accepting the proposed defaults where they are good.
func fillClickHouse(t *testing.T, w *runWizard) {
	t.Helper()
	steps := []struct{ key, value string }{
		{wkEngine, "clickhouse"},
		{wkDSN, "clickhouse://default:clickhouse@ex-clickhouse:9000/default"},
		{wkSourceName, ""}, // default: clickhouse
		{wkTable, "default.orders"},
		{wkCursor, "id"},
		{wkIncr, optFullExport},
		{wkEndpoint, "http://minio:9000"},
		{wkBucket, "orabbit"},
		{wkAccessKey, "minio-user"},
		{wkSecretKey, "minio-secret"},
		{wkRegion, ""}, // default: us-east-1
		{wkPathStyle, optYes},
		{wkJobName, ""},   // default: clickhouse-orders
		{wkNamespace, ""}, // default: clickhouse
		{wkTargetTbl, ""}, // default: orders
		{wkIceberg, optYes},
		{wkIceEngine, "rest-go"},
		{wkIceURI, "http://iceberg-rest:8181"},
		{wkIceTable, ""}, // default: clickhouse.orders
	}
	for _, s := range steps {
		if got := w.field().Key; got != s.key {
			t.Fatalf("expected question %q, got %q", s.key, got)
		}
		answer(t, w, s.value)
	}
	if w.field().Key != wkReview {
		t.Fatalf("expected the review, got %q", w.field().Key)
	}
}

func TestWizardBuildsValidSpec(t *testing.T) {
	clearWizardEnv(t)
	w := newRunWizard(nil)
	fillClickHouse(t, w)

	spec, err := buildWizardSpec(w.answers, "http://127.0.0.1:9100")
	if err != nil {
		t.Fatalf("spec invalid: %v", err)
	}
	checks := map[string][2]string{
		"source name":   {spec.Source.Name, "clickhouse"},
		"region":        {spec.Target.Region, "us-east-1"},
		"job name":      {spec.Job.Name, "clickhouse-orders"},
		"namespace":     {spec.Job.TargetNamespace, "clickhouse"},
		"target table":  {spec.Job.TargetTable, "orders"},
		"iceberg table": {spec.Iceberg.Table, "clickhouse.orders"},
		"cursor":        {spec.Job.CursorColumn, "id"},
	}
	for name, c := range checks {
		if c[0] != c[1] {
			t.Errorf("%s = %q, want %q", name, c[0], c[1])
		}
	}
	if !spec.Target.ForcePathStyle || *spec.Job.Incremental || !*spec.Job.AutoTune || !spec.Iceberg.Enabled {
		t.Errorf("flags wrong: %+v", spec)
	}
}

func TestWizardRequiredFields(t *testing.T) {
	clearWizardEnv(t)
	w := newRunWizard(nil)
	answer(t, w, "clickhouse")
	// DSN has only a placeholder: Enter must not move on.
	w.handleKey(keyEnter)
	if w.field().Key != wkDSN || !strings.Contains(w.err, "required") {
		t.Fatalf("empty DSN accepted: field=%s err=%q", w.field().Key, w.err)
	}
	if !strings.Contains(w.view(), "required") {
		t.Fatal("the error should be shown")
	}
}

func TestWizardNoIcebergSkipsQuestions(t *testing.T) {
	clearWizardEnv(t)
	w := newRunWizard(nil)
	w.answers[wkIceberg] = optNo
	for _, f := range wizardFields(w.answers) {
		if f.Key == wkIceURI || f.Key == wkIceEngine || f.Key == wkIceTable {
			t.Fatalf("%s asked although Iceberg is off", f.Key)
		}
	}
}

func TestWizardFlightSQLAsksForQuery(t *testing.T) {
	a := map[string]string{wkEngine: "flightsql"}
	keys := map[string]bool{}
	for _, f := range wizardFields(a) {
		keys[f.Key] = true
	}
	if !keys[wkSQL] || keys[wkCursor] || keys[wkIncr] {
		t.Fatalf("flightsql questions = %v", keys)
	}
}

func TestWizardEnginesAreSubmittable(t *testing.T) {
	engines := wizardEngines()
	if len(engines) == 0 {
		t.Fatal("no engines")
	}
	for _, e := range engines {
		if e == "s3" {
			t.Fatal("s3 is not supported by run submit and must not be offered")
		}
	}
}

func TestWizardEscGoesBackAndKeepsAnswers(t *testing.T) {
	clearWizardEnv(t)
	w := newRunWizard(nil)
	answer(t, w, "clickhouse")
	answer(t, w, "clickhouse://u:p@h:9000/db")
	if action, _ := w.handleKey(keyEsc); action != wizContinue || w.field().Key != wkDSN {
		t.Fatalf("esc should go back to the DSN, at %s", w.field().Key)
	}
	if w.input.Value() != "clickhouse://u:p@h:9000/db" {
		t.Fatalf("going back lost the answer: %q", w.input.Value())
	}
	w.handleKey(keyEsc)
	if action, _ := w.handleKey(keyEsc); action != wizClose {
		t.Fatal("esc on the first question should close the wizard")
	}
}

func TestWizardEngineChangeClearsDependentAnswers(t *testing.T) {
	clearWizardEnv(t)
	w := newRunWizard(nil)
	answer(t, w, "clickhouse")
	answer(t, w, "clickhouse://u:p@h:9000/db")
	w.pos = 0
	w.enterField()
	answer(t, w, "postgres")
	if _, ok := w.answers[wkDSN]; ok {
		t.Fatal("a ClickHouse DSN must not survive switching to postgres")
	}
}

func TestWizardSelectArrowsWrap(t *testing.T) {
	w := newRunWizard(nil)
	n := len(w.field().Options)
	w.handleKey(tea.KeyMsg{Type: tea.KeyUp})
	if w.choice != n-1 {
		t.Fatalf("up from the top = %d, want %d", w.choice, n-1)
	}
	w.handleKey(keyDown)
	if w.choice != 0 {
		t.Fatalf("down wraps to %d", w.choice)
	}
}

func TestWizardHidesSecrets(t *testing.T) {
	clearWizardEnv(t)
	w := newRunWizard(nil)
	fillClickHouse(t, w)
	v := w.view()
	for _, secret := range []string{"minio-secret", ":clickhouse@"} {
		if strings.Contains(v, secret) {
			t.Fatalf("review shows %q:\n%s", secret, v)
		}
	}
	if !strings.Contains(v, "default:••••@ex-clickhouse") {
		t.Fatalf("DSN should be shown redacted:\n%s", v)
	}
}

func TestWizardPrefillsFromEnvironment(t *testing.T) {
	clearWizardEnv(t)
	t.Setenv("ORABBIT_DEFAULT_S3_ENDPOINT", "http://env-minio:9000")
	f := wizardFields(map[string]string{})
	for _, x := range f {
		if x.Key == wkEndpoint && x.Default(nil) != "http://env-minio:9000" {
			t.Fatal("endpoint should default to ORABBIT_DEFAULT_S3_ENDPOINT")
		}
	}
}

func TestSaveWizardSpecRoundTrips(t *testing.T) {
	clearWizardEnv(t)
	t.Chdir(t.TempDir())
	w := newRunWizard(nil)
	fillClickHouse(t, w)
	spec, err := buildWizardSpec(w.answers, "http://127.0.0.1:9100")
	if err != nil {
		t.Fatal(err)
	}
	path, err := saveWizardSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path) != "clickhouse_orders.run.yaml" {
		t.Fatalf("path = %s", path)
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, want 0600 (the file holds secrets)", st.Mode().Perm())
	}
	// The saved file must load with the strict `run submit` loader.
	loaded, err := loadRunSubmitFile(path)
	if err != nil {
		t.Fatalf("saved spec does not load: %v", err)
	}
	if loaded.Job.Name != spec.Job.Name || loaded.Target.SecretAccessKey != "minio-secret" {
		t.Fatalf("round trip lost data: %+v", loaded.Job)
	}
	if _, err := saveWizardSpec(spec); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second save should refuse to overwrite, got %v", err)
	}
}

func TestShellWizardSubmitsAndWatches(t *testing.T) {
	clearWizardEnv(t)
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.Method+" "+r.URL.Path)
		mu.Unlock()
		switch {
		case r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode([]any{}) // lookups by name: nothing exists yet
		case r.URL.Path == "/api/v1/connections":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "conn-1"})
		case r.URL.Path == "/api/v1/jobs":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "job-1"})
		case r.URL.Path == "/api/v1/jobs/job-1/runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"run": map[string]any{"id": "run-9"}, "tasks": []any{map[string]any{}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	m := newTestTUIModel(srv.URL)
	m, cmd := m.dispatchModel(t, "/run interactive")
	if m.wizard == nil || cmd == nil {
		t.Fatal("/run interactive should open the wizard")
	}
	fillClickHouse(t, m.wizard)
	m.wizard.choice = 0 // optSubmit
	m, cmd = update(t, m, keyEnter)
	if m.wizard != nil || m.busy == "" || cmd == nil {
		t.Fatalf("submit should close the wizard and go busy (err=%v)", m.wizard)
	}

	// Run the submit command directly (tea.Sequence wraps it with a print).
	var spec runSubmitFile
	spec, err := buildWizardSpec(m.lastWizard, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	msg := m.submitWizardCmd(spec)().(tuiWizardSubmittedMsg)
	if msg.err != nil || msg.runID != "run-9" {
		t.Fatalf("submit result = %+v (calls: %v)", msg, paths)
	}
	m, _ = update(t, m, msg)
	if m.watch == nil || m.watch.runID != "run-9" {
		t.Fatal("a submitted run should be watched automatically")
	}
}

func TestShellRunClassicAndFlagsUseCLI(t *testing.T) {
	m := newTestTUIModel("http://127.0.0.1:1")
	for _, line := range []string{"/run classic", "/run --advanced", "/run interactive --advanced"} {
		next, _ := m.dispatchModel(t, line)
		if next.wizard != nil || !next.execing {
			t.Errorf("%q should run the CLI prompts, not the wizard", line)
		}
	}
	if next, _ := m.dispatchModel(t, "/run"); next.wizard == nil {
		t.Error("/run should open the wizard")
	}
}

func TestShellWizardCloseKeepsAnswers(t *testing.T) {
	clearWizardEnv(t)
	m := newTestTUIModel("http://127.0.0.1:1")
	m, _ = m.dispatchModel(t, "/run")
	answer(t, m.wizard, "clickhouse")
	m, _ = update(t, m, tea.KeyMsg{Type: tea.KeyCtrlC})
	if m.wizard != nil {
		t.Fatal("Ctrl+C should close the wizard")
	}
	m, _ = m.dispatchModel(t, "/run")
	if m.wizard.answers[wkEngine] != "clickhouse" {
		t.Fatal("reopening should keep the earlier answers")
	}
}

func TestRedactDSN(t *testing.T) {
	for in, want := range map[string]string{
		"clickhouse://default:clickhouse@h:9000/db": "clickhouse://default:••••@h:9000/db",
		"postgresql://u@h/db":                       "postgresql://u@h/db",
		"grpc+tcp://localhost:32010":                "grpc+tcp://localhost:32010",
	} {
		if got := redactDSN(in); got != want {
			t.Errorf("redactDSN(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLastTablePart(t *testing.T) {
	for in, want := range map[string]string{"default.orders": "orders", `"Sales"."Order Items"`: "order_items", "[dbo].[T1]": "t1", "": ""} {
		if got := lastTablePart(in); got != want {
			t.Errorf("lastTablePart(%q) = %q, want %q", in, got, want)
		}
	}
}
