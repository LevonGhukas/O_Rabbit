package orabbitcli

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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
		{wkAdvanced, optNo},
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

func TestShellEveryRunFormOpensWizard(t *testing.T) {
	m := newTestTUIModel("http://127.0.0.1:1")
	for _, line := range []string{"/run classic", "/run interactive", "/run --advanced", "/run classic --count 2"} {
		next, _ := m.dispatchModel(t, line)
		if next.wizard == nil || next.execing {
			t.Errorf("%q should open the wizard, not the old CLI prompts", line)
		}
	}
	if got := wizardIgnoredArgs([]string{"classic", "--count", "2"}); strings.Join(got, " ") != "--count 2" {
		t.Errorf("ignored args = %v", got)
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

func TestWizardAdvancedManualTuning(t *testing.T) {
	clearWizardEnv(t)
	w := newRunWizard(nil)
	fillClickHouse(t, w)
	// Go back from the review to the advanced question and turn it on.
	w.handleKey(keyEsc)
	answer(t, w, optYes)
	answer(t, w, "exports/ch") // S3 prefix
	answer(t, w, optNo)        // auto-tune off
	if w.field().Key != wkMaxFlight {
		t.Fatalf("manual tuning should ask for max in flight, got %s", w.field().Key)
	}
	w.input.SetValue("0")
	w.handleKey(keyEnter)
	if w.field().Key != wkMaxFlight || !strings.Contains(w.err, "at least 1") {
		t.Fatalf("0 accepted: field=%s err=%q", w.field().Key, w.err)
	}
	answer(t, w, "4")
	answer(t, w, "8")     // planned tasks
	answer(t, w, "")      // rows per task: optional
	answer(t, w, "20000") // fetch size

	spec, err := buildWizardSpec(w.answers, "http://127.0.0.1:9100")
	if err != nil {
		t.Fatalf("spec invalid: %v", err)
	}
	if *spec.Job.AutoTune || spec.Job.MaxInFlightTasks != 4 || spec.Job.PlannedTasks != 8 || spec.Job.FetchLimit != 20000 || spec.Target.Prefix != "exports/ch" {
		t.Fatalf("advanced settings not applied: %+v %+v", spec.Job, spec.Target)
	}
}

func TestWizardAdvancedSkippedForFlightSQL(t *testing.T) {
	a := map[string]string{wkEngine: "flightsql", wkAdvanced: optYes, wkAutoTune: optNo}
	for _, f := range wizardFields(a) {
		if f.Key == wkAutoTune || f.Key == wkMaxFlight || f.Key == wkFetch {
			t.Fatalf("FlightSQL must not be asked %s", f.Key)
		}
	}
}

func TestWizardTabUsesExample(t *testing.T) {
	clearWizardEnv(t)
	w := newRunWizard(nil)
	answer(t, w, "mongodb")
	answer(t, w, "mongodb://u:p@h:27017")
	answer(t, w, "") // connection name default
	if w.field().Key != wkTable {
		t.Fatalf("at %s", w.field().Key)
	}
	w.handleKey(tea.KeyMsg{Type: tea.KeyTab})
	// The explanation in the placeholder is not part of the value.
	if w.input.Value() != "shop.orders" {
		t.Fatalf("tab filled %q", w.input.Value())
	}
	w.handleKey(keyEnter)
	w.handleKey(keyEnter) // empty cursor column: required
	if !strings.Contains(w.err, "press Tab") {
		t.Fatalf("required error should mention Tab, got %q", w.err)
	}
}

func TestParseMasterErrorAndHints(t *testing.T) {
	raw := `submit: http 503: {
  "error": {"code": "run_planning_failed", "message": "run planning failed",
    "details": {"error": "open source reader (engine=clickhouse): dial tcp: lookup ex-clickhouse on 127.0.0.11:53: server misbehaving"},
    "failure_class": "NETWORK_CONNECTION_FAILED", "retryable": true}
}`
	e, ok := parseMasterError(errors.New(raw))
	if !ok || e.Status != "503" || e.Message != "run planning failed" || !e.Retryable {
		t.Fatalf("parsed = %+v, %v", e, ok)
	}
	hints := strings.Join(errorHints(e), "\n")
	for _, want := range []string{`"ex-clickhouse"`, "docker network connect orabbit-control-plane ex-clickhouse", "both must reach", "retryable"} {
		if !strings.Contains(hints, want) {
			t.Errorf("hints miss %q:\n%s", want, hints)
		}
	}
	// Collapse the box's wrapping and borders before searching.
	block := strings.Join(strings.FieldsFunc(renderErrorBlock(80, "Submit failed", errors.New(raw)), func(r rune) bool {
		return r == ' ' || r == '\n' || r == '│'
	}), " ")
	for _, want := range []string{"Submit failed", "run planning failed", "NETWORK_CONNECTION_FAILED", "clickhouse on 127.0.0.11:53"} {
		if !strings.Contains(block, want) {
			t.Errorf("block misses %q:\n%q", want, block)
		}
	}
	if strings.Contains(block, `"error":`) {
		t.Error("raw JSON should not be shown")
	}
	if _, ok := parseMasterError(errors.New("dial tcp: connection refused")); ok {
		t.Error("a plain error is not a master error")
	}
}

func TestErrorBlockRedactsDSN(t *testing.T) {
	raw := `http 400: {"error":{"message":"bad dsn","details":{"dsn":"clickhouse://default:hunter2@h:9000/db"}}}`
	if block := renderErrorBlock(80, "x", errors.New(raw)); strings.Contains(block, "hunter2") {
		t.Fatalf("password leaked:\n%s", block)
	}
}

func TestRenderBoxFitsWidth(t *testing.T) {
	for _, width := range []int{0, 40, 80, 200} {
		out := renderBox(width, tuiColorBorder, "Title", "body line")
		for _, line := range strings.Split(out, "\n") {
			if w := lipgloss.Width(line); w > boxWidth(width) {
				t.Fatalf("width %d: line is %d wide, max %d", width, w, boxWidth(width))
			}
		}
	}
}

func TestShellPrintsSummaryBeforeSubmit(t *testing.T) {
	clearWizardEnv(t)
	m := newTestTUIModel("http://127.0.0.1:1")
	m, _ = m.dispatchModel(t, "/run")
	fillClickHouse(t, m.wizard)
	m.wizard.choice = 0 // optSubmit
	_, cmd := update(t, m, keyEnter)
	// tea.Sequence returns a message holding its commands in order.
	first := firstSequenced(t, cmd)
	if !strings.Contains(first, "Run configuration") || !strings.Contains(first, "default.orders") || strings.Contains(first, "minio-secret") {
		t.Fatalf("summary printed first should list the answers without secrets:\n%s", first)
	}
}

// firstSequenced runs the first command of a tea.Sequence and returns what it prints.
func firstSequenced(t *testing.T, cmd tea.Cmd) string {
	t.Helper()
	msg := cmd()
	v := reflect.ValueOf(msg)
	if v.Kind() != reflect.Slice || v.Len() == 0 {
		t.Fatalf("not a sequence: %T", msg)
	}
	first, ok := v.Index(0).Interface().(tea.Cmd)
	if !ok {
		t.Fatalf("sequence element is %T", v.Index(0).Interface())
	}
	return fmtMsg(first())
}
