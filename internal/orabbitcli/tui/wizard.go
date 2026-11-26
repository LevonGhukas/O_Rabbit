package tui

import (
	"github.com/LevonGhukas/O_Rabbit/internal/orabbitcli"

	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"gopkg.in/yaml.v3"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

// The run wizard is the shell's guided path for starting an export. It asks
// one question at a time, shows real choices instead of free text where it
// can, and builds a runSubmitFile, so the result is checked by the same
// Validate() and sent through the same orabbitcli.SubmitRun as `run submit`.

type wizFieldKind int

const (
	wizText wizFieldKind = iota
	wizSecret
	wizSelect
)

// wizard answer keys
const (
	wkEngine     = "engine"
	wkSourceName = "source_name"
	wkDSN        = "dsn"
	wkTable      = "table"
	wkCursor     = "cursor"
	wkIncr       = "incremental"
	wkSQL        = "sql"
	wkEndpoint   = "endpoint"
	wkBucket     = "bucket"
	wkAccessKey  = "access_key"
	wkSecretKey  = "secret_key"
	wkRegion     = "region"
	wkPathStyle  = "path_style"
	wkJobName    = "job_name"
	wkNamespace  = "namespace"
	wkTargetTbl  = "target_table"
	wkIceberg    = "iceberg"
	wkIceEngine  = "iceberg_engine"
	wkIceURI     = "iceberg_uri"
	wkIceTable   = "iceberg_table"
	wkAdvanced   = "advanced"
	wkAutoTune   = "auto_tune"
	wkMaxFlight  = "max_in_flight"
	wkPlanned    = "planned_tasks"
	wkRowsTask   = "rows_per_task"
	wkFetch      = "fetch_size"
	wkPrefix     = "s3_prefix"
	wkReview     = "review"
)

const (
	optYes          = "Yes"
	optNo           = "No"
	optFullExport   = "No — export the whole table"
	optIncremental  = "Yes — only rows newer than the last run"
	optSubmit       = "Submit and watch the run"
	optSaveYAML     = "Save as a YAML spec (for /submit later)"
	optEdit         = "Change answers (starts from the first question)"
	optCancelWizard = "Cancel"
)

var wizSteps = []string{"Source", "Data", "Storage", "Destination", "Advanced", "Review"}

const (
	stepAdvanced = 4
	stepReview   = 5
)

type wizField struct {
	Key         string
	Step        int // index into wizSteps
	Label       string
	Help        string
	Kind        wizFieldKind
	Options     []string
	Placeholder string
	Optional    bool
	// Default proposes a value from earlier answers (or the environment).
	Default func(a map[string]string) string
	// Validate checks a non-empty text answer; nil accepts anything.
	Validate func(string) error
}

// tabValue is what Tab fills into an empty text field: the example without
// explanations such as "shop.orders (database.collection)".
func (f wizField) tabValue() string {
	v, _, _ := strings.Cut(f.Placeholder, " (")
	return strings.TrimSpace(v)
}

// positiveInt validates whole numbers >= min, for the tuning fields.
func positiveInt(min int64) func(string) error {
	return func(v string) error {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < min {
			return fmt.Errorf("enter a whole number of at least %d", min)
		}
		return nil
	}
}

// wizardEngines lists the engines `run submit` can handle: engines with an
// ordered cursor, plus FlightSQL. Others are rejected by Validate().
func wizardEngines() []string {
	var out []string
	for _, e := range connectors.KnownSourceEngines() {
		if e == "flightsql" || connectors.SupportsOrderedCursor(e) {
			out = append(out, e)
		}
	}
	return out
}

func wizardTablePlaceholder(engine string) string {
	switch engine {
	case "clickhouse":
		return "default.orders"
	case "postgres":
		return "public.orders"
	case "mssql":
		return "SalesDB.dbo.Orders"
	case "mysql", "mariadb":
		return "shop.orders"
	case "oracle":
		return "APP.ORDERS"
	case "trino":
		return "catalog.schema.orders"
	case "mongodb":
		return "shop.orders (database.collection)"
	case "cassandra":
		return "shop.orders (keyspace.table)"
	}
	return "schema.table"
}

var nonIdentChars = regexp.MustCompile(`[^a-z0-9_]+`)

// lastTablePart turns "default.orders" or `"Sales"."Orders"` into "orders".
func lastTablePart(table string) string {
	table = strings.TrimSpace(table)
	if i := strings.LastIndex(table, "."); i >= 0 {
		table = table[i+1:]
	}
	table = strings.ToLower(strings.Trim(table, "\"`[] "))
	return strings.Trim(nonIdentChars.ReplaceAllString(table, "_"), "_")
}

func envDefault(key string) func(map[string]string) string {
	return func(map[string]string) string { return strings.TrimSpace(os.Getenv(key)) }
}

func isFlightSQL(a map[string]string) bool { return a[wkEngine] == "flightsql" }

// datasetName is the export's short name: the table, or for FlightSQL the
// dataset label typed in place of a table.
func datasetName(a map[string]string) string {
	if n := lastTablePart(a[wkTable]); n != "" {
		return n
	}
	return "export"
}

// wizardFields returns the questions to ask for the current answers; it is
// recomputed after every answer, so choices such as FlightSQL or "no
// Iceberg" add or remove the questions that depend on them.
func wizardFields(a map[string]string) []wizField {
	f := []wizField{
		{Key: wkEngine, Step: 0, Label: "Database engine", Help: "The database to export from.", Kind: wizSelect, Options: wizardEngines()},
		{Key: wkDSN, Step: 0, Label: "Connection URL (DSN)",
			Help:        "Used by the workers, not this shell: localhost means the worker's own container.",
			Placeholder: orabbitcli.DefaultSourceDSN(a[wkEngine])},
		{Key: wkSourceName, Step: 0, Label: "Connection name",
			Help:    "Saved on the master under this name; reusing a name updates that connection.",
			Default: func(a map[string]string) string { return a[wkEngine] }},
	}

	if isFlightSQL(a) {
		f = append(f,
			wizField{Key: wkSQL, Step: 1, Label: "SQL query", Help: "Query text sent to the FlightSQL server.", Placeholder: "SELECT * FROM orders"},
			wizField{Key: wkTable, Step: 1, Label: "Dataset label", Help: "Short name used for the S3 prefix and table names.", Placeholder: "orders"},
		)
	} else {
		f = append(f,
			wizField{Key: wkTable, Step: 1, Label: "Source table", Placeholder: wizardTablePlaceholder(a[wkEngine])},
			wizField{Key: wkCursor, Step: 1, Label: "Cursor column",
				Help:        "A column that only grows (id, created_at). Used to split the work and for incremental runs.",
				Placeholder: map[bool]string{true: "_id", false: "id"}[a[wkEngine] == "mongodb"]},
			wizField{Key: wkIncr, Step: 1, Label: "Incremental?", Kind: wizSelect, Options: []string{optFullExport, optIncremental}},
		)
	}

	f = append(f,
		wizField{Key: wkEndpoint, Step: 2, Label: "S3 / MinIO endpoint",
			Help: "As the workers reach it, e.g. http://minio:9000 on the same Docker network.", Placeholder: "http://minio:9000",
			Default: envDefault("ORABBIT_DEFAULT_S3_ENDPOINT")},
		wizField{Key: wkBucket, Step: 2, Label: "Bucket", Help: "Must already exist.", Placeholder: "orabbit"},
		wizField{Key: wkAccessKey, Step: 2, Label: "Access key ID", Default: envDefault("ORABBIT_DEFAULT_S3_ACCESS_KEY_ID")},
		wizField{Key: wkSecretKey, Step: 2, Label: "Secret access key", Kind: wizSecret, Default: envDefault("ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY")},
		wizField{Key: wkRegion, Step: 2, Label: "Region", Default: func(map[string]string) string {
			if r := strings.TrimSpace(os.Getenv("ORABBIT_DEFAULT_S3_REGION")); r != "" {
				return r
			}
			return "us-east-1"
		}},
		wizField{Key: wkPathStyle, Step: 2, Label: "Path-style URLs?", Help: "Yes for MinIO and most self-hosted S3.", Kind: wizSelect, Options: []string{optYes, optNo}},

		wizField{Key: wkJobName, Step: 3, Label: "Job name", Help: "Saved on the master; reusing a name updates that job.",
			Default: func(a map[string]string) string { return a[wkEngine] + "-" + datasetName(a) }},
		wizField{Key: wkNamespace, Step: 3, Label: "Target namespace", Default: func(a map[string]string) string { return a[wkEngine] }},
		wizField{Key: wkTargetTbl, Step: 3, Label: "Target table", Default: datasetName},
		wizField{Key: wkIceberg, Step: 3, Label: "Register in Iceberg?", Help: "Makes the files queryable from ClickHouse, Trino or Spark.", Kind: wizSelect, Options: []string{optYes, optNo}},
	)
	if a[wkIceberg] != optNo {
		f = append(f,
			wizField{Key: wkIceEngine, Step: 3, Label: "Iceberg engine", Kind: wizSelect, Options: []string{"rest-go", "ice"}},
			wizField{Key: wkIceURI, Step: 3, Label: "Iceberg REST catalog URI", Help: "As the master reaches it (registration runs on the master).", Placeholder: "http://iceberg-rest:8181"},
			wizField{Key: wkIceTable, Step: 3, Label: "Iceberg table",
				Default: func(a map[string]string) string { return a[wkNamespace] + "." + a[wkTargetTbl] }},
		)
	}
	f = append(f, wizField{Key: wkAdvanced, Step: stepAdvanced, Label: "Change advanced settings?",
		Help: "Performance tuning and the S3 prefix. The defaults suit most exports.", Kind: wizSelect,
		Options: []string{optNo, optYes}})
	if a[wkAdvanced] == optYes {
		f = append(f, wizField{Key: wkPrefix, Step: stepAdvanced, Label: "S3 prefix override", Optional: true,
			Help: "Leave empty to derive it from the engine and table."})
		// FlightSQL submissions take no planning settings at all.
		if !isFlightSQL(a) {
			f = append(f, wizField{Key: wkAutoTune, Step: stepAdvanced, Label: "Automatic performance tuning?",
				Help: "Lets the planner choose chunking and concurrency.", Kind: wizSelect, Options: []string{optYes, optNo}})
			if a[wkAutoTune] == optNo {
				f = append(f,
					wizField{Key: wkMaxFlight, Step: stepAdvanced, Label: "Max tasks in flight", Help: "How many tasks run at the same time.",
						Placeholder: "4", Validate: positiveInt(1)},
					wizField{Key: wkPlanned, Step: stepAdvanced, Label: "Planned tasks", Optional: true, Validate: positiveInt(1),
						Help: "Split the table into this many tasks. Set this or rows per task."},
					wizField{Key: wkRowsTask, Step: stepAdvanced, Label: "Target rows per task", Optional: true, Validate: positiveInt(1),
						Placeholder: "200000"},
				)
			}
			f = append(f, wizField{Key: wkFetch, Step: stepAdvanced, Label: "Fetch size", Optional: true, Validate: positiveInt(1),
				Help: "Rows per source round-trip (Oracle, Cassandra, ClickHouse, MongoDB). Empty keeps the engine default."})
		}
	}
	f = append(f, wizField{Key: wkReview, Step: stepReview, Label: "Ready?", Kind: wizSelect, Options: []string{optSubmit, optSaveYAML, optEdit, optCancelWizard}})
	return f
}

// buildWizardSpec turns the answers into the same structure `run submit`
// reads from a file, then validates it with the same rules.
func buildWizardSpec(a map[string]string, masterHTTP string) (orabbitcli.RunSubmitFile, error) {
	incremental := a[wkIncr] == optIncremental
	// FlightSQL submissions require auto_tune=false; otherwise it is on unless
	// the advanced step turned it off.
	autoTune := !isFlightSQL(a) && !(a[wkAdvanced] == optYes && a[wkAutoTune] == optNo)
	spec := orabbitcli.RunSubmitFile{
		Master: orabbitcli.RunSubmitMasterSpec{HTTP: masterHTTP},
		Source: orabbitcli.RunSubmitSourceSpec{Name: a[wkSourceName], Engine: a[wkEngine], DSN: a[wkDSN], SQL: a[wkSQL]},
		Target: orabbitcli.RunSubmitTargetSpec{
			Name:            "s3",
			Endpoint:        a[wkEndpoint],
			Region:          a[wkRegion],
			Bucket:          a[wkBucket],
			ForcePathStyle:  a[wkPathStyle] != optNo,
			AccessKeyID:     a[wkAccessKey],
			SecretAccessKey: a[wkSecretKey],
		},
		Job: orabbitcli.RunSubmitJobSpec{
			Name:            a[wkJobName],
			TargetNamespace: a[wkNamespace],
			TargetTable:     a[wkTargetTbl],
			Incremental:     &incremental,
			Table:           a[wkTable],
			CursorColumn:    a[wkCursor],
			AutoTune:        &autoTune,
		},
	}
	if a[wkAdvanced] == optYes {
		spec.Target.Prefix = a[wkPrefix]
		spec.Job.FetchLimit = int(atoiOrZero(a[wkFetch]))
		if !autoTune && !isFlightSQL(a) {
			spec.Job.MaxInFlightTasks = int(atoiOrZero(a[wkMaxFlight]))
			spec.Job.PlannedTasks = int(atoiOrZero(a[wkPlanned]))
			spec.Job.TargetRowsPerTask = atoiOrZero(a[wkRowsTask])
		}
	}
	if a[wkIceberg] != optNo {
		uri := a[wkIceURI]
		spec.Iceberg = orabbitcli.RunSubmitIcebergSpec{Enabled: true, Engine: a[wkIceEngine], Table: a[wkIceTable]}
		spec.Iceberg.Options.URI = &uri
	}
	if err := spec.Validate(); err != nil {
		return orabbitcli.RunSubmitFile{}, err
	}
	return spec, nil
}

// atoiOrZero reads an answer already checked by positiveInt; empty is 0.
func atoiOrZero(v string) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	return n
}

// runWizard is the wizard's state. model owns at most one and forwards
// keys to it while it is open.
type runWizard struct {
	answers map[string]string
	pos     int // index into wizardFields(answers)
	input   textinput.Model
	choice  int // highlighted option of a select field
	err     string
	notice  string // e.g. where a YAML spec was saved
}

func newRunWizard(previous map[string]string) *runWizard {
	w := &runWizard{answers: map[string]string{}}
	for k, v := range previous { // reuse the last wizard's answers this session
		if k != wkReview {
			w.answers[k] = v
		}
	}
	w.input = textinput.New()
	w.input.Prompt = stylePrompt.Render("› ")
	w.input.Focus()
	w.enterField()
	return w
}

func (w *runWizard) field() wizField { return wizardFields(w.answers)[w.pos] }

// enterField loads the current answer (or its default) into the editor.
func (w *runWizard) enterField() {
	f := w.field()
	w.err = ""
	cur, answered := w.answers[f.Key]
	if !answered && f.Default != nil {
		cur = f.Default(w.answers)
	}
	switch f.Kind {
	case wizSelect:
		w.choice = 0
		for i, o := range f.Options {
			if o == cur {
				w.choice = i
			}
		}
	default:
		w.input.SetValue(cur)
		w.input.Placeholder = f.Placeholder
		// Typing the start of the example shows the rest as dim text that
		// Tab accepts, the same way as in the shell prompt.
		w.input.ShowSuggestions = true
		if tv := f.tabValue(); tv != "" {
			w.input.SetSuggestions([]string{tv})
		} else {
			w.input.SetSuggestions(nil)
		}
		w.input.EchoMode = textinput.EchoNormal
		if f.Kind == wizSecret {
			w.input.EchoMode = textinput.EchoPassword
			w.input.EchoCharacter = '•'
		}
		w.input.CursorEnd()
	}
}

// wizAction tells the shell what to do after a key press.
type wizAction int

const (
	wizContinue wizAction = iota
	wizClose
	wizSubmit
	wizSave
)

func (w *runWizard) handleKey(msg tea.KeyMsg) (wizAction, tea.Cmd) {
	f := w.field()
	switch msg.Type {
	case tea.KeyCtrlC:
		return wizClose, nil
	case tea.KeyEsc:
		if w.pos == 0 {
			return wizClose, nil
		}
		w.pos--
		w.enterField()
		return wizContinue, nil
	case tea.KeyEnter:
		return w.commit(f)
	}

	// Tab on an empty text field takes the example; with text typed, the
	// input itself completes the suggestion.
	if msg.Type == tea.KeyTab && f.Kind != wizSelect && w.input.Value() == "" && f.tabValue() != "" {
		w.input.SetValue(f.tabValue())
		w.input.CursorEnd()
		w.err = ""
		return wizContinue, nil
	}

	if f.Kind == wizSelect {
		switch msg.Type {
		case tea.KeyUp:
			w.choice = (w.choice - 1 + len(f.Options)) % len(f.Options)
		case tea.KeyDown, tea.KeyTab:
			w.choice = (w.choice + 1) % len(f.Options)
		}
		return wizContinue, nil
	}
	var cmd tea.Cmd
	w.input, cmd = w.input.Update(msg)
	w.err = ""
	return wizContinue, cmd
}

// commit stores the answer for f and moves on, or reports what is wrong.
func (w *runWizard) commit(f wizField) (wizAction, tea.Cmd) {
	var v string
	if f.Kind == wizSelect {
		v = f.Options[w.choice]
	} else {
		v = strings.TrimSpace(w.input.Value())
		if v == "" && !f.Optional {
			w.err = f.Label + " is required"
			if f.tabValue() != "" {
				w.err += " — press Tab to use the example"
			}
			return wizContinue, nil
		}
		if v != "" && f.Validate != nil {
			if err := f.Validate(v); err != nil {
				w.err = err.Error()
				return wizContinue, nil
			}
		}
	}

	if f.Key == wkReview {
		switch v {
		case optSubmit:
			return wizSubmit, nil
		case optSaveYAML:
			return wizSave, nil
		case optEdit:
			w.pos = 0
			w.notice = ""
			w.enterField()
			return wizContinue, nil
		default:
			return wizClose, nil
		}
	}

	// Changing the engine invalidates the engine-specific answers.
	if f.Key == wkEngine && w.answers[wkEngine] != v {
		for _, k := range []string{wkDSN, wkSourceName, wkTable, wkCursor, wkSQL, wkJobName, wkNamespace, wkTargetTbl, wkIceTable} {
			delete(w.answers, k)
		}
	}
	// A new table changes the derived names, unless the user already typed one.
	if f.Key == wkTable && w.answers[wkTable] != v {
		for _, k := range []string{wkJobName, wkTargetTbl, wkIceTable} {
			delete(w.answers, k)
		}
	}
	w.answers[f.Key] = v

	if w.pos+1 < len(wizardFields(w.answers)) {
		w.pos++
	}
	w.enterField()
	return wizContinue, nil
}

// view renders the current step: a header with progress, the answers
// already given in this step, and the current question.
func (w *runWizard) view() string {
	fields := wizardFields(w.answers)
	f := fields[w.pos]
	var b strings.Builder

	b.WriteString(styleTitle.Render(fmt.Sprintf("Step %d/%d · %s", f.Step+1, len(wizSteps), wizSteps[f.Step])))
	b.WriteString(styleDim.Render("   " + wizardStepTrail(f.Step)))
	b.WriteString("\n\n")

	if f.Key == wkReview {
		b.WriteString(w.reviewSummary(fields))
		b.WriteString("\n")
	} else {
		for _, prev := range fields[:w.pos] {
			if prev.Step == f.Step {
				b.WriteString(styleOK.Render("✓ ") + styleDim.Render(prev.Label+": ") + displayAnswer(prev, w.answers[prev.Key]) + "\n")
			}
		}
	}

	b.WriteString(lipglossBold(f.Label) + "\n")
	if f.Help != "" {
		b.WriteString(styleDim.Render("  "+f.Help) + "\n")
	}
	if f.Kind == wizSelect {
		for i, o := range f.Options {
			if i == w.choice {
				b.WriteString(stylePrompt.Render("› ") + styleSelected.Render(o) + "\n")
			} else {
				b.WriteString("  " + o + "\n")
			}
		}
	} else {
		b.WriteString(w.input.View() + "\n")
	}
	if w.err != "" {
		b.WriteString(styleErr.Render("  ✗ "+w.err) + "\n")
	}
	if w.notice != "" {
		b.WriteString(styleOK.Render("  "+w.notice) + "\n")
	}
	keys := "Enter next · Esc back · Ctrl+C cancel"
	switch {
	case f.Kind == wizSelect:
		keys = "↑↓ choose · " + keys
	case f.tabValue() != "":
		keys = "Tab use example · " + keys
	case f.Optional:
		keys = "Enter skips when empty · " + keys
	}
	b.WriteString(styleDim.Render(keys) + "\n")
	return b.String()
}

// wizardStepTrail is a compact progress line such as ✓ ✓ ● ○ ○ ○, short
// enough not to wrap on narrow terminals.
func wizardStepTrail(current int) string {
	parts := make([]string, len(wizSteps))
	for i := range wizSteps {
		switch {
		case i < current:
			parts[i] = "✓"
		case i == current:
			parts[i] = "●"
		default:
			parts[i] = "○"
		}
	}
	return strings.Join(parts, " ")
}

func (w *runWizard) reviewSummary(fields []wizField) string {
	var b strings.Builder
	step := -1
	for _, f := range fields {
		if f.Key == wkReview {
			continue
		}
		if f.Step != step {
			step = f.Step
			b.WriteString(styleCmd.Render(wizSteps[step]) + "\n")
		}
		fmt.Fprintf(&b, "  %-26s %s\n", f.Label, displayAnswer(f, w.answers[f.Key]))
	}
	return b.String()
}

// displayAnswer masks secrets, including passwords inside DSNs, so they
// never appear on screen or in the scrollback.
func displayAnswer(f wizField, v string) string {
	switch {
	case f.Kind == wizSecret && v != "":
		return "••••••••"
	case f.Key == wkDSN:
		return redactDSN(v)
	case v == "":
		return styleDim.Render("(empty)")
	}
	return v
}

var (
	// URL form: scheme://user:password@host.
	dsnPassword = regexp.MustCompile(`(://[^:/@\s]+:)[^@\s]*@`)
	// Key/value form, e.g. FlightSQL "username=u;password=p;authorization_header=Bearer t"
	// or a query string "?password=p&...". The value runs to the next ; & or quote.
	dsnSecretParam = regexp.MustCompile(`(?i)\b((?:password|pwd|passwd|token|secret|authorization_header)=)(?:Bearer\s+)?[^;&"'\s]*`)
)

// redactDSN masks passwords and tokens in a DSN, or in error text that
// quotes one, before it is shown on screen.
func redactDSN(dsn string) string {
	dsn = dsnPassword.ReplaceAllString(dsn, "${1}••••@")
	return dsnSecretParam.ReplaceAllString(dsn, "${1}••••")
}

func lipglossBold(s string) string { return styleTitle.UnsetForeground().Render(s) }

// --- commands the wizard triggers ------------------------------------------

type wizardSubmittedMsg struct {
	runID string
	tasks int
	err   error
}

func (m model) submitWizardCmd(spec orabbitcli.RunSubmitFile) tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(m.ctx, 2*requestTimeout)
		defer cancel()
		runID, tasks, err := orabbitcli.SubmitRun(ctx, base, spec)
		return wizardSubmittedMsg{runID: runID, tasks: tasks, err: err}
	}
}

// saveWizardSpec writes the spec as YAML next to the shell's working
// directory. The file holds the source DSN and S3 secret, so it is created
// with owner-only permissions and never overwrites an existing file.
func saveWizardSpec(spec orabbitcli.RunSubmitFile) (string, error) {
	spec.Master = orabbitcli.RunSubmitMasterSpec{} // the shell passes --master-http on /submit
	raw, err := yaml.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("encode spec: %w", err)
	}
	name := lastTablePart(spec.Job.Name)
	if name == "" {
		name = "run"
	}
	path, err := filepath.Abs(name + ".run.yaml")
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("%s already exists; rename or remove it first", path)
		}
		return "", err
	}
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		return "", err
	}
	return path, f.Close()
}
