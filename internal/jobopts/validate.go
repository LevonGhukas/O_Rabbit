package jobopts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// FieldError is one problem with a job's options_json.
type FieldError struct {
	Field string `json:"field"` // e.g. "options_json.planned_tasks"
	Error string `json:"error"`
}

// ValidationError lists every problem found in a job's configuration
// (options_json and related job fields), so a caller can fix them all at once.
type ValidationError struct {
	Errors []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Errors))
	for i, fe := range e.Errors {
		parts[i] = fe.Field + ": " + fe.Error
	}
	return "invalid job configuration: " + strings.Join(parts, "; ")
}

// ValidateMode selects how strictly options_json is checked.
type ValidateMode int

const (
	// Strict is used when a job is created or updated: unknown keys are
	// rejected, so a typo never silently disables a setting.
	Strict ValidateMode = iota
	// Lenient is used when a stored job is run: jobs saved before strict
	// validation may carry unknown keys or values that planning tolerates,
	// so only problems that would make planning fail anyway are reported.
	Lenient
)

// iceberg_registration is owned by icebergreg (JobConfig); jobopts cannot
// import it, so its shape is checked here.
const icebergRegistrationKey = "iceberg_registration"

type icebergRegistrationShape struct {
	Enabled bool   `json:"enabled"`
	Engine  string `json:"engine"`
	Table   string `json:"table"`
}

// deprecatedKeys were written by older O_Rabbit versions but are never read.
// They stay accepted so stored jobs can be fetched and saved back unchanged.
var deprecatedKeys = map[string]string{
	"iceberg_enabled":        "use iceberg_registration.enabled",
	"iceberg_engine":         "use iceberg_registration.engine",
	"iceberg_table":          "use iceberg_registration.table",
	"iceberg_partition_keys": "use partition_keys",
	"fetch_limit_rows":       "it has no effect",
}

// optionFieldTypes maps every options_json key of Options to its Go type.
var optionFieldTypes = func() map[string]reflect.Type {
	out := map[string]reflect.Type{}
	t := reflect.TypeOf(Options{})
	for i := 0; i < t.NumField(); i++ {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			out[name] = t.Field(i).Type
		}
	}
	return out
}()

// Validate checks a job's options_json and returns a *ValidationError listing
// every problem, or nil. Empty and null options are valid (all defaults).
// sourceSQL is the job's source_sql, used as the query in query mode when
// options_json has none.
func Validate(raw json.RawMessage, sourceSQL string, mode ValidateMode) error {
	var errs []FieldError
	add := func(field, format string, args ...any) {
		errs = append(errs, FieldError{Field: "options_json" + field, Error: fmt.Sprintf(format, args...)})
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	var keys map[string]json.RawMessage
	if trimmed[0] != '{' || json.Unmarshal(trimmed, &keys) != nil {
		add("", "must be a JSON object, got %s", jsonKind(trimmed))
		return &ValidationError{Errors: errs}
	}

	names := make([]string, 0, len(keys))
	for name := range keys {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := keys[name]
		if t, ok := optionFieldTypes[name]; ok {
			if err := json.Unmarshal(value, reflect.New(t).Interface()); err != nil {
				add("."+name, "must be %s", typeDescription(t))
			} else if name == reconciliationKey {
				checkReconciliationKeys(value, add)
			}
			continue
		}
		if name == icebergRegistrationKey {
			var reg icebergRegistrationShape
			if !isNull(value) && (bytes.TrimSpace(value)[0] != '{' || json.Unmarshal(value, &reg) != nil) {
				add("."+name, `must be an object like {"enabled": true, "engine": "rest-go", "table": "ns.table"}`)
			}
			continue
		}
		if _, ok := deprecatedKeys[name]; ok {
			continue
		}
		if mode == Strict {
			msg := "unknown option"
			if suggestion := closestOption(name); suggestion != "" {
				msg += fmt.Sprintf("; did you mean %s?", suggestion)
			}
			add("."+name, "%s", msg)
		}
	}
	if len(errs) > 0 {
		// Field values are only meaningful once every type is right.
		return &ValidationError{Errors: errs}
	}

	var o Options
	if err := json.Unmarshal(trimmed, &o); err != nil {
		add("", "%v", err)
		return &ValidationError{Errors: errs}
	}
	switch o.NormalizedPartitionStrategy() {
	case "single", "ordered_cursor":
	case "cdc_stream":
		add(".partition_strategy", "%s", ErrCDCNotSupported)
	default:
		add(".partition_strategy", "must be single or ordered_cursor, got %q", o.PartitionStrategy)
	}
	switch o.NormalizedSourceMode() {
	case "table":
	case "query":
		query := strings.TrimSpace(o.Query)
		if query == "" {
			query = strings.TrimSpace(sourceSQL)
		}
		if _, err := connectors.NormalizeReadOnlySQLQuery(query); err != nil {
			add(".query", "query mode needs a single read-only SELECT or WITH query (in query or source_sql): %v", err)
		}
	default:
		add(".source_mode", "must be table or query, got %q", o.SourceMode)
	}
	if pool := strings.TrimSpace(o.WorkerPool); pool != "" && !ValidWorkerPool(pool) {
		add(".worker_pool", "must be 1-63 lowercase letters, digits, '-' or '_', starting with a letter or digit")
	}
	if clause := strings.TrimSpace(o.WhereClause); clause != "" {
		if err := connectors.ValidateWhereClause(clause); err != nil {
			add(".where_clause", "%v", err)
		}
	}
	columns := make([]string, 0, len(o.ColumnTypes))
	for column := range o.ColumnTypes {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	for _, column := range columns {
		if err := typesystem.ValidateOverride(o.ColumnTypes[column]); err != nil {
			add(".column_types."+column, "%v", err)
		}
	}
	if lookback := strings.TrimSpace(o.CursorLookback); lookback != "" && !validLookback(lookback) {
		add(".cursor_lookback", `must be a positive integer for integer cursors or a positive duration such as "30m" or "48h", got %q`, lookback)
	}
	if o.FetchSize > connectors.MaxFetchSize {
		add(".fetch_size", "must be at most %d rows, got %d", connectors.MaxFetchSize, o.FetchSize)
	}
	if format := strings.TrimSpace(o.FileFormat); format != "" {
		if err := connectors.ValidateS3Format(format); err != nil {
			add(".format", "%v", err)
		}
	}
	o.validateReconciliation(add)
	if mode == Strict {
		// Older versions accepted these and fell back to defaults at run
		// time, so stored jobs relying on that keep running.
		strictOnlyChecks(o, add)
	}
	if len(errs) == 0 {
		return nil
	}
	sort.Slice(errs, func(i, j int) bool { return errs[i].Field < errs[j].Field })
	return &ValidationError{Errors: errs}
}

// ErrCDCNotSupported explains why change data capture options are refused.
// No worker executes a change stream, so accepting them only produced runs
// that failed later, and implied that source deletes are captured.
const ErrCDCNotSupported = "change data capture is not supported yet: incremental runs export inserts and updates and never propagate source deletes; run a full refresh to remove deleted rows (see docs/cdc-design.md)"

// strictOnlyChecks rejects values that planning tolerates (an unknown
// consistency mode is ignored, a negative number means "use the default") but
// that are almost certainly mistakes when a job is saved.
func strictOnlyChecks(o Options, add func(field, format string, args ...any)) {
	switch strings.ToUpper(strings.TrimSpace(o.ConsistencyMode)) {
	case "", "EVENTUAL", "STRONG_SNAPSHOT":
	case "STRONG_CDC":
		add(".consistency_mode", "%s", ErrCDCNotSupported)
	default:
		add(".consistency_mode", "must be EVENTUAL or STRONG_SNAPSHOT, got %q", o.ConsistencyMode)
	}
	for field, value := range map[string]int64{
		"max_in_flight_tasks":  int64(o.MaxInFlightTasks),
		"target_rows_per_task": o.TargetRowsPerTask,
		"min_tasks_multiplier": int64(o.MinTasksMultiplier),
		"planned_tasks":        int64(o.PlannedTasks),
		"chunk_size":           o.ChunkSize,
		"target_file_bytes":    o.TargetFileBytes,
		"fetch_size":           int64(o.FetchSize),
	} {
		if value < 0 {
			add("."+field, "must be 0 or greater, got %d", value)
		}
	}
}

// DeprecatedKeys lists the deprecated options_json keys present in raw.
func DeprecatedKeys(raw json.RawMessage) []string {
	var keys map[string]json.RawMessage
	if json.Unmarshal(raw, &keys) != nil {
		return nil
	}
	var out []string
	for name := range keys {
		if hint, ok := deprecatedKeys[name]; ok {
			out = append(out, fmt.Sprintf("%s (%s)", name, hint))
		}
	}
	sort.Strings(out)
	return out
}

func validLookback(text string) bool {
	if n, err := strconv.ParseUint(text, 10, 64); err == nil {
		return n > 0
	}
	d, err := time.ParseDuration(text)
	return err == nil && d > 0
}

func isNull(value json.RawMessage) bool {
	v := bytes.TrimSpace(value)
	return len(v) == 0 || bytes.Equal(v, []byte("null"))
}

func jsonKind(value []byte) string {
	switch value[0] {
	case '"':
		return "a string"
	case '[':
		return "an array"
	case 't', 'f':
		return "a boolean"
	case '{':
		return "an invalid object"
	default:
		return "a number"
	}
}

func typeDescription(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Bool:
		return "a boolean (true or false)"
	case reflect.Int, reflect.Int64:
		return "an integer"
	case reflect.Slice:
		return "an array of strings"
	case reflect.Map:
		return "an object of strings"
	case reflect.Pointer, reflect.Struct:
		return "an object"
	default:
		return "a " + t.String()
	}
}

// closestOption suggests the known option within two edits of name.
func closestOption(name string) string {
	best, bestDistance := "", 3
	candidates := []string{icebergRegistrationKey}
	for option := range optionFieldTypes {
		candidates = append(candidates, option)
	}
	sort.Strings(candidates)
	for _, option := range candidates {
		if d := editDistance(strings.ToLower(name), option); d < bestDistance {
			best, bestDistance = option, d
		}
	}
	return best
}

// editDistance is the Levenshtein distance between a and b.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}
