package jobopts

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func validationErrors(t *testing.T, raw, sourceSQL string, mode ValidateMode) []FieldError {
	t.Helper()
	err := Validate(json.RawMessage(raw), sourceSQL, mode)
	if err == nil {
		return nil
	}
	var invalid *ValidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("Validate(%s) returned %T, want *ValidationError", raw, err)
	}
	return invalid.Errors
}

func TestValidateAcceptsValidOptions(t *testing.T) {
	valid := []string{
		``,
		`null`,
		`{}`,
		`{"partition_strategy":"single"}`,
		`{"partition_strategy":"int_range","id_column":"id","chunk_size":1000}`,
		`{"partition_strategy":"cdc_stream","consistency_mode":"strong_cdc"}`,
		`{"source_mode":"query","query":"SELECT id FROM t"}`,
		`{"worker_pool":"etl-a","where_clause":"status = 'open'","cursor_lookback":"48h"}`,
		`{"cursor_lookback":"1000","column_types":{"amount":"decimal(12,2)","note":"nullable<source>"}}`,
		`{"format":"xlsx","record_path":"/items","select_columns":["a","b"],"partition_keys":["a"]}`,
		// Keys O_Rabbit itself writes: the registration config and leftovers
		// from older versions, so stored jobs can be fetched and saved back.
		`{"iceberg_registration":{"enabled":true,"engine":"rest-go","table":"ns.t"},"iceberg_enabled":true,"iceberg_engine":"rest-go","iceberg_table":"ns.t","iceberg_partition_keys":[],"fetch_limit_rows":5000}`,
	}
	for _, raw := range valid {
		if errs := validationErrors(t, raw, "", Strict); errs != nil {
			t.Errorf("Validate(%s) = %+v, want valid", raw, errs)
		}
	}
	// Query mode may take its query from the job's source_sql.
	if errs := validationErrors(t, `{"source_mode":"query"}`, "SELECT 1", Strict); errs != nil {
		t.Errorf("query from source_sql: %+v", errs)
	}
}

func TestValidateRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name, raw, field, contains string
	}{
		{"string", `"not an object"`, "options_json", "must be a JSON object, got a string"},
		{"array", `[1,2]`, "options_json", "got an array"},
		{"number", `42`, "options_json", "got a number"},
		{"broken object", `{`, "options_json", "got an invalid object"},
		{"wrong type", `{"planned_tasks":"ten"}`, "options_json.planned_tasks", "must be an integer"},
		{"wrong bool", `{"auto_tune":"yes"}`, "options_json.auto_tune", "must be a boolean"},
		{"typo", `{"planed_tasks":4}`, "options_json.planed_tasks", "did you mean planned_tasks?"},
		{"unknown", `{"colour":"red"}`, "options_json.colour", "unknown option"},
		{"registration shape", `{"iceberg_registration":"yes"}`, "options_json.iceberg_registration", "must be an object"},
		{"strategy", `{"partition_strategy":"bogus"}`, "options_json.partition_strategy", `got "bogus"`},
		{"source mode", `{"source_mode":"stream"}`, "options_json.source_mode", "must be table or query"},
		{"query mode without query", `{"source_mode":"query"}`, "options_json.query", "read-only"},
		{"write query", `{"source_mode":"query","query":"DELETE FROM t"}`, "options_json.query", "read-only"},
		{"worker pool", `{"worker_pool":"Bad Pool!"}`, "options_json.worker_pool", "lowercase"},
		{"where clause", `{"where_clause":"1=1; DROP TABLE x"}`, "options_json.where_clause", "';'"},
		{"column type", `{"column_types":{"a":"notatype<"}}`, "options_json.column_types.a", ""},
		{"lookback", `{"cursor_lookback":"soon"}`, "options_json.cursor_lookback", "positive"},
		{"zero lookback", `{"cursor_lookback":"0"}`, "options_json.cursor_lookback", "positive"},
		{"format", `{"format":"pdf"}`, "options_json.format", "unsupported"},
		{"consistency", `{"consistency_mode":"sometimes"}`, "options_json.consistency_mode", "EVENTUAL"},
		{"negative", `{"max_in_flight_tasks":-1}`, "options_json.max_in_flight_tasks", "0 or greater"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validationErrors(t, tt.raw, "", Strict)
			if len(errs) == 0 {
				t.Fatalf("Validate(%s) = valid, want an error for %s", tt.raw, tt.field)
			}
			for _, fe := range errs {
				if fe.Field == tt.field && strings.Contains(fe.Error, tt.contains) {
					return
				}
			}
			t.Fatalf("Validate(%s) = %+v, want %s containing %q", tt.raw, errs, tt.field, tt.contains)
		})
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	errs := validationErrors(t, `{"planned_tasks":"ten","planed_tasks":4,"auto_tune":1}`, "", Strict)
	if len(errs) != 3 {
		t.Fatalf("errors=%+v, want all 3 problems", errs)
	}
	errs = validationErrors(t, `{"worker_pool":"BAD","where_clause":"a;b","partition_strategy":"x"}`, "", Strict)
	if len(errs) != 3 {
		t.Fatalf("errors=%+v, want all 3 problems", errs)
	}
}

func TestValidateLenientAcceptsWhatPlanningTolerates(t *testing.T) {
	// Stored before strict validation: unknown keys, an unknown consistency
	// mode and negative numbers (defaults at run time) keep running.
	if errs := validationErrors(t, `{"planed_tasks":4,"consistency_mode":"sometimes","target_rows_per_task":-1}`, "", Lenient); errs != nil {
		t.Fatalf("lenient errors=%+v, want none", errs)
	}
	// Problems that would make planning fail are still reported.
	for _, raw := range []string{`"text"`, `{"planned_tasks":"ten"}`, `{"worker_pool":"Bad Pool!"}`, `{"partition_strategy":"bogus"}`, `{"where_clause":"a; b"}`} {
		if errs := validationErrors(t, raw, "", Lenient); errs == nil {
			t.Errorf("lenient Validate(%s) = valid, want an error", raw)
		}
	}
}

func TestDeprecatedKeys(t *testing.T) {
	got := DeprecatedKeys(json.RawMessage(`{"fetch_limit_rows":1,"iceberg_table":"t","table":"x"}`))
	if len(got) != 2 || !strings.HasPrefix(got[0], "fetch_limit_rows") || !strings.HasPrefix(got[1], "iceberg_table") {
		t.Fatalf("DeprecatedKeys=%v", got)
	}
	if DeprecatedKeys(json.RawMessage(`"x"`)) != nil {
		t.Fatal("non-object options have no deprecated keys")
	}
}
