package jobopts

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReconciliationDefaultsToOff(t *testing.T) {
	o, err := Parse(json.RawMessage(`{"table":"t"}`))
	if err != nil {
		t.Fatal(err)
	}
	got := o.EffectiveReconciliation()
	if got.Enabled() || got.Mode != ReconciliationModeOff || got.Consistency != ReconciliationConsistencyNone || got.OnMismatch != ReconciliationOnMismatchFail {
		t.Fatalf("defaults=%+v", got)
	}
}

func TestReconciliationEffectiveNormalizesCase(t *testing.T) {
	o, err := Parse(json.RawMessage(`{"reconciliation":{"mode":"COUNT","consistency":"Immutable","on_mismatch":"REVIEW"}}`))
	if err != nil {
		t.Fatal(err)
	}
	got := o.EffectiveReconciliation()
	if !got.Enabled() || got.Consistency != ReconciliationConsistencyImmutable || got.OnMismatch != ReconciliationOnMismatchReview {
		t.Fatalf("effective=%+v", got)
	}
}

func TestValidateReconciliation(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		mode    ValidateMode
		wantErr string
	}{
		{"absent", `{"table":"t"}`, Strict, ""},
		{"off", `{"reconciliation":{"mode":"off"}}`, Strict, ""},
		{"count ordered cursor", `{"partition_strategy":"ordered_cursor","reconciliation":{"mode":"count","consistency":"immutable","on_mismatch":"review"}}`, Strict, ""},
		{"count with where in table mode", `{"partition_strategy":"ordered_cursor","where_clause":"a=1","reconciliation":{"mode":"count"}}`, Strict, ""},
		{"bad mode", `{"reconciliation":{"mode":"sample"}}`, Strict, "reconciliation.mode"},
		{"bad consistency", `{"reconciliation":{"consistency":"snapshot"}}`, Strict, "reconciliation.consistency"},
		{"bad on_mismatch", `{"reconciliation":{"on_mismatch":"ignore"}}`, Strict, "reconciliation.on_mismatch"},
		{"bad value lenient", `{"reconciliation":{"mode":"sample"}}`, Lenient, "reconciliation.mode"},
		{"unknown key", `{"reconciliation":{"modee":"count"}}`, Strict, "unknown field"},
		{"not an object", `{"reconciliation":"count"}`, Strict, "must be an object"},
		{"single strategy", `{"partition_strategy":"single","reconciliation":{"mode":"count"}}`, Strict, "ordered_cursor"},
		{"query mode with where", `{"partition_strategy":"ordered_cursor","source_mode":"query","query":"SELECT 1 AS id","where_clause":"a=1","reconciliation":{"mode":"count"}}`, Strict, "where_clause"},
		{"query mode without where", `{"partition_strategy":"ordered_cursor","source_mode":"query","query":"SELECT 1 AS id","reconciliation":{"mode":"count"}}`, Strict, ""},
		{"query mode where but off", `{"source_mode":"query","query":"SELECT 1 AS id","where_clause":"a=1","reconciliation":{"mode":"off"}}`, Strict, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(json.RawMessage(tc.raw), "", tc.mode)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error=%v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateReconciliationEngine(t *testing.T) {
	count := Options{Reconciliation: &Reconciliation{Mode: ReconciliationModeCount}}
	for _, engine := range []string{"postgres", "mysql", "mariadb", "mssql", "oracle", "clickhouse", "trino"} {
		if err := ValidateReconciliationEngine(count, engine); err != nil {
			t.Errorf("%s rejected: %v", engine, err)
		}
	}
	for _, engine := range []string{"mongodb", "cassandra", "s3", "flightsql", "other"} {
		if err := ValidateReconciliationEngine(count, engine); err == nil {
			t.Errorf("%s accepted", engine)
		}
	}
	if err := ValidateReconciliationEngine(Options{}, "mongodb"); err != nil {
		t.Errorf("off must accept any engine: %v", err)
	}
}

func TestMergeIntoKeepsReconciliation(t *testing.T) {
	o := Options{Reconciliation: &Reconciliation{Mode: "COUNT"}}
	m := o.MergeInto(map[string]any{})
	got, ok := m["reconciliation"].(Reconciliation)
	if !ok || got.Mode != ReconciliationModeCount {
		t.Fatalf("merged=%v", m["reconciliation"])
	}
	if _, ok := (Options{}).MergeInto(map[string]any{"reconciliation": 1})["reconciliation"]; ok {
		t.Fatal("absent reconciliation must be removed")
	}
}
