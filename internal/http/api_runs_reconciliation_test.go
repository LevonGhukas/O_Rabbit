package httpapi

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

func TestValidateSubmitReconciliation(t *testing.T) {
	count := &jobopts.Reconciliation{Mode: "count", Consistency: "immutable", OnMismatch: "review"}
	tests := []struct {
		name    string
		req     runSubmitRequest
		mode    string
		engine  string
		wantErr string
	}{
		{"absent", runSubmitRequest{}, "table", "mongodb", ""},
		{"count on postgres", runSubmitRequest{Reconciliation: count}, "table", "postgres", ""},
		{"count on mongodb", runSubmitRequest{Reconciliation: count}, "table", "mongodb", "not supported"},
		{"bad mode", runSubmitRequest{Reconciliation: &jobopts.Reconciliation{Mode: "sample"}}, "table", "postgres", "reconciliation.mode"},
		{"query mode with where", runSubmitRequest{Reconciliation: count, Source: runSubmitSourceRequest{WhereClause: "a=1"}}, "query", "postgres", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSubmitReconciliation(tc.req, tc.mode, tc.engine)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error=%v want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestOneShotJobOptionsCarryReconciliation(t *testing.T) {
	spec := validatedRunSubmitSpec{SourceEngine: "postgres", SourceMode: "table", SourceTable: "t", Reconciliation: &jobopts.Reconciliation{Mode: "count", Consistency: "immutable"}}
	req, err := buildOneShotJobRequest(spec, "src", "tgt")
	if err != nil {
		t.Fatal(err)
	}
	opts, err := jobopts.Parse(json.RawMessage(req.OptionsJSON))
	if err != nil {
		t.Fatal(err)
	}
	got := opts.EffectiveReconciliation()
	if !got.Enabled() || got.Consistency != jobopts.ReconciliationConsistencyImmutable {
		t.Fatalf("effective=%+v", got)
	}
}

func TestJobCreateValidatesReconciliationAgainstSourceEngine(t *testing.T) {
	srv, st := jobValidationServer(t) // postgres source
	ok := serveJSON(srv, "POST", "/api/v1/jobs", jobBody(`{"partition_strategy":"ordered_cursor","reconciliation":{"mode":"count","consistency":"immutable"}}`))
	if ok.Code != 201 {
		t.Fatalf("postgres count job: status=%d body=%s", ok.Code, ok.Body.String())
	}
	createTestConnection(t, st, db.Connection{ID: "mongo", Name: "mongo", Kind: "source", Engine: "mongodb", MetadataJSON: []byte(`{}`), SecretEncBlob: []byte(`{}`)})
	body := strings.Replace(jobBody(`{"reconciliation":{"mode":"count"}}`), `"src"`, `"mongo"`, 1)
	rec := serveJSON(srv, "POST", "/api/v1/jobs", body)
	if resp := decodeJobError(t, rec); rec.Code != 400 || !resp.hasField("options_json.reconciliation.mode") {
		t.Fatalf("mongodb count job: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestJobCreateRejectsQueryModeWhereClauseOnCassandra(t *testing.T) {
	srv, st := jobValidationServer(t) // postgres source
	options := `{"source_mode":"query","query":"SELECT id FROM ks.orders","where_clause":"status = 'open'"}`
	ok := serveJSON(srv, "POST", "/api/v1/jobs", jobBody(options))
	if ok.Code != 201 {
		t.Fatalf("postgres query-mode filter: status=%d body=%s", ok.Code, ok.Body.String())
	}
	createTestConnection(t, st, db.Connection{ID: "cass", Name: "cass", Kind: "source", Engine: "cassandra", MetadataJSON: []byte(`{}`), SecretEncBlob: []byte(`{}`)})
	body := strings.Replace(jobBody(options), `"src"`, `"cass"`, 1)
	rec := serveJSON(srv, "POST", "/api/v1/jobs", body)
	if resp := decodeJobError(t, rec); rec.Code != 400 || !resp.hasField("options_json.where_clause") {
		t.Fatalf("cassandra query-mode filter: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestAPIRunValidateQueryModeWhereClauseByEngine(t *testing.T) {
	tests := []struct {
		name    string
		engine  string
		dsn     string
		wantErr bool
	}{
		{"postgres applies filter", "postgres", "postgresql://user:pass@db:5432/app?sslmode=disable", false},
		{"cassandra rejects filter", "cassandra", "cassandra://db:9042/ks", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRunSubmitRequestForValidation()
			req.Source.Engine, req.Source.DSN = tc.engine, tc.dsn
			req.Source.Mode, req.Source.Table, req.Source.Query = "query", "", "SELECT id, status FROM orders"
			req.Source.WhereClause = "status = 'open'"
			spec, err := validateRunSubmitRequest(req)
			if !tc.wantErr {
				if err != nil || spec.WhereClause != "status = 'open'" {
					t.Fatalf("spec.WhereClause=%q err=%v", spec.WhereClause, err)
				}
				return
			}
			var validationErr *requestValidationError
			if !AsValidationError(err, &validationErr) || validationErr.details["field"] != "source.where_clause" {
				t.Fatalf("err=%v, want source.where_clause rejection", err)
			}
		})
	}
}
