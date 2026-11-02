package jobopts

import (
	"strings"
	"testing"
)

func TestValidateWhereClauseEngine(t *testing.T) {
	tests := []struct {
		name    string
		opts    Options
		engine  string
		wantErr bool
	}{
		{"no filter", Options{SourceMode: "query", Query: "SELECT 1"}, "cassandra", false},
		{"table mode filter", Options{WhereClause: "a=1"}, "cassandra", false},
		{"sql query mode filter", Options{SourceMode: "query", WhereClause: "a=1"}, "postgres", false},
		{"oracle query mode filter", Options{SourceMode: "query", WhereClause: "a=1"}, "oracle", false},
		{"cassandra query mode filter", Options{SourceMode: "query", WhereClause: "a=1"}, "cassandra", true},
		{"mongodb query mode filter", Options{SourceMode: "query", WhereClause: "a=1"}, "mongodb", true},
		{"blank filter", Options{SourceMode: "query", WhereClause: "  "}, "cassandra", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateWhereClauseEngine(tc.opts, tc.engine)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "options_json.where_clause") {
				t.Fatalf("error=%v, want options_json.where_clause rejection", err)
			}
		})
	}
}
