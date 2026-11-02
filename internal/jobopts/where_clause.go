package jobopts

import (
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

// ValidateWhereClauseEngine rejects where_clause in query mode for a source
// engine that runs the query as written and so cannot apply the filter. It is
// separate from Validate because options_json does not carry the engine.
func ValidateWhereClauseEngine(o Options, engine string) error {
	if o.NormalizedSourceMode() != "query" || strings.TrimSpace(o.WhereClause) == "" || connectors.SupportsQueryModeWhereClause(engine) {
		return nil
	}
	return &ValidationError{Errors: []FieldError{{
		Field: "options_json.where_clause",
		Error: fmt.Sprintf("where_clause is not applied in query mode for source engine %q; put the filter in the query", connectors.NormalizeSourceEngine(engine)),
	}}}
}
