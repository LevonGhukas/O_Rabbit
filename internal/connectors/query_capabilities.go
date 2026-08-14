package connectors

// QueryLanguage identifies the engine-native language carried by a query-mode
// request. Batch A exposes this metadata without changing the existing SQL
// string request or execution contracts.
type QueryLanguage string

const (
	QueryLanguagePostgresSQL   QueryLanguage = "postgres_sql"
	QueryLanguageMySQLSQL      QueryLanguage = "mysql_sql"
	QueryLanguageMariaDBSQL    QueryLanguage = "mariadb_sql"
	QueryLanguageTSQL          QueryLanguage = "tsql"
	QueryLanguageOracleSQL     QueryLanguage = "oracle_sql"
	QueryLanguageClickHouseSQL QueryLanguage = "clickhouse_sql"
	QueryLanguageTrinoSQL      QueryLanguage = "trino_sql"

	// Reserved for future engine-native implementations. Their presence as
	// constants does not advertise runtime support.
	QueryLanguageMongoFilter   QueryLanguage = "mongo_filter"
	QueryLanguageMongoPipeline QueryLanguage = "mongo_pipeline"
	QueryLanguageCQL           QueryLanguage = "cql"
	QueryLanguageFlightSQL     QueryLanguage = "flightsql_sql"
)

// QueryCapabilities describes query-mode behavior that is implemented end to
// end for an engine. Supported remains explicit so partially implemented
// connector methods cannot accidentally advertise a public API capability.
type QueryCapabilities struct {
	Supported                bool
	Languages                []QueryLanguage
	IncrementalSupported     bool
	SchemaInferenceSupported bool
}

func supportedSQLQueryCapabilities(language QueryLanguage) QueryCapabilities {
	return QueryCapabilities{
		Supported:                true,
		Languages:                []QueryLanguage{language},
		IncrementalSupported:     true,
		SchemaInferenceSupported: true,
	}
}

func cloneQueryCapabilities(capabilities QueryCapabilities) QueryCapabilities {
	out := capabilities
	out.Languages = append([]QueryLanguage{}, capabilities.Languages...)
	return out
}

// QueryCapabilitiesForEngine returns a defensive copy of the registered
// query-mode capabilities. Unknown and unsupported engines return an empty
// language list and false feature flags.
func QueryCapabilitiesForEngine(engine string) QueryCapabilities {
	spec, ok := sourceEnginesByCanonical[NormalizeSourceEngine(engine)]
	if !ok {
		return QueryCapabilities{Languages: []QueryLanguage{}}
	}
	return cloneQueryCapabilities(spec.QueryCapabilities)
}

// DefaultQueryLanguageForEngine returns the implicit language used by the
// legacy source.query string contract when exactly one language is advertised.
// Batch B can use this helper without changing Batch A runtime behavior.
func DefaultQueryLanguageForEngine(engine string) (QueryLanguage, bool) {
	capabilities := QueryCapabilitiesForEngine(engine)
	if !capabilities.Supported || len(capabilities.Languages) != 1 {
		return "", false
	}
	return capabilities.Languages[0], true
}
