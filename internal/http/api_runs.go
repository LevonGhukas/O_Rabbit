package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

const (
	defaultFrontendSourceConnectionName = ""
	defaultFrontendTargetConnectionName = "s3"
	defaultFrontendTargetNamespace      = "orders"
	defaultFrontendTargetTable          = "Orders"
	defaultFrontendWriteMode            = "append"
)

func resolveFrontendWriteMode(incremental bool) string {
	if incremental {
		return "append"
	}
	return "overwrite"
}

type runSubmitRequest struct {
	Source      runSubmitSourceRequest      `json:"source"`
	Target      runSubmitTargetRequest      `json:"target"`
	Performance runSubmitPerformanceRequest `json:"performance"`
	Iceberg     runSubmitIcebergRequest     `json:"iceberg"`
	Consistency runSubmitConsistencyRequest `json:"consistency"`
}

type runSubmitConsistencyRequest struct {
	Mode string `json:"mode"`
}

type runSubmitSourceRequest struct {
	Engine       string `json:"engine"`
	DSN          string `json:"dsn"`
	Mode         string `json:"mode"`
	Table        string `json:"table"`
	Query        string `json:"query"`
	CursorColumn string `json:"cursor_column"`
	Incremental  bool   `json:"incremental"` 
	WhereClause  string `json:"where_clause,omitempty"` 
	SelectColumns []string          `json:"select_columns,omitempty"` 
	ColumnTypes   map[string]string `json:"column_types,omitempty"`
}

type runSubmitTargetRequest struct {
	S3Endpoint        string `json:"s3_endpoint"`
	S3Region          string `json:"s3_region"`
	S3Bucket          string `json:"s3_bucket"`
	S3Prefix          string `json:"s3_prefix"`
	S3ForcePathStyle  *bool  `json:"s3_force_path_style"`
	S3AccessKeyID     string `json:"s3_access_key_id"`
	S3SecretAccessKey string `json:"s3_secret_access_key"`
}

type runSubmitPerformanceRequest struct {
	AutoTune          *bool `json:"auto_tune"`
	MaxInFlightTasks  int   `json:"max_in_flight_tasks"`
	PlannedTasks      int   `json:"planned_tasks"`
	TargetRowsPerTask int64 `json:"target_rows_per_task"`
	TargetFileBytes   int64 `json:"target_file_bytes"`
}

type runSubmitIcebergRequest struct {
	Enabled       bool     `json:"enabled"`
	Engine        string   `json:"engine"`
	Table         string   `json:"table"`
	ConfigYAML    string   `json:"config_yaml"`
	PartitionKeys []string `json:"partition_keys"`
}

type existingJobRunRequest struct {
	Mode    string                  `json:"mode"`
	Iceberg runSubmitIcebergRequest `json:"iceberg"`
}

type validatedRunSubmitSpec struct {
	SourceEngine            string
	SourceDSN               string
	SourceMode              string
	SourceTable             string
	SourceQuery             string
	QueryHash               string
	WhereClause             string
	SelectColumns           []string
	ColumnTypes             map[string]string
	SourceName              string
	CursorColumn            string
	Incremental             bool
	TargetEndpoint          string
	TargetRegion            string
	TargetBucket            string
	TargetPrefixOverride    string
	TargetPrefix            string
	TargetForcePathStyle    bool
	TargetAccessKeyID       string
	TargetSecretAccessKey   string
	AutoTune                bool
	MaxInFlightTasks        int
	PlannedTasks            int
	TargetRowsPerTask       int64
	TargetFileBytes         int64
	SourceConnectionName    string
	TargetConnectionName    string
	JobName                 string
	TargetNamespace         string
	TargetTable             string
	WriteMode               string
	IcebergEnabled          bool
	IcebergEngine           string
	IcebergTable            string
	IcebergConfigYAML       string
	IcebergPartitionKeys    []string
	ParsedIceConfig         icebergreg.IceYAML
	ConsistencyMode         string
	OrderedCursorSupported  bool
	QuerySupported          bool
	FrontendSubmitSupported bool
}

type requestValidationError struct {
	message string
	details map[string]any
}

func (e *requestValidationError) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

func invalidSubmitField(field, message string, extra map[string]any) error {
	details := map[string]any{"field": field}
	for k, v := range extra {
		details[k] = v
	}
	return &requestValidationError{message: message, details: details}
}

func cloneRequestWithPath(r *http.Request, path string) *http.Request {
	clone := r.Clone(r.Context())
	u := *r.URL
	u.Path = path
	clone.URL = &u
	clone.RequestURI = path
	return clone
}

func frontendSourceConnectionName(engine string) string {
	engine = connectors.NormalizeSourceEngine(engine)
	if strings.TrimSpace(engine) == "" {
		return defaultFrontendSourceConnectionName
	}
	return engine + "_source"
}

func frontendDefaultJobName(engine, table string) string {
	t := strings.TrimSpace(table)
	if t == "" {
		return "export"
	}
	t = strings.ReplaceAll(t, "[", "")
	t = strings.ReplaceAll(t, "]", "")
	t = strings.ReplaceAll(t, ".", "_")
	t = strings.ReplaceAll(t, "/", "_")
	if len(t) > 80 {
		t = t[:80]
	}
	engine = connectors.NormalizeSourceEngine(engine)
	if strings.TrimSpace(engine) == "" {
		engine = "mssql"
	}
	return engine + "_" + t
}

func normalizedTargetDestinationIdentity(endpoint, region, bucket, prefix string, forcePathStyle bool) string {
	endpoint = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(endpoint), "/"))
	region = strings.ToLower(strings.TrimSpace(region))
	bucket = strings.TrimSpace(bucket)
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	return strings.Join([]string{endpoint, region, bucket, prefix, fmt.Sprint(forcePathStyle)}, "\x00")
}

func applyFrontendDestinationIdentity(spec *validatedRunSubmitSpec) {
	if spec == nil {
		return
	}
	identity := normalizedTargetDestinationIdentity(spec.TargetEndpoint, spec.TargetRegion, spec.TargetBucket, spec.TargetPrefixOverride, spec.TargetForcePathStyle)
	digest := sha256.Sum256([]byte(identity))
	suffix := fmt.Sprintf("%x", digest[:8])
	spec.TargetConnectionName = "s3-" + suffix
	spec.JobName += "-" + suffix
}

func normalizedIcebergEngine(raw string) string {
	engine := strings.ToLower(strings.TrimSpace(raw))
	if engine == "" {
		return "rest-go"
	}
	return engine
}

func icebergTableValid(table string) bool {
	table = strings.TrimSpace(table)
	return table != "" && strings.Contains(table, ".")
}

func (s *Server) handleAPIRuns(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/runs" {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	s.handleRuns(w, cloneRequestWithPath(r, "/runs"))
}

func (s *Server) handleAPIJobByID(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/api/jobs/") {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	jobID, isRunCreate, ok := parseJobRoute(strings.TrimPrefix(r.URL.Path, "/api"))
	if !ok || !isRunCreate {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}

	var req existingJobRunRequest
	if err := readOptionalJSON(r, &req); err != nil {
		writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
		return
	}

	job, err := s.st.GetJob(r.Context(), jobID)
	if err != nil {
		if handleLookupError(w, err, "job") {
			return
		}
		writeInternalError(w, "failed to fetch job")
		return
	}
	if _, err := validateExistingJobRunMode(job, req.Mode); err != nil {
		s.writeRunSubmitError(w, err)
		return
	}

	registrationConfig, err := s.buildExistingJobRunRegistrationConfig(r.Context(), job, req.Iceberg)
	if err != nil {
		var validationErr *requestValidationError
		if AsValidationError(err, &validationErr) {
			writeInvalidInput(w, validationErr.message, validationErr.details)
			return
		}
		writeInternalError(w, "failed to prepare registration config")
		return
	}

	run, _, err := s.createRunForJobRequest(r, job.ID, runCreateRequest{RegistrationConfig: registrationConfig})
	if err != nil {
		var validationErr *requestValidationError
		if AsValidationError(err, &validationErr) {
			writeInvalidInput(w, validationErr.message, validationErr.details)
			return
		}
		if handleLookupError(w, err, "job") {
			return
		}
		writePlannerFailure(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"run_id":     run.ID,
		"job_id":     job.ID,
		"status":     run.Status,
		"events_url": "/api/runs/" + run.ID + "/events",
		"run_url":    "/api/runs/" + run.ID,
	})
}

func (s *Server) handleAPIRunByID(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/api/runs/") {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	trimmed := strings.TrimPrefix(r.URL.Path, "/api")
	s.handleRunByID(w, cloneRequestWithPath(r, trimmed))
}

func (s *Server) handleSourceEngines(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	engines := connectors.KnownSourceEngines()
	type sourceEngineInfo struct {
		Engine                        string                     `json:"engine"`
		TableModeSupported            bool                       `json:"table_mode_supported"`
		OrderedCursorSupported        bool                       `json:"ordered_cursor_supported"`
		QuerySupported                bool                       `json:"query_supported"`
		QueryLanguages                []connectors.QueryLanguage `json:"query_languages"`
		QueryIncrementalSupported     bool                       `json:"query_incremental_supported"`
		QuerySchemaInferenceSupported bool                       `json:"query_schema_inference_supported"`
		FrontendSubmitSupported       bool                       `json:"frontend_submit_supported"`
	}
	out := make([]sourceEngineInfo, 0, len(engines))
	for _, engine := range engines {
		ordered := connectors.SupportsOrderedCursor(engine)
		queryCapabilities := connectors.QueryCapabilitiesForEngine(engine)
		out = append(out, sourceEngineInfo{
			Engine:                        engine,
			TableModeSupported:            ordered,
			OrderedCursorSupported:        ordered,
			QuerySupported:                queryCapabilities.Supported,
			QueryLanguages:                queryCapabilities.Languages,
			QueryIncrementalSupported:     queryCapabilities.IncrementalSupported,
			QuerySchemaInferenceSupported: queryCapabilities.SchemaInferenceSupported,
			FrontendSubmitSupported:       ordered || queryCapabilities.Supported,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleRunValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	var req runSubmitRequest
	if err := readJSON(r, &req); err != nil {
		writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
		return
	}
	spec, err := validateRunSubmitRequest(req)
	if err != nil {
		s.writeRunSubmitError(w, err)
		return
	}
	applyFrontendDestinationIdentity(&spec)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                        true,
		"source_engine":             spec.SourceEngine,
		"source_mode":               spec.SourceMode,
		"ordered_cursor_supported":  spec.OrderedCursorSupported,
		"query_supported":           spec.QuerySupported,
		"frontend_submit_supported": spec.FrontendSubmitSupported,
		"available_workers":         s.activeWorkerCount(r.Context()),
		"derived": map[string]any{
			"job_name":               spec.JobName,
			"source_connection_name": spec.SourceConnectionName,
			"source_name":            spec.SourceName,
			"query_hash":             spec.QueryHash,
		"where_clause":           spec.WhereClause,
		"select_columns":         spec.SelectColumns,
		"column_types":           spec.ColumnTypes,
			"target_connection_name": spec.TargetConnectionName,
			"target_prefix":          spec.TargetPrefix,
			"iceberg_table":          spec.IcebergTable,
			"target_namespace":       spec.TargetNamespace,
			"target_table":           spec.TargetTable,
			"write_mode":             spec.WriteMode,
			"auto_tune":              spec.AutoTune,
			"s3_region":              spec.TargetRegion,
			"s3_force_path_style":    spec.TargetForcePathStyle,
		},
	})
}

func (s *Server) handleRunSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	var req runSubmitRequest
	if err := readJSON(r, &req); err != nil {
		writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
		return
	}
	spec, err := validateRunSubmitRequest(req)
	if err != nil {
		s.writeRunSubmitError(w, err)
		return
	}
	applyFrontendDestinationIdentity(&spec)

	sourceReq, err := buildFrontendSourceConnectionRequest(spec)
	if err != nil {
		writeInternalError(w, "failed to prepare source connection")
		return
	}
	sourceConn, err := s.upsertConnectionByName(r, sourceReq)
	if err != nil {
		writeInternalError(w, "failed to prepare source connection")
		return
	}

	targetReq, err := buildFrontendTargetConnectionRequest(spec)
	if err != nil {
		writeInternalError(w, "failed to prepare target connection")
		return
	}
	targetConn, err := s.upsertConnectionByName(r, targetReq)
	if err != nil {
		writeInternalError(w, "failed to prepare target connection")
		return
	}

	jobReq, err := buildFrontendJobRequest(spec, sourceConn.ID, targetConn.ID)
	if err != nil {
		s.writeRunSubmitError(w, err)
		return
	}
	job, err := s.upsertJobByName(r, jobReq)
	if err != nil {
		writeInternalError(w, "failed to prepare job")
		return
	}

	registrationConfig, err := buildFrontendRegistrationConfig(spec)
	if err != nil {
		s.writeRunSubmitError(w, err)
		return
	}
	run, _, err := s.createRunForJobRequest(r, job.ID, runCreateRequest{RegistrationConfig: registrationConfig})
	if err != nil {
		writePlannerFailure(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"run_id":               run.ID,
		"job_id":               job.ID,
		"source_connection_id": sourceConn.ID,
		"target_connection_id": targetConn.ID,
		"status":               run.Status,
		"events_url":           "/api/runs/" + run.ID + "/events",
		"run_url":              "/api/runs/" + run.ID,
	})
}

func (s *Server) createRunForJobRequest(r *http.Request, jobID string, req runCreateRequest) (db.Run, []db.TaskInsert, error) {
	if len(req.RegistrationConfig) != 0 {
		cfg, err := icebergreg.ParseRunConfig(req.RegistrationConfig)
		if err != nil {
			return db.Run{}, nil, invalidSubmitField("registration_config", "invalid registration config", nil)
		}
		if !cfg.Enabled {
			req.RegistrationConfig = explicitDisabledRunRegistrationConfig()
		} else {
			raw, err := icebergreg.MarshalRunConfig(cfg)
			if err != nil {
				return db.Run{}, nil, fmt.Errorf("failed to prepare registration config: %w", err)
			}
			req.RegistrationConfig = raw
		}
	}
	j, err := s.st.GetJob(r.Context(), jobID)
	if err != nil {
		return db.Run{}, nil, err
	}

	opts, _ := jobopts.Parse(j.OptionsJSON)
	srcEngine := ""
	if srcConn, err := s.st.GetConnection(r.Context(), j.SourceConnectionID); err == nil {
		srcEngine = srcConn.Engine
	}

	planCtx, cancel := context.WithTimeout(context.Background(), runPlanningTimeout)
	defer cancel()

	planStart := time.Now()
	if dl, ok := planCtx.Deadline(); ok {
		s.log.Info("run planning started",
			slog.String("job_id", jobID),
			slog.String("source_engine", srcEngine),
			slog.String("source_mode", opts.NormalizedSourceMode()),
			slog.String("table", opts.Table),
			slog.String("query_hash", opts.QueryHash),
			slog.String("id_column", opts.IDColumn),
			slog.String("deadline", dl.UTC().Format(time.RFC3339Nano)),
		)
	} else {
		s.log.Info("run planning started",
			slog.String("job_id", jobID),
			slog.String("source_engine", srcEngine),
			slog.String("source_mode", opts.NormalizedSourceMode()),
			slog.String("table", opts.Table),
			slog.String("query_hash", opts.QueryHash),
			slog.String("id_column", opts.IDColumn),
		)
	}

	runAudit, err := s.newAuditRecord(r, auditActionJobRunStart, "run", "", nil)
	if err != nil {
		return db.Run{}, nil, fmt.Errorf("run planning failed: %w", err)
	}

	run, tasks, err := s.runPlanner(planCtx, s.st, s.k, j, req.RegistrationConfig, &runAudit)
	if err != nil {
		s.log.Error("run planning failed",
			slog.String("job_id", jobID),
			slog.String("source_engine", srcEngine),
			slog.String("source_mode", opts.NormalizedSourceMode()),
			slog.String("table", opts.Table),
			slog.String("query_hash", opts.QueryHash),
			slog.String("id_column", opts.IDColumn),
			slog.Duration("duration", time.Since(planStart)),
			slog.String("err", err.Error()),
		)
		return db.Run{}, nil, fmt.Errorf("run planning failed: %w", err)
	}
	s.log.Info("run planning completed",
		slog.String("job_id", jobID),
		slog.String("run_id", run.ID),
		slog.String("source_engine", srcEngine),
		slog.String("source_mode", opts.NormalizedSourceMode()),
		slog.String("table", opts.Table),
		slog.String("query_hash", opts.QueryHash),
		slog.String("id_column", opts.IDColumn),
		slog.Int("tasks", len(tasks)),
		slog.Duration("duration", time.Since(planStart)),
	)
	return run, tasks, nil
}

func (s *Server) writeRunSubmitError(w http.ResponseWriter, err error) {
	var validationErr *requestValidationError
	switch {
	case err == nil:
		writeInternalError(w, "internal server error")
	case strings.Contains(strings.ToLower(err.Error()), "invalid registration config"):
		writeInvalidInput(w, err.Error(), nil)
	case strings.Contains(strings.ToLower(err.Error()), "not yet supported"):
		writeNotImplemented(w, err.Error(), nil)
	case AsValidationError(err, &validationErr):
		writeInvalidInput(w, validationErr.message, validationErr.details)
	default:
		writeInternalError(w, "internal server error")
	}
}

func AsValidationError(err error, target **requestValidationError) bool {
	if target == nil {
		return false
	}
	ve, ok := err.(*requestValidationError)
	if !ok {
		return false
	}
	*target = ve
	return true
}

func (s *Server) activeWorkerCount(ctx context.Context) int {
	if s == nil || s.st == nil {
		return 0
	}
	cutoff := time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano)
	workers, err := s.st.ListWorkersActive(ctx, cutoff)
	if err != nil {
		return 0
	}
	return len(workers)
}

func validateRunSubmitRequest(req runSubmitRequest) (validatedRunSubmitSpec, error) {
	engine := connectors.NormalizeSourceEngine(req.Source.Engine)
	if strings.TrimSpace(engine) == "" {
		return validatedRunSubmitSpec{}, invalidSubmitField("source.engine", "source.engine is required", nil)
	}
	if !connectors.IsKnownSourceEngine(engine) {
		return validatedRunSubmitSpec{}, invalidSubmitField("source.engine", fmt.Sprintf("source.engine %q is not supported in this build", engine), map[string]any{
			"known_engines": connectors.KnownSourceEngines(),
		})
	}

	sourceDSN := strings.TrimSpace(req.Source.DSN)
	if sourceDSN == "" {
		return validatedRunSubmitSpec{}, invalidSubmitField("source.dsn", "source.dsn is required", nil)
	}
	sourceTable := strings.TrimSpace(req.Source.Table)
	sourceQuery := strings.TrimSpace(req.Source.Query)
	hasTable := sourceTable != ""
	hasQuery := sourceQuery != ""
	if hasTable == hasQuery {
		return validatedRunSubmitSpec{}, invalidSubmitField("source", "exactly one of source.table or source.query is required", map[string]any{
			"table_provided": hasTable,
			"query_provided": hasQuery,
		})
	}

	sourceMode := strings.ToLower(strings.TrimSpace(req.Source.Mode))
	if sourceMode == "" {
		if hasQuery {
			sourceMode = "query"
		} else {
			sourceMode = "table"
		}
	}
	switch sourceMode {
	case "table":
		if !hasTable {
			return validatedRunSubmitSpec{}, invalidSubmitField("source.mode", "source.mode=table requires source.table", nil)
		}
		if !connectors.SupportsOrderedCursor(engine) && !connectors.SupportsDocumentReader(engine) {
			return validatedRunSubmitSpec{}, invalidSubmitField("source.engine", fmt.Sprintf("source.engine %q is not yet supported by /api/runs/submit; ordered-cursor or document-reader engines only", engine), nil)
		}
	case "query", "sql":
		sourceMode = "query"
		if !hasQuery {
			return validatedRunSubmitSpec{}, invalidSubmitField("source.query", "source.query is required when source.mode=query", nil)
		}
		if hasTable {
			return validatedRunSubmitSpec{}, invalidSubmitField("source", "exactly one of source.table or source.query is required", map[string]any{
				"table_provided": true,
				"query_provided": true,
			})
		}
		if !connectors.SupportsQueryMode(engine) {
			return validatedRunSubmitSpec{}, invalidSubmitField("source.engine", fmt.Sprintf("query mode is not supported for %s", engine), nil)
		}
		normalizedQuery, err := connectors.NormalizeReadOnlySQLQuery(sourceQuery)
		if err != nil {
			return validatedRunSubmitSpec{}, invalidSubmitField("source.query", err.Error(), nil)
		}
		sourceQuery = normalizedQuery
	default:
		return validatedRunSubmitSpec{}, invalidSubmitField("source.mode", "source.mode must be table or query", map[string]any{
			"supported_modes": []string{"table", "query"},
		})
	}
	cursorColumn := strings.TrimSpace(req.Source.CursorColumn)
	if cursorColumn == "" && req.Source.Incremental && !connectors.SupportsDocumentReader(engine) {
		return validatedRunSubmitSpec{}, invalidSubmitField("source.cursor_column", "source.cursor_column is required for incremental runs", nil)
	}
	queryHash := ""
	sourceName := sourceTable
	if sourceMode == "query" {
		queryHash = connectors.QueryHash(sourceQuery)
		sourceName = "query_" + queryHash
	}

	targetEndpoint := strings.TrimSpace(req.Target.S3Endpoint)
	if targetEndpoint == "" {
		return validatedRunSubmitSpec{}, invalidSubmitField("target.s3_endpoint", "target.s3_endpoint is required", nil)
	}
	targetBucket := strings.TrimSpace(req.Target.S3Bucket)
	if targetBucket == "" {
		return validatedRunSubmitSpec{}, invalidSubmitField("target.s3_bucket", "target.s3_bucket is required", nil)
	}
	targetAccessKeyID := strings.TrimSpace(req.Target.S3AccessKeyID)
	if targetAccessKeyID == "" {
		return validatedRunSubmitSpec{}, invalidSubmitField("target.s3_access_key_id", "target.s3_access_key_id is required", nil)
	}
	targetSecretAccessKey := strings.TrimSpace(req.Target.S3SecretAccessKey)
	if targetSecretAccessKey == "" {
		return validatedRunSubmitSpec{}, invalidSubmitField("target.s3_secret_access_key", "target.s3_secret_access_key is required", nil)
	}

	targetRegion := strings.TrimSpace(req.Target.S3Region)
	if targetRegion == "" {
		targetRegion = "us-east-1"
	}
	targetForcePathStyle := true
	if req.Target.S3ForcePathStyle != nil {
		targetForcePathStyle = *req.Target.S3ForcePathStyle
	}

	autoTune := true
	if req.Performance.AutoTune != nil {
		autoTune = *req.Performance.AutoTune
	}
	if req.Performance.MaxInFlightTasks < 0 || req.Performance.PlannedTasks < 0 ||
		req.Performance.TargetRowsPerTask < 0 || req.Performance.TargetFileBytes < 0 {
		return validatedRunSubmitSpec{}, invalidSubmitField("performance", "performance values must be >= 0", nil)
	}
	if !autoTune {
		if req.Performance.MaxInFlightTasks < 1 {
			return validatedRunSubmitSpec{}, invalidSubmitField("performance.max_in_flight_tasks", "performance.max_in_flight_tasks must be >= 1 when performance.auto_tune=false", nil)
		}
		if req.Performance.PlannedTasks < 1 {
			return validatedRunSubmitSpec{}, invalidSubmitField("performance.planned_tasks", "performance.planned_tasks must be >= 1 when performance.auto_tune=false", nil)
		}
	}

	consistencyMode := strings.TrimSpace(req.Consistency.Mode)
	if consistencyMode == "" {
		consistencyMode = "EVENTUAL"
	}

	icebergEnabled := req.Iceberg.Enabled
	icebergEngine := normalizedIcebergEngine(req.Iceberg.Engine)
	icebergTable := strings.TrimSpace(req.Iceberg.Table)
	if icebergTable == "" {
		icebergTable = icebergreg.DefaultTable(engine, sourceName)
	}
	if icebergEnabled {
		switch icebergEngine {
		case "rest-go", "ice":
		default:
			return validatedRunSubmitSpec{}, invalidSubmitField("iceberg.engine", fmt.Sprintf("iceberg.engine %q is not supported", icebergEngine), map[string]any{
				"supported_engines": []string{"rest-go", "ice"},
			})
		}
		if !icebergTableValid(icebergTable) {
			return validatedRunSubmitSpec{}, invalidSubmitField("iceberg.table", "iceberg.table must use namespace.table format", nil)
		}
		rawYAML := strings.TrimSpace(req.Iceberg.ConfigYAML)
		if rawYAML == "" {
			return validatedRunSubmitSpec{}, invalidSubmitField("iceberg.config_yaml", "iceberg.config_yaml is required when iceberg.enabled=true", nil)
		}
		iceCfg, err := icebergreg.ParseIceYAMLBytes([]byte(rawYAML))
		if err != nil {
			return validatedRunSubmitSpec{}, invalidSubmitField("iceberg.config_yaml", "iceberg.config_yaml is not valid YAML", map[string]any{"cause": err.Error()})
		}
		if strings.TrimSpace(iceCfg.URI) == "" {
			return validatedRunSubmitSpec{}, invalidSubmitField("iceberg.config_yaml", "iceberg.config_yaml must define uri when iceberg.enabled=true", nil)
		}
		return validatedRunSubmitSpec{
			SourceEngine:            engine,
			SourceDSN:               sourceDSN,
			SourceMode:              sourceMode,
			SourceTable:             sourceTable,
			SourceQuery:             sourceQuery,
			QueryHash:               queryHash,
			WhereClause:             strings.TrimSpace(req.Source.WhereClause),
			SelectColumns:           req.Source.SelectColumns,
			ColumnTypes:             req.Source.ColumnTypes,
			SourceName:              sourceName,
			CursorColumn:            cursorColumn,
			Incremental:             req.Source.Incremental,
			TargetEndpoint:          targetEndpoint,
			TargetRegion:            targetRegion,
			TargetBucket:            targetBucket,
			TargetPrefixOverride:    strings.TrimSpace(req.Target.S3Prefix),
			TargetPrefix:            dataset.Prefix(req.Target.S3Prefix, engine, sourceName),
			TargetForcePathStyle:    targetForcePathStyle,
			TargetAccessKeyID:       targetAccessKeyID,
			TargetSecretAccessKey:   targetSecretAccessKey,
			AutoTune:                autoTune,
			MaxInFlightTasks:        req.Performance.MaxInFlightTasks,
			PlannedTasks:            req.Performance.PlannedTasks,
			TargetRowsPerTask:       req.Performance.TargetRowsPerTask,
			TargetFileBytes:         req.Performance.TargetFileBytes,
			SourceConnectionName:    frontendSourceConnectionName(engine),
			TargetConnectionName:    defaultFrontendTargetConnectionName,
			JobName:                 frontendDefaultJobName(engine, sourceName),
			TargetNamespace:         defaultFrontendTargetNamespace,
			TargetTable:             defaultFrontendTargetTable,
			WriteMode:               resolveFrontendWriteMode(req.Source.Incremental),
			IcebergEnabled:          true,
			IcebergEngine:           icebergEngine,
			IcebergTable:            icebergTable,
			IcebergConfigYAML:       rawYAML,
			IcebergPartitionKeys:    req.Iceberg.PartitionKeys,
			ParsedIceConfig:         iceCfg,
			ConsistencyMode:         consistencyMode,
			OrderedCursorSupported:  true,
			QuerySupported:          connectors.SupportsQueryMode(engine),
			FrontendSubmitSupported: true,
		}, nil
	}

	return validatedRunSubmitSpec{
		SourceEngine:            engine,
		SourceDSN:               sourceDSN,
		SourceMode:              sourceMode,
		SourceTable:             sourceTable,
		SourceQuery:             sourceQuery,
		QueryHash:               queryHash,
			WhereClause:             strings.TrimSpace(req.Source.WhereClause),
			SelectColumns:           req.Source.SelectColumns,
			ColumnTypes:             req.Source.ColumnTypes,
		SourceName:              sourceName,
		CursorColumn:            cursorColumn,
		Incremental:             req.Source.Incremental,
		TargetEndpoint:          targetEndpoint,
		TargetRegion:            targetRegion,
		TargetBucket:            targetBucket,
		TargetPrefixOverride:    strings.TrimSpace(req.Target.S3Prefix),
		TargetPrefix:            dataset.Prefix(req.Target.S3Prefix, engine, sourceName),
		TargetForcePathStyle:    targetForcePathStyle,
		TargetAccessKeyID:       targetAccessKeyID,
		TargetSecretAccessKey:   targetSecretAccessKey,
		AutoTune:                autoTune,
		MaxInFlightTasks:        req.Performance.MaxInFlightTasks,
		PlannedTasks:            req.Performance.PlannedTasks,
		TargetRowsPerTask:       req.Performance.TargetRowsPerTask,
		TargetFileBytes:         req.Performance.TargetFileBytes,
		SourceConnectionName:    frontendSourceConnectionName(engine),
		TargetConnectionName:    defaultFrontendTargetConnectionName,
		JobName:                 frontendDefaultJobName(engine, sourceName),
		TargetNamespace:         defaultFrontendTargetNamespace,
		TargetTable:             defaultFrontendTargetTable,
		WriteMode:               resolveFrontendWriteMode(req.Source.Incremental),
		ConsistencyMode:         consistencyMode,
		IcebergEnabled:          false,
		IcebergEngine:           icebergEngine,
		IcebergTable:            icebergTable,
		OrderedCursorSupported:  true,
		QuerySupported:          connectors.SupportsQueryMode(engine),
		FrontendSubmitSupported: true,
	}, nil
}

func buildFrontendSourceConnectionRequest(spec validatedRunSubmitSpec) (connectionCreateRequest, error) {
	secret, err := json.Marshal(map[string]any{"dsn": spec.SourceDSN})
	if err != nil {
		return connectionCreateRequest{}, err
	}
	return connectionCreateRequest{
		Name:     spec.SourceConnectionName,
		Kind:     "source",
		Engine:   spec.SourceEngine,
		Metadata: json.RawMessage(`{}`),
		Secret:   secret,
	}, nil
}

func buildFrontendTargetConnectionRequest(spec validatedRunSubmitSpec) (connectionCreateRequest, error) {
	metadata, err := json.Marshal(map[string]any{
		"endpoint":         spec.TargetEndpoint,
		"region":           spec.TargetRegion,
		"bucket":           spec.TargetBucket,
		"prefix":           spec.TargetPrefixOverride,
		"force_path_style": spec.TargetForcePathStyle,
	})
	if err != nil {
		return connectionCreateRequest{}, err
	}
	secret, err := json.Marshal(map[string]any{
		"access_key_id":     spec.TargetAccessKeyID,
		"secret_access_key": spec.TargetSecretAccessKey,
	})
	if err != nil {
		return connectionCreateRequest{}, err
	}
	return connectionCreateRequest{
		Name:     spec.TargetConnectionName,
		Kind:     "target",
		Engine:   "s3",
		Metadata: metadata,
		Secret:   secret,
	}, nil
}

func buildFrontendJobRequest(spec validatedRunSubmitSpec, sourceConnectionID, targetConnectionID string) (jobCreateRequest, error) {

	partitionStrategy := "ordered_cursor"
	if connectors.SupportsDocumentReader(spec.SourceEngine) {
		partitionStrategy = "single"
	} else if spec.ConsistencyMode == "STRONG_CDC" {
		partitionStrategy = "cdc_stream"
	}
	options := map[string]any{
		"source_mode":            spec.SourceMode,
		"source_name":            spec.SourceName,
		"table":                  spec.SourceTable,
		"query":                  spec.SourceQuery,
		"query_hash":             spec.QueryHash,
		"partition_strategy":     partitionStrategy,
		"auto_tune":              spec.AutoTune,
		"max_in_flight_tasks":    spec.MaxInFlightTasks,
		"target_rows_per_task":   spec.TargetRowsPerTask,
		"cursor_column":          spec.CursorColumn,
		"id_column":              spec.CursorColumn,
		"planned_tasks":          spec.PlannedTasks,
		"target_file_bytes":      spec.TargetFileBytes,
		"iceberg_enabled":        spec.IcebergEnabled,
		"iceberg_engine":         spec.IcebergEngine,
		"iceberg_table":          spec.IcebergTable,
		"iceberg_partition_keys": spec.IcebergPartitionKeys,
		"consistency_mode":       spec.ConsistencyMode,
	}
	options = icebergreg.MergeJobConfig(options, icebergreg.JobConfig{
		Enabled: spec.IcebergEnabled,
		Engine:  spec.IcebergEngine,
		Table:   spec.IcebergTable,
	})
	optionsJSON, err := json.Marshal(options)
	if err != nil {
		return jobCreateRequest{}, err
	}
	hwmColumn := spec.CursorColumn
	return jobCreateRequest{
		Name:               spec.JobName,
		SourceConnectionID: sourceConnectionID,
		TargetConnectionID: targetConnectionID,
		SourceSQL:          spec.SourceQuery,
		TargetNamespace:    spec.TargetNamespace,
		TargetTable:        spec.TargetTable,
		WriteMode:          spec.WriteMode,
		Incremental:        spec.Incremental,
		HWMColumn:          &hwmColumn,
		OptionsJSON:        optionsJSON,
	}, nil
}

func buildFrontendRegistrationConfig(spec validatedRunSubmitSpec) (json.RawMessage, error) {
	if !spec.IcebergEnabled {
		return nil, nil
	}
	runCfg := icebergreg.ResolveRunConfig(
		true,
		spec.IcebergEngine,
		spec.IcebergTable,
		s3io.Config{
			Endpoint:        spec.TargetEndpoint,
			Region:          spec.TargetRegion,
			Bucket:          spec.TargetBucket,
			ForcePathStyle:  spec.TargetForcePathStyle,
			AccessKeyID:     spec.TargetAccessKeyID,
			SecretAccessKey: spec.TargetSecretAccessKey,
		},
		spec.ParsedIceConfig,
	)
	if spec.IcebergEngine == "ice" {
		runCfg.ConfigYAML = spec.IcebergConfigYAML
	}
	return icebergreg.MarshalRunConfig(runCfg)
}

func explicitDisabledRunRegistrationConfig() json.RawMessage {
	return json.RawMessage(`{"enabled":false}`)
}

func validateExistingJobRunMode(job db.Job, raw string) (string, error) {
	want := "full"
	if job.Incremental {
		want = "incremental"
	}
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		return want, nil
	}
	if mode != "incremental" && mode != "full" {
		return "", invalidSubmitField("mode", "mode must be incremental or full", map[string]any{
			"supported_modes": []string{"incremental", "full"},
		})
	}
	if mode != want {
		return "", invalidSubmitField("mode", "mode must match the existing job configuration", map[string]any{
			"job_mode": want,
		})
	}
	return mode, nil
}

func (s *Server) buildExistingJobRunRegistrationConfig(ctx context.Context, job db.Job, req runSubmitIcebergRequest) (json.RawMessage, error) {
	if !req.Enabled {
		return explicitDisabledRunRegistrationConfig(), nil
	}

	rawYAML := strings.TrimSpace(req.ConfigYAML)
	if rawYAML == "" {
		return nil, invalidSubmitField("iceberg.config_yaml", "iceberg.config_yaml is required when iceberg.enabled=true", nil)
	}
	iceCfg, err := icebergreg.ParseIceYAMLBytes([]byte(rawYAML))
	if err != nil {
		return nil, invalidSubmitField("iceberg.config_yaml", "iceberg.config_yaml is not valid YAML", map[string]any{"cause": err.Error()})
	}
	if strings.TrimSpace(iceCfg.URI) == "" {
		return nil, invalidSubmitField("iceberg.config_yaml", "iceberg.config_yaml must define uri when iceberg.enabled=true", nil)
	}

	engine := normalizedIcebergEngine(req.Engine)
	jobRegCfg, _ := icebergreg.ParseJobConfig(job.OptionsJSON)
	if strings.TrimSpace(req.Engine) == "" && strings.TrimSpace(jobRegCfg.Engine) != "" {
		engine = normalizedIcebergEngine(jobRegCfg.Engine)
	}
	switch engine {
	case "rest-go", "ice":
	default:
		return nil, invalidSubmitField("iceberg.engine", fmt.Sprintf("iceberg.engine %q is not supported", engine), map[string]any{
			"supported_engines": []string{"rest-go", "ice"},
		})
	}

	srcConn, err := s.st.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		return nil, err
	}
	table := strings.TrimSpace(req.Table)
	if table == "" {
		table = strings.TrimSpace(jobRegCfg.Table)
	}
	if table == "" {
		opts, _ := jobopts.Parse(job.OptionsJSON)
		sourceTable := strings.TrimSpace(opts.Table)
		if sourceTable == "" {
			sourceTable = job.TargetTable
		}
		table = icebergreg.DefaultTable(srcConn.Engine, sourceTable)
	}
	if !icebergTableValid(table) {
		return nil, invalidSubmitField("iceberg.table", "iceberg.table must use namespace.table format", nil)
	}

	tgtConn, err := s.st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return nil, err
	}
	baseS3, err := s.loadConnectionS3Config(tgtConn)
	if err != nil {
		return nil, err
	}

	runCfg := icebergreg.ResolveRunConfig(true, engine, table, baseS3, iceCfg)
	if engine == "ice" {
		runCfg.ConfigYAML = rawYAML
	}
	return icebergreg.MarshalRunConfig(runCfg)
}

func (s *Server) loadConnectionS3Config(conn db.Connection) (s3io.Config, error) {
	var metadata map[string]any
	_ = json.Unmarshal(conn.MetadataJSON, &metadata)

	secretPlain, err := crypto.Decrypt(s.k, conn.SecretEncBlob, []byte(conn.ID))
	if err != nil {
		return s3io.Config{}, err
	}
	var secret map[string]any
	_ = json.Unmarshal(secretPlain, &secret)

	forcePathStyle := true
	if v, ok := metadata["force_path_style"].(bool); ok {
		forcePathStyle = v
	}
	cfg := s3io.Config{
		Endpoint:        strings.TrimSpace(anyStringValue(metadata["endpoint"])),
		Region:          strings.TrimSpace(anyStringValue(metadata["region"])),
		Bucket:          strings.TrimSpace(anyStringValue(metadata["bucket"])),
		ForcePathStyle:  forcePathStyle,
		AccessKeyID:     strings.TrimSpace(anyStringValue(secret["access_key_id"])),
		SecretAccessKey: strings.TrimSpace(anyStringValue(secret["secret_access_key"])),
	}
	return cfg, nil
}

func anyStringValue(v any) string {
	s, _ := v.(string)
	return s
}

func (s *Server) upsertConnectionByName(r *http.Request, req connectionCreateRequest) (db.Connection, error) {
	connections, err := s.st.ListConnections(r.Context())
	if err != nil {
		return db.Connection{}, err
	}
	for _, existing := range connections {
		if existing.Name != req.Name {
			continue
		}
		if req.Kind == "target" && req.Engine == "s3" && req.Metadata != nil {
			var before, after map[string]any
			if json.Unmarshal(existing.MetadataJSON, &before) != nil || json.Unmarshal(req.Metadata, &after) != nil ||
				normalizedTargetDestinationIdentity(anyStringValue(before["endpoint"]), anyStringValue(before["region"]), anyStringValue(before["bucket"]), anyStringValue(before["prefix"]), boolValue(before["force_path_style"], true)) !=
					normalizedTargetDestinationIdentity(anyStringValue(after["endpoint"]), anyStringValue(after["region"]), anyStringValue(after["bucket"]), anyStringValue(after["prefix"]), boolValue(after["force_path_style"], true)) {
				return db.Connection{}, fmt.Errorf("target connection identity collision for %q", req.Name)
			}
		}
		blob := existing.SecretEncBlob
		if len(req.Secret) != 0 {
			blob, err = crypto.Encrypt(s.k, req.Secret, []byte(existing.ID))
			if err != nil {
				return db.Connection{}, err
			}
		}
		metadata := existing.MetadataJSON
		if req.Metadata != nil {
			metadata = append(json.RawMessage(nil), req.Metadata...)
		}
		upd := db.Connection{
			ID:            existing.ID,
			Name:          req.Name,
			Kind:          req.Kind,
			Engine:        req.Engine,
			MetadataJSON:  metadata,
			SecretEncBlob: blob,
		}
		audit, err := s.newAuditRecord(r, auditActionConnectionUpdate, "connection", existing.ID, nil)
		if err != nil {
			return db.Connection{}, err
		}
		return s.st.UpdateConnectionAudited(r.Context(), existing, upd, audit)
	}

	id := newID()
	blob, err := crypto.Encrypt(s.k, req.Secret, []byte(id))
	if err != nil {
		return db.Connection{}, err
	}
	conn := db.Connection{
		ID:            id,
		Name:          req.Name,
		Kind:          req.Kind,
		Engine:        req.Engine,
		MetadataJSON:  append(json.RawMessage(nil), req.Metadata...),
		SecretEncBlob: blob,
	}
	audit, err := s.newAuditRecord(r, auditActionConnectionCreate, "connection", conn.ID, nil)
	if err != nil {
		return db.Connection{}, err
	}
	return s.st.CreateConnectionAudited(r.Context(), conn, audit)
}

func boolValue(v any, fallback bool) bool {
	b, ok := v.(bool)
	if !ok {
		return fallback
	}
	return b
}

func (s *Server) upsertJobByName(r *http.Request, req jobCreateRequest) (db.Job, error) {
	jobs, err := s.st.ListJobs(r.Context())
	if err != nil {
		return db.Job{}, err
	}
	for _, existing := range jobs {
		if existing.Name != req.Name {
			continue
		}
		upd := db.Job{
			ID:                 existing.ID,
			Name:               req.Name,
			SourceConnectionID: req.SourceConnectionID,
			TargetConnectionID: req.TargetConnectionID,
			SourceSQL:          req.SourceSQL,
			TargetNamespace:    req.TargetNamespace,
			TargetTable:        req.TargetTable,
			WriteMode:          req.WriteMode,
			Incremental:        req.Incremental,
			HWMColumn:          req.HWMColumn,
			OptionsJSON:        append(json.RawMessage(nil), req.OptionsJSON...),
			CreatedAt:          existing.CreatedAt,
		}
		audit, err := s.newAuditRecord(r, auditActionJobUpdate, "job", existing.ID, nil)
		if err != nil {
			return db.Job{}, err
		}
		return s.st.UpdateJobAudited(r.Context(), existing, upd, audit)
	}

	job := db.Job{
		ID:                 newID(),
		Name:               req.Name,
		SourceConnectionID: req.SourceConnectionID,
		TargetConnectionID: req.TargetConnectionID,
		SourceSQL:          req.SourceSQL,
		TargetNamespace:    req.TargetNamespace,
		TargetTable:        req.TargetTable,
		WriteMode:          req.WriteMode,
		Incremental:        req.Incremental,
		HWMColumn:          req.HWMColumn,
		OptionsJSON:        append(json.RawMessage(nil), req.OptionsJSON...),
	}
	audit, err := s.newAuditRecord(r, auditActionJobCreate, "job", job.ID, nil)
	if err != nil {
		return db.Job{}, err
	}
	return s.st.CreateJobAudited(r.Context(), job, audit)
}
