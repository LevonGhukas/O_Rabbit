// Package httpapi implements the HTTP API server for the control plane.
// It provides endpoints for managing connections, jobs, runs, and workers, as well as a health check and status endpoint.
// The server uses a db.Store for persistence, a Broadcaster for real-time updates, and a crypto.Key for encrypting secrets at rest.
// The API is designed to be simple and RESTful, with JSON request and response bodies.

package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
	"github.com/LevonGhukas/O_Rabbit/internal/planner"
)

// Server represents the HTTP API server for the control plane.
// It holds references to the logger, database store, broadcaster, encryption key, and status information.
type Server struct {
	log              *slog.Logger
	st               *db.Store
	bc               *Broadcaster
	k                crypto.Key
	authToken        string
	status           StatusInfo
	streams          *StreamHub
	runPlanner       runPlannerFunc
	sshTester        sshTester
	sshExec          sshCommandExecutor
	docker           dockerService
	configs          configService
	deploy           deployService
	taskMaxAttempts  int
	commitReconciler interface {
		ReconcileCommittingRuns(context.Context) error
	}
	leadership interface {
		Assert(context.Context) error
		Status() db.Leadership
	}
}

type runPlannerFunc func(context.Context, *db.Store, crypto.Key, db.Job, json.RawMessage, *db.AuditRecord) (db.Run, []db.TaskInsert, error)

const runPlanningTimeout = 10 * time.Minute

// StatusInfo represents the status information of the server, including the PID, HTTP and gRPC addresses, and database path.
type StatusInfo struct {
	PID      int    `json:"pid"`
	HTTPAddr string `json:"http_addr"`
	GRPCAddr string `json:"grpc_addr"`
	DBPath   string `json:"db_path"`
}

// NewServer creates a new instance of the Server with the provided logger, database store, broadcaster, encryption key, and status information.
func NewServer(log *slog.Logger, st *db.Store, bc *Broadcaster, k crypto.Key, status StatusInfo, authToken string) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{
		log:             log,
		st:              st,
		bc:              bc,
		k:               k,
		status:          status,
		authToken:       strings.TrimSpace(authToken),
		streams:         NewStreamHub(log),
		runPlanner:      planner.CreateRunAndTasks,
		taskMaxAttempts: 3,
	}
	s.sshTester = sshTesterFunc(sshops.TestConnection)
	s.sshExec = sshCommandExecutorFunc(sshops.ExecuteCommand)
	s.docker = newDockerService(s.sshExec)
	s.configs = newConfigService(s.sshExec)
	s.deploy = newDeployService(s.sshExec, s.docker, s.sshTester)
	return s
}

func (s *Server) SetLeadershipGuard(guard interface {
	Assert(context.Context) error
	Status() db.Leadership
}) {
	s.leadership = guard
}

func (s *Server) SetOperability(taskMaxAttempts int, reconciler interface {
	ReconcileCommittingRuns(context.Context) error
}) {
	if taskMaxAttempts > 0 {
		s.taskMaxAttempts = taskMaxAttempts
	}
	s.commitReconciler = reconciler
}

// Handler returns the HTTP handler for the server, which routes incoming requests to the appropriate handler functions based on the URL path and HTTP method.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/metrics", s.handleMetrics)

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, r.Method, http.MethodGet)
			return
		}
		if s.leadership == nil {
			writeJSON(w, http.StatusOK, s.status)
			return
		}
		writeJSON(w, http.StatusOK, struct {
			StatusInfo
			Leadership db.Leadership `json:"leadership"`
		}{s.status, s.leadership.Status()})
	})

	mux.HandleFunc("/workers", s.handleWorkers)
	mux.HandleFunc("/api/workers", s.handleWorkers)

	mux.HandleFunc("/servers", s.handleServers)
	mux.HandleFunc("/servers/", s.handleServerByID)
	mux.HandleFunc("/deployments", s.handleDeployments)
	mux.HandleFunc("/deployments/", s.handleDeploymentByID)
	mux.HandleFunc("/executions/", s.handleExecutionByID)

	mux.HandleFunc("/connections", s.handleConnections)
	mux.HandleFunc("/connections/", s.handleConnectionByID)

	mux.HandleFunc("/jobs", s.handleJobs)
	mux.HandleFunc("/jobs/", s.handleJobByID)
	mux.HandleFunc("/api/jobs/", s.handleAPIJobByID)

	mux.HandleFunc("/runs", s.handleRuns)
	mux.HandleFunc("/runs/", s.handleRunByID)
	mux.HandleFunc("/api/source-engines", s.handleSourceEngines)
	mux.HandleFunc("/api/runs/submit", s.handleRunSubmit)
	mux.HandleFunc("/api/runs/validate", s.handleRunValidate)
	mux.HandleFunc("/api/maintenance/submit", s.handleMaintenanceSubmit)
	mux.HandleFunc("/api/runs", s.handleAPIRuns)
	mux.HandleFunc("/api/runs/", s.handleAPIRunByID)

	mux.HandleFunc("/sse", SSEHandler(s.log, s.st, s.bc))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeUnknownRoute(w, r.URL.Path)
	})

	handler := http.Handler(mux)
	if s.leadership != nil {
		handler = s.withLeadership(handler)
	}
	if strings.TrimSpace(s.authToken) == "" {
		return s.withRecoverer(handler)
	}
	return s.withRecoverer(s.withAuth(handler))
}

func (s *Server) withLeadership(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			if err := s.leadership.Assert(r.Context()); err != nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "master is not the active leader", "retryable": true})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// Serve starts the HTTP server on the specified address and listens for incoming requests.
// It also handles graceful shutdown when the context is canceled.
func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler()}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		_ = srv.Shutdown(context.Background())
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health probes stay unauthenticated.
		if r.URL.Path == "/healthz" || r.URL.Path == "/ready" || !isKnownAPIPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if !constantTimeBearerMatch(r.Header.Get("Authorization"), s.authToken) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="orabbit"`)
			writeUnauthorized(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func constantTimeBearerMatch(header, expectedToken string) bool {
	const prefix = "Bearer "
	header = strings.TrimSpace(header)
	validScheme := strings.HasPrefix(header, prefix)
	presented := header
	if validScheme {
		presented = strings.TrimPrefix(header, prefix)
	}
	presentedDigest := sha256.Sum256([]byte(presented))
	expectedDigest := sha256.Sum256([]byte(expectedToken))
	return validScheme && subtle.ConstantTimeCompare(presentedDigest[:], expectedDigest[:]) == 1
}

// handleWorkers handles the /workers endpoint, which returns a list of workers that have heartbeated recently.
// If the "all" query parameter is set to "1" or "true", it returns all workers regardless of their heartbeat status.
func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	q := r.URL.Query()
	all := q.Get("all") == "1" || strings.EqualFold(q.Get("all"), "true")
	if all {
		ws, err := s.st.ListWorkers(r.Context())
		if err != nil {
			writeInternalError(w, "failed to list workers")
			return
		}
		writeJSON(w, http.StatusOK, ws)
		return
	}

	// Default: show only workers that have heartbeated recently.
	cutoff := time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano)
	ws, err := s.st.ListWorkersActive(r.Context(), cutoff)
	if err != nil {
		writeInternalError(w, "failed to list workers")
		return
	}
	writeJSON(w, http.StatusOK, ws)
}

type readinessResponse struct {
	OK     bool              `json:"ok"`
	Checks map[string]string `json:"checks"`
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}

	resp := readinessResponse{
		OK:     true,
		Checks: map[string]string{"db": "ok"},
	}
	if s.st == nil {
		resp.OK = false
		resp.Checks["db"] = "error"
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}
	if err := s.st.Ready(r.Context()); err != nil {
		resp.OK = false
		resp.Checks["db"] = "error"
		if s.log != nil {
			s.log.Warn("http readiness check failed", slog.String("dependency", "db"), slog.String("err", err.Error()))
		}
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}
	if s.leadership != nil {
		state := s.leadership.Status()
		resp.Checks["leadership"] = state.State
		if !state.Ready || state.State != "LEADER" || s.leadership.Assert(r.Context()) != nil {
			resp.OK = false
			resp.Checks["leadership"] = "NOT_DURABLE_LEADER"
			writeJSON(w, http.StatusServiceUnavailable, resp)
			return
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- Helpers
// writeJSON writes the given value as a JSON response with the specified HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// readJSON reads the JSON request body and unmarshals it into the provided output variable.
func readJSON(r *http.Request, out any) error {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	return json.Unmarshal(b, out)
}

func readOptionalJSON(r *http.Request, out any) error {
	if r.Body == nil {
		return nil
	}
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	return json.Unmarshal(b, out)
}

// newID generates a new random ID as a hex string. It uses 16 random bytes, which results in a 32-character hex string.
func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// --- Connections
// connectionCreateRequest represents the request body for creating or updating a connection. It includes the name, kind, engine, metadata, and secret (which is encrypted at rest).
type connectionCreateRequest struct {
	Name     string          `json:"name"`
	Kind     string          `json:"kind"`
	Engine   string          `json:"engine"`
	Metadata json.RawMessage `json:"metadata"`
	Secret   json.RawMessage `json:"secret"` // encrypted at rest
}

// handleConnections handles the /connections endpoint, which supports listing all connections (GET) and creating a new connection (POST).
func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		cs, err := s.st.ListConnections(r.Context())
		if err != nil {
			writeInternalError(w, "failed to list connections")
			return
		}
		writeJSON(w, http.StatusOK, cs)

	case http.MethodPost:
		var req connectionCreateRequest
		if err := readJSON(r, &req); err != nil {
			writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
			return
		}
		if len(req.Secret) == 0 {
			writeInvalidInput(w, "missing secret", map[string]any{"field": "secret"})
			return
		}
		if s.k.IsZero() {
			writeMasterKeyRequired(w, "connection secrets")
			return
		}

		id := newID()
		blob, err := crypto.Encrypt(s.k, req.Secret, []byte(id))
		if err != nil {
			writeInternalError(w, "failed to create connection")
			return
		}
		c := db.Connection{
			ID:            id,
			Name:          req.Name,
			Kind:          req.Kind,
			Engine:        req.Engine,
			MetadataJSON:  req.Metadata,
			SecretEncBlob: blob,
		}
		audit, err := s.newAuditRecord(r, auditActionConnectionCreate, "connection", c.ID, nil)
		if err != nil {
			writeInternalError(w, "failed to create connection")
			return
		}
		created, err := s.st.CreateConnectionAudited(r.Context(), c, audit)
		if err != nil {
			writeInternalError(w, "failed to create connection")
			return
		}
		writeJSON(w, http.StatusCreated, created)

	default:
		writeMethodNotAllowed(w, r.Method, http.MethodGet, http.MethodPost)
	}
}

// handleConnectionByID handles the /connections/{id} endpoint, which supports fetching a connection by ID (GET), updating a connection (PUT), and deleting a connection (DELETE).
func (s *Server) handleConnectionByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/connections/")
	id = strings.Trim(id, "/")
	if id == "" {
		writeUnknownRoute(w, r.URL.Path)
		return
	}

	switch r.Method {
	case http.MethodGet:
		c, err := s.st.GetConnection(r.Context(), id)
		if err != nil {
			if handleLookupError(w, err, "connection") {
				return
			}
			writeInternalError(w, "failed to fetch connection")
			return
		}
		writeJSON(w, http.StatusOK, c)

	case http.MethodPut:
		cur, err := s.st.GetConnection(r.Context(), id)
		if err != nil {
			if handleLookupError(w, err, "connection") {
				return
			}
			writeInternalError(w, "failed to fetch connection")
			return
		}

		var req connectionCreateRequest
		if err := readJSON(r, &req); err != nil {
			writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
			return
		}
		if len(req.Secret) != 0 && s.k.IsZero() {
			writeMasterKeyRequired(w, "connection secrets")
			return
		}

		blob := cur.SecretEncBlob
		if len(req.Secret) != 0 {
			b, err := crypto.Encrypt(s.k, req.Secret, []byte(id))
			if err != nil {
				writeInternalError(w, "failed to update connection")
				return
			}
			blob = b
		}
		meta := cur.MetadataJSON
		if req.Metadata != nil {
			meta = req.Metadata
		}

		upd := db.Connection{ID: id, Name: req.Name, Kind: req.Kind, Engine: req.Engine, MetadataJSON: meta, SecretEncBlob: blob}
		audit, err := s.newAuditRecord(r, auditActionConnectionUpdate, "connection", id, nil)
		if err != nil {
			writeInternalError(w, "failed to update connection")
			return
		}
		updated, err := s.st.UpdateConnectionAudited(r.Context(), cur, upd, audit)
		if err != nil {
			writeInternalError(w, "failed to update connection")
			return
		}
		writeJSON(w, http.StatusOK, updated)

	case http.MethodDelete:
		cur, err := s.st.GetConnection(r.Context(), id)
		if err != nil {
			if handleLookupError(w, err, "connection") {
				return
			}
			writeInternalError(w, "failed to delete connection")
			return
		}
		n, err := s.st.CountJobsUsingConnection(r.Context(), id)
		if err != nil {
			writeInternalError(w, "failed to delete connection")
			return
		}
		if n > 0 {
			writeConflict(w, "", fmt.Sprintf("connection is referenced by %d job(s)", n), map[string]any{"job_count": n})
			return
		}
		audit, err := s.newAuditRecord(r, auditActionConnectionDelete, "connection", id, nil)
		if err != nil {
			writeInternalError(w, "failed to delete connection")
			return
		}
		if err := s.st.DeleteConnectionAudited(r.Context(), cur, audit); err != nil {
			if handleLookupError(w, err, "connection") {
				return
			}
			writeInternalError(w, "failed to delete connection")
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		writeMethodNotAllowed(w, r.Method, http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

// --- Jobs
// jobCreateRequest represents the request body for creating or updating a job. It includes the name, source and target connection IDs, source SQL, target namespace and table, write mode, incremental flag, high-water mark column, and additional options.
type jobCreateRequest struct {
	Name               string          `json:"name"`
	SourceConnectionID string          `json:"source_connection_id"`
	TargetConnectionID string          `json:"target_connection_id"`
	SourceSQL          string          `json:"source_sql"`
	TargetNamespace    string          `json:"target_namespace"`
	TargetTable        string          `json:"target_table"`
	WriteMode          string          `json:"write_mode"`
	Incremental        bool            `json:"incremental"`
	HWMColumn          *string         `json:"hwm_column"`
	OptionsJSON        json.RawMessage `json:"options_json"`
}

type runCreateRequest struct {
	RegistrationConfig json.RawMessage `json:"registration_config"`
}

// handleJobs handles the /jobs endpoint, which supports listing all jobs (GET) and creating a new job (POST).
func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		js, err := s.st.ListJobs(r.Context())
		if err != nil {
			writeInternalError(w, "failed to list jobs")
			return
		}
		writeJSON(w, http.StatusOK, js)

	case http.MethodPost:
		var req jobCreateRequest
		if err := readJSON(r, &req); err != nil {
			writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
			return
		}
		j := db.Job{
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
			OptionsJSON:        req.OptionsJSON,
		}
		audit, err := s.newAuditRecord(r, auditActionJobCreate, "job", j.ID, nil)
		if err != nil {
			writeInternalError(w, "failed to create job")
			return
		}
		created, err := s.st.CreateJobAudited(r.Context(), j, audit)
		if err != nil {
			writeInternalError(w, "failed to create job")
			return
		}
		writeJSON(w, http.StatusCreated, created)

	default:
		writeMethodNotAllowed(w, r.Method, http.MethodGet, http.MethodPost)
	}
}

// handleJobByID handles the /jobs/{id} endpoint, which supports fetching a job by ID (GET), updating a job (PUT), deleting a job (DELETE), and creating a new run for the job (POST /jobs/{id}/runs).
func (s *Server) handleJobByID(w http.ResponseWriter, r *http.Request) {
	jobID, isRunCreate, ok := parseJobRoute(r.URL.Path)
	if !ok {
		writeUnknownRoute(w, r.URL.Path)
		return
	}

	// POST /jobs/{id}/runs
	if isRunCreate {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, r.Method, http.MethodPost)
			return
		}
		var req runCreateRequest
		if err := readOptionalJSON(r, &req); err != nil {
			writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
			return
		}
		run, tasks, err := s.createRunForJobRequest(r, jobID, req)
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
		writeJSON(w, http.StatusCreated, map[string]any{"run": run, "tasks": tasks})
		return
	}

	switch r.Method {
	case http.MethodGet:
		j, err := s.st.GetJob(r.Context(), jobID)
		if err != nil {
			if handleLookupError(w, err, "job") {
				return
			}
			writeInternalError(w, "failed to fetch job")
			return
		}
		writeJSON(w, http.StatusOK, j)

	case http.MethodPut:
		cur, err := s.st.GetJob(r.Context(), jobID)
		if err != nil {
			if handleLookupError(w, err, "job") {
				return
			}
			writeInternalError(w, "failed to fetch job")
			return
		}
		var req jobCreateRequest
		if err := readJSON(r, &req); err != nil {
			writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
			return
		}
		upd := db.Job{
			ID:                 jobID,
			Name:               req.Name,
			SourceConnectionID: req.SourceConnectionID,
			TargetConnectionID: req.TargetConnectionID,
			SourceSQL:          req.SourceSQL,
			TargetNamespace:    req.TargetNamespace,
			TargetTable:        req.TargetTable,
			WriteMode:          req.WriteMode,
			Incremental:        req.Incremental,
			HWMColumn:          req.HWMColumn,
			OptionsJSON:        req.OptionsJSON,
			CreatedAt:          cur.CreatedAt,
		}
		audit, err := s.newAuditRecord(r, auditActionJobUpdate, "job", jobID, nil)
		if err != nil {
			writeInternalError(w, "failed to update job")
			return
		}
		updated, err := s.st.UpdateJobAudited(r.Context(), cur, upd, audit)
		if err != nil {
			writeInternalError(w, "failed to update job")
			return
		}
		writeJSON(w, http.StatusOK, updated)

	case http.MethodDelete:
		cur, err := s.st.GetJob(r.Context(), jobID)
		if err != nil {
			if handleLookupError(w, err, "job") {
				return
			}
			writeInternalError(w, "failed to delete job")
			return
		}
		totalRuns, activeRuns, err := s.st.CountRunsForJob(r.Context(), jobID)
		if err != nil {
			writeInternalError(w, "failed to delete job")
			return
		}
		if activeRuns > 0 {
			writeConflict(w, "", fmt.Sprintf("job has %d active run(s); stop them first", activeRuns), map[string]any{"active_runs": activeRuns})
			return
		}
		if totalRuns > 0 {
			writeConflict(w, "", fmt.Sprintf("job has %d historical run(s); deletion is blocked to preserve run history", totalRuns), map[string]any{"historical_runs": totalRuns})
			return
		}
		audit, err := s.newAuditRecord(r, auditActionJobDelete, "job", jobID, nil)
		if err != nil {
			writeInternalError(w, "failed to delete job")
			return
		}
		if err := s.st.DeleteJobAudited(r.Context(), cur, audit); err != nil {
			if handleLookupError(w, err, "job") {
				return
			}
			writeInternalError(w, "failed to delete job")
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		writeMethodNotAllowed(w, r.Method, http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

// --- Runs
// handleRuns handles the /runs endpoint, which returns a list of all runs.
func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	rs, err := s.st.ListRuns(r.Context())
	if err != nil {
		writeInternalError(w, "failed to list runs")
		return
	}
	writeJSON(w, http.StatusOK, rs)
}

// handleRunByID handles /runs/{id} lookups and /runs/{id}/cancel requests.
func (s *Server) handleRunByID(w http.ResponseWriter, r *http.Request) {
	raw := strings.TrimPrefix(r.URL.Path, "/runs/")
	raw = strings.Trim(raw, "/")
	if raw == "" {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	parts := strings.Split(raw, "/")
	runID := strings.TrimSpace(parts[0])
	if runID == "" {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" {
		s.handleRunCancel(w, r, runID)
		return
	}
	if len(parts) == 3 && parts[1] == "registration" && parts[2] == "cancel" {
		s.handleRegistrationCancel(w, r, runID)
		return
	}
	if len(parts) == 3 && parts[1] == "registration" && parts[2] == "retry" {
		s.handleRegistrationRetry(w, r, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "progress" {
		s.handleRunProgress(w, r, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "events" {
		s.handleRunEvents(w, r, runID)
		return
	}
	if len(parts) == 3 && parts[1] == "events" && parts[2] == "stream" {
		s.handleRunEventsStream(w, r, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "artifacts" {
		s.handleRunArtifacts(w, r, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "diagnosis" {
		s.handleRunDiagnosis(w, r, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "recover" {
		s.handleRunRecovery(w, r, runID)
		return
	}
	if len(parts) != 1 {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	run, err := s.st.GetRun(r.Context(), runID)
	if err != nil {
		if handleLookupError(w, err, "run") {
			return
		}
		writeInternalError(w, "failed to fetch run")
		return
	}
	tasks, err := s.st.ListTasksForRun(r.Context(), runID)
	if err != nil {
		writeInternalError(w, "failed to fetch run tasks")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": run, "tasks": tasks})
}

func (s *Server) handleRegistrationRetry(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	var req struct {
		OverrideConfig bool   `json:"override_config"`
		ConfigYAML     string `json:"config_yaml"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeInvalidInput(w, "invalid retry request", nil)
		return
	}
	var override json.RawMessage
	if req.OverrideConfig {
		yamlText := strings.TrimSpace(req.ConfigYAML)
		if yamlText == "" {
			writeInvalidInput(w, "config_yaml is required when override_config is true", nil)
			return
		}
		ice, err := icebergreg.ParseIceYAMLBytes([]byte(yamlText))
		if err != nil || strings.TrimSpace(ice.URI) == "" {
			writeInvalidInput(w, "invalid iceberg configuration", nil)
			return
		}
		run, err := s.st.GetRun(r.Context(), runID)
		if err != nil {
			if handleLookupError(w, err, "run") {
				return
			}
			writeInternalError(w, "failed to fetch run")
			return
		}
		base, err := icebergreg.ParseRunConfig(run.RegistrationConfigJSON)
		if err != nil || !base.Enabled || (base.Engine != "rest-go" && base.Engine != "ice") {
			writeInvalidInput(w, "unsupported persisted registration configuration", nil)
			return
		}
		base.URI, base.BearerToken = ice.URI, ice.BearerToken
		base.S3.Endpoint, base.S3.Region, base.S3.AccessKeyID, base.S3.SecretAccessKey = ice.S3.Endpoint, ice.S3.Region, ice.S3.AccessKeyID, ice.S3.SecretAccessKey
		if ice.S3.PathStyleAccess != nil {
			base.S3.PathStyleAccess = *ice.S3.PathStyleAccess
		}
		if base.Engine == "ice" {
			base.ConfigYAML = req.ConfigYAML
		}
		override, err = icebergreg.MarshalRunConfig(base)
		if err != nil {
			writeInvalidInput(w, "invalid iceberg configuration", nil)
			return
		}
	}
	reg, queued, err := s.st.RequeueRegistrationManual(r.Context(), runID, override, time.Now())
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeNotFound(w, "registration", nil)
			return
		}
		writeConflict(w, "", err.Error(), map[string]any{"run_id": runID})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"run_id": runID, "registration_id": reg.ID, "catalog_status": reg.Status, "queued": queued})
}

func (s *Server) handleRegistrationCancel(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	reg, err := s.st.GetRegistrationForRun(r.Context(), runID)
	if err != nil {
		if handleLookupError(w, err, "registration") {
			return
		}
		writeInternalError(w, "failed to fetch registration")
		return
	}
	result, err := s.st.CancelRegistration(r.Context(), reg.ID, time.Now())
	if err != nil {
		if errors.Is(err, db.ErrRegistrationCancelTooLate) {
			writeConflict(w, "", "registration cancellation is too late", map[string]any{"run_id": runID, "registration_id": reg.ID, "catalog_status": result.Status})
			return
		}
		writeInternalError(w, "failed to cancel registration")
		return
	}
	if result.Status == db.RegistrationReconciling {
		writeConflict(w, "", "catalog outcome may be ambiguous; reconciliation is required", map[string]any{"run_id": runID, "registration_id": reg.ID, "catalog_status": result.Status})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run_id": runID, "registration_id": reg.ID, "catalog_status": result.Status, "changed": result.Changed})
}

func (s *Server) handleRunCancel(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}

	changed, status, pendingTasksCanceled, err := s.st.CancelRun(r.Context(), runID, "canceled by client")
	if err != nil {
		if handleLookupError(w, err, "run") {
			return
		}
		writeInternalError(w, "failed to cancel run")
		return
	}

	switch status {
	case "SUCCEEDED", "FAILED", "COMMITTING":
		writeConflict(w, "", fmt.Sprintf("run is already %s", strings.ToLower(status)), map[string]any{"run_id": runID, "status": status})
		return
	}

	run, err := s.st.GetRun(r.Context(), runID)
	if err != nil {
		if handleLookupError(w, err, "run") {
			return
		}
		writeInternalError(w, "failed to fetch run")
		return
	}

	if changed {
		fields, _ := json.Marshal(map[string]any{
			"pending_tasks_canceled": pendingTasksCanceled,
			"reason":                 "canceled by client",
		})
		ev := db.Event{
			ID:         newID(),
			RunID:      runID,
			TS:         time.Now().UTC().Format(time.RFC3339Nano),
			Level:      "INFO",
			Message:    "run CANCELED",
			FieldsJSON: fields,
		}
		_ = s.st.InsertEvent(r.Context(), ev)
		if s.bc != nil {
			s.bc.Publish(ev)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"run":                    run,
		"canceled":               changed,
		"pending_tasks_canceled": pendingTasksCanceled,
	})
}

// --- Logging helpers
func (s *Server) LogError(msg string, err error) {
	s.log.Error(msg, slog.String("err", err.Error()))
}

// Debugf logs a debug message with formatting.
func (s *Server) Debugf(format string, args ...any) {
	s.log.Debug(fmt.Sprintf(format, args...))
}
