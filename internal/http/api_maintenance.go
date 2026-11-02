package httpapi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
)

// minOrphanMinAgeHours mirrors the master's lower bound on the orphan grace
// period, so a bad request fails here instead of in the background.
const minOrphanMinAgeHours = 1.0

type maintenanceSubmitRequest struct {
	JobID string `json:"job_id"`
	// Operation is orphan_cleanup or compaction; vacuum and compact are
	// accepted as their older names.
	Operation string `json:"operation"`
	// DryRun defaults to true: a cleanup only reports, a compaction only
	// plans, until a request sets it to false.
	DryRun        *bool                      `json:"dry_run,omitempty"`
	OrphanCleanup *maintenanceOrphanSettings `json:"orphan_cleanup,omitempty"`
	Compaction    *maintenanceCompactionOpts `json:"compaction,omitempty"`
}

type maintenanceOrphanSettings struct {
	MinAgeHours float64 `json:"min_age_hours,omitempty"`
}

type maintenanceCompactionOpts struct {
	TargetFileBytes int64 `json:"target_file_bytes,omitempty"`
	SmallFileBytes  int64 `json:"small_file_bytes,omitempty"`
	MinInputFiles   int   `json:"min_input_files,omitempty"`
	MaxInputFiles   int   `json:"max_input_files,omitempty"`
}

func maintenanceKind(operation string) string {
	switch strings.ToLower(strings.TrimSpace(operation)) {
	case "orphan_cleanup", "vacuum":
		return db.MaintenanceOrphanCleanup
	case "compaction", "compact":
		return db.MaintenanceCompaction
	}
	return ""
}

// handleMaintenanceSubmit creates an orphan-cleanup or compaction operation
// for a job's Iceberg table. The leader runs it in the background; the
// response names the operation to poll at GET /maintenance/{id}.
func (s *Server) handleMaintenanceSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	var req maintenanceSubmitRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeInvalidInput(w, "Invalid JSON payload", invalidJSONDetails(err))
		return
	}
	kind := maintenanceKind(req.Operation)
	if kind == "" {
		writeInvalidInput(w, "operation must be orphan_cleanup or compaction", map[string]any{"field": "operation"})
		return
	}
	req.JobID = strings.TrimSpace(req.JobID)
	if req.JobID == "" {
		writeInvalidInput(w, "job_id is required", map[string]any{"field": "job_id"})
		return
	}
	var params any = map[string]any{}
	switch kind {
	case db.MaintenanceOrphanCleanup:
		if req.Compaction != nil {
			writeInvalidInput(w, "compaction settings do not apply to orphan_cleanup", map[string]any{"field": "compaction"})
			return
		}
		if req.OrphanCleanup != nil {
			if h := req.OrphanCleanup.MinAgeHours; h != 0 && h < minOrphanMinAgeHours {
				writeInvalidInput(w, "orphan_cleanup.min_age_hours must be at least 1", map[string]any{"field": "orphan_cleanup.min_age_hours"})
				return
			}
			params = req.OrphanCleanup
		}
	case db.MaintenanceCompaction:
		if req.OrphanCleanup != nil {
			writeInvalidInput(w, "orphan_cleanup settings do not apply to compaction", map[string]any{"field": "orphan_cleanup"})
			return
		}
		if c := req.Compaction; c != nil {
			if c.TargetFileBytes < 0 || c.SmallFileBytes < 0 || c.MinInputFiles < 0 || c.MaxInputFiles < 0 {
				writeInvalidInput(w, "compaction settings must be zero (default) or positive", map[string]any{"field": "compaction"})
				return
			}
			if c.TargetFileBytes > 0 && c.SmallFileBytes > c.TargetFileBytes {
				writeInvalidInput(w, "compaction.small_file_bytes must not exceed target_file_bytes", map[string]any{"field": "compaction.small_file_bytes"})
				return
			}
			params = c
		}
	}
	if s.st == nil {
		writeDependencyUnavailable(w, "store unavailable")
		return
	}
	if _, err := s.st.GetJob(r.Context(), req.JobID); err != nil {
		if !handleLookupError(w, err, "job") {
			writeInternalError(w, err.Error())
		}
		return
	}
	raw, err := json.Marshal(params)
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}
	op := db.MaintenanceOperation{ID: newID(), Kind: kind, JobID: req.JobID, DryRun: req.DryRun == nil || *req.DryRun, Params: raw}
	if err := s.st.CreateMaintenanceOperation(r.Context(), op, time.Now()); err != nil {
		if errors.Is(err, db.ErrMaintenanceBusy) {
			writeConflict(w, httperr.CodeConflict, "job already has an active maintenance operation", map[string]any{"job_id": req.JobID})
			return
		}
		writeInternalError(w, err.Error())
		return
	}
	created, err := s.st.GetMaintenanceOperation(r.Context(), op.ID)
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"operation": created})
}

// handleMaintenance lists operations, newest first: GET /maintenance?job_id=.
func (s *Server) handleMaintenance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	limit := 100
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeInvalidInput(w, "limit must be between 1 and 1000", map[string]any{"field": "limit"})
			return
		}
		limit = n
	}
	ops, err := s.st.ListMaintenanceOperations(r.Context(), r.URL.Query().Get("job_id"), limit)
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}
	if ops == nil {
		ops = []db.MaintenanceOperation{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"operations": ops})
}

// handleMaintenanceByID returns one operation with its per-object report:
// GET /maintenance/{id}.
func (s *Server) handleMaintenanceByID(w http.ResponseWriter, r *http.Request) {
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/maintenance/"), "/")
	if id == "" || strings.Contains(id, "/") {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	op, err := s.st.GetMaintenanceOperation(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeNotFound(w, "maintenance operation", map[string]any{"id": id})
		return
	}
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}
	candidates, err := s.st.ListMaintenanceCandidates(r.Context(), id)
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}
	if candidates == nil {
		candidates = []db.MaintenanceCandidate{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": op, "candidates": candidates})
}
