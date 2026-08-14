package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func (s *Server) handleRunProgress(w http.ResponseWriter, r *http.Request, runID string) {
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
	cutoff := time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano)
	workers, err := s.st.ListWorkersActive(r.Context(), cutoff)
	if err != nil {
		writeInternalError(w, "failed to fetch worker status")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run_id":   run.ID,
		"status":   run.Status,
		"progress": summarizeRunProgress(tasks, workers),
	})
}

func (s *Server) handleRunEvents(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	items, nextCursor, err := s.st.ListEventsForRunPage(r.Context(), runID, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		writeInternalError(w, "failed to fetch run events")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":       items,
		"next_cursor": nextCursor,
	})
}

func (s *Server) handleRunEventsStream(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	q := r.URL.Query()
	q.Set("run_id", runID)
	r.URL.RawQuery = q.Encode()
	SSEHandler(s.log, s.st, s.bc).ServeHTTP(w, r)
}

func (s *Server) handleRunArtifacts(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	if _, err := s.st.GetRun(r.Context(), runID); err != nil {
		if handleLookupError(w, err, "run") {
			return
		}
		writeInternalError(w, "failed to fetch run")
		return
	}
	tasks, err := s.st.ListTasksForRun(r.Context(), runID)
	if err != nil {
		writeInternalError(w, "failed to fetch task artifacts")
		return
	}
	objects := make([]map[string]any, 0)
	for _, task := range tasks {
		if len(task.ParquetObjects) == 0 {
			continue
		}
		var entries []map[string]any
		if err := json.Unmarshal(task.ParquetObjects, &entries); err != nil {
			continue
		}
		for _, entry := range entries {
			item := map[string]any{
				"task_id": task.ID,
			}
			for key, value := range entry {
				item[key] = value
			}
			objects = append(objects, item)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run_id":  runID,
		"objects": objects,
	})
}

func summarizeRunProgress(tasks []db.Task, activeWorkers []db.Worker) map[string]any {
	summary := map[string]any{
		"tasks_total":     len(tasks),
		"tasks_pending":   0,
		"tasks_running":   0,
		"tasks_succeeded": 0,
		"tasks_failed":    0,
		"tasks_canceled":  0,
		"rows_read":       int64(0),
		"bytes_written":   int64(0),
		"workers_active":  0,
	}
	runningWorkers := make(map[string]struct{})
	for _, task := range tasks {
		summary["rows_read"] = summary["rows_read"].(int64) + task.RowsRead
		summary["bytes_written"] = summary["bytes_written"].(int64) + task.BytesWritten
		switch task.Status {
		case "PENDING":
			summary["tasks_pending"] = summary["tasks_pending"].(int) + 1
		case "RUNNING":
			summary["tasks_running"] = summary["tasks_running"].(int) + 1
			if task.WorkerID != nil && strings.TrimSpace(*task.WorkerID) != "" {
				runningWorkers[*task.WorkerID] = struct{}{}
			}
		case "SUCCEEDED":
			summary["tasks_succeeded"] = summary["tasks_succeeded"].(int) + 1
		case "FAILED":
			summary["tasks_failed"] = summary["tasks_failed"].(int) + 1
		case "CANCELED":
			summary["tasks_canceled"] = summary["tasks_canceled"].(int) + 1
		}
	}
	if len(runningWorkers) > 0 {
		summary["workers_active"] = len(runningWorkers)
	} else {
		summary["workers_active"] = len(activeWorkers)
	}
	return summary
}
