package httpapi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

type recoveryRequest struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

func (s *Server) handleRunDiagnosis(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	diagnosis, err := s.st.DiagnoseRun(r.Context(), runID, time.Now(), s.taskMaxAttempts)
	if err != nil {
		if handleLookupError(w, err, "run") {
			return
		}
		writeInternalError(w, "failed to diagnose run")
		return
	}
	writeJSON(w, http.StatusOK, diagnosis)
}

func (s *Server) handleRunRecovery(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	var req recoveryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
		return
	}
	req.Action = strings.ToLower(strings.TrimSpace(req.Action))
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Action == "" || req.Reason == "" {
		writeInvalidInput(w, "action and reason are required", nil)
		return
	}
	audit, err := s.newAuditRecord(r, "", "run", runID, nil)
	if err != nil {
		writeInternalError(w, "failed to prepare recovery audit")
		return
	}

	if req.Action == "reconcile_commit" {
		run, getErr := s.st.GetRun(r.Context(), runID)
		if getErr != nil {
			if handleLookupError(w, getErr, "run") {
				return
			}
			writeInternalError(w, "failed to fetch run")
			return
		}
		if run.Status != "COMMITTING" {
			if recordErr := s.st.RecordRecoveryRequest(r.Context(), runID, req.Action, req.Reason, run.Status, audit); recordErr != nil {
				writeInternalError(w, "failed to audit recovery request")
				return
			}
			writeJSON(w, http.StatusOK, db.RecoveryResult{Action: req.Action, Changed: false, Status: run.Status, Message: "run is no longer COMMITTING"})
			return
		}
		if s.commitReconciler == nil {
			writeDependencyUnavailable(w, "commit reconciler is unavailable")
			return
		}
		if err := s.st.RecordRecoveryRequest(r.Context(), runID, req.Action, req.Reason, run.Status, audit); err != nil {
			writeInternalError(w, "failed to audit recovery request")
			return
		}
		if err := s.commitReconciler.ReconcileCommittingRuns(r.Context()); err != nil {
			writeDependencyUnavailable(w, "commit reconciliation failed")
			return
		}
		recovered, err := s.st.GetRun(r.Context(), runID)
		if err != nil {
			writeInternalError(w, "failed to verify recovery result")
			return
		}
		writeJSON(w, http.StatusOK, db.RecoveryResult{Action: req.Action, Changed: recovered.Status != run.Status || recovered.CommitPhase != run.CommitPhase, Status: recovered.Status, Message: "commit reconciliation scan completed"})
		return
	}

	result, err := s.st.RequestLifecycleRecovery(r.Context(), runID, req.Action, req.Reason, audit)
	if err != nil {
		switch {
		case errors.Is(err, db.ErrRecoveryRefused):
			writeConflict(w, "", err.Error(), map[string]any{"run_id": runID, "action": req.Action})
		case errors.Is(err, sql.ErrNoRows):
			writeNotFound(w, "run or recovery target", nil)
		default:
			writeInternalError(w, "failed to request recovery")
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	metrics, err := s.st.LifecycleMetrics(r.Context(), time.Now())
	if err != nil {
		writeInternalError(w, "failed to collect lifecycle metrics")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(renderLifecycleMetrics(metrics)))
}

func renderLifecycleMetrics(m db.LifecycleMetrics) string {
	var b strings.Builder
	writeMetricMap(&b, "orabbit_runs", "status", m.RunsByStatus, []string{"PLANNING", "RUNNING", "COMMITTING", "SUCCEEDED", "FAILED", "CANCELED"})
	writeMetricMap(&b, "orabbit_runs_commit_phase", "phase", m.RunsByCommitPhase, []string{"NONE", "PREPARING", "INTENT", "MANIFEST_VERIFIED", "STATE_VERIFIED", "VERIFIED", "RETRY_REQUIRED", "COMPLETE"})
	writeScalarMetric(&b, "orabbit_oldest_committing_run_age_seconds", m.OldestCommittingAgeSeconds)
	writeMetricMap(&b, "orabbit_tasks", "status", m.TasksByStatus, []string{"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED", "QUARANTINED"})
	writeScalarMetric(&b, "orabbit_task_leases_active", float64(m.LeasedTasks))
	writeScalarMetric(&b, "orabbit_task_leases_expired", float64(m.ExpiredActiveLeases))
	writeScalarMetric(&b, "orabbit_tasks_quarantined", float64(m.TasksByStatus["QUARANTINED"]))
	writeMetricMap(&b, "orabbit_registrations", "status", m.RegistrationsByStatus, []string{"PENDING", "REGISTERING", "RETRY_REQUIRED", "RECONCILING", "REGISTERED", "FAILED", "QUARANTINED", "CANCELED", "BLOCKED"})
	writeMetricMap(&b, "orabbit_registration_attempts", "phase", m.RegistrationAttemptsByPhase, []string{"PREPARED", "EXTERNAL_COMMIT_STARTED", "CATALOG_COMMITTED", "ICE_STATE_WRITING", "VERIFIED"})
	writeMetricMap(&b, "orabbit_reconciliations", "status", m.ReconciliationsByStatus, []string{"NONE", "PENDING", "INSPECTING", "RETRY_REQUIRED", "SUCCEEDED", "FAILED", "CANCELED"})
	writeMetricMap(&b, "orabbit_reconciliations_by_classification", "classification", m.ReconciliationsByClassification, []string{"NONE", "CATALOG_OBSERVATION_UNAVAILABLE", "CATALOG_HISTORY_INCOMPLETE", "RECONCILIATION_CANCELED", "RETRY_LIMIT_EXHAUSTED", "OTHER"})
	writeScalarMetric(&b, "orabbit_registrations_blocked", float64(m.RegistrationBlocked))
	writeScalarMetric(&b, "orabbit_registrations_retry_required", float64(m.RegistrationRetryRequired))
	writeLabeledMetric(&b, "orabbit_commit_reconciliation_runs", "outcome", "complete", float64(m.RunsByCommitPhase["COMPLETE"]))
	writeLabeledMetric(&b, "orabbit_commit_reconciliation_runs", "outcome", "retry_required", float64(m.RunsByCommitPhase["RETRY_REQUIRED"]))
	writeScalarMetric(&b, "orabbit_leadership_active", float64(m.LeadershipActive))
	writeScalarMetric(&b, "orabbit_leadership_epoch", float64(m.LeadershipEpoch))
	writeScalarMetric(&b, "orabbit_leadership_renewal_failures_total", float64(m.LeadershipRenewalFailures))
	return b.String()
}

func writeMetricMap(b *strings.Builder, name, labelName string, values map[string]int, allowed []string) {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, label := range allowed {
		allowedSet[label] = struct{}{}
	}
	bounded := map[string]int{}
	for label, value := range values {
		if _, ok := allowedSet[label]; !ok {
			if _, supportsOther := allowedSet["OTHER"]; !supportsOther {
				continue
			}
			label = "OTHER"
		}
		bounded[label] += value
	}
	labels := make([]string, 0, len(bounded))
	for label := range bounded {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		writeLabeledMetric(b, name, labelName, label, float64(bounded[label]))
	}
}

func writeLabeledMetric(b *strings.Builder, name, labelName, label string, value float64) {
	b.WriteString(name)
	b.WriteString(`{`)
	b.WriteString(labelName)
	b.WriteString(`="`)
	b.WriteString(label)
	b.WriteString(`"} `)
	b.WriteString(strconv.FormatFloat(value, 'f', -1, 64))
	b.WriteByte('\n')
}

func writeScalarMetric(b *strings.Builder, name string, value float64) {
	b.WriteString(name)
	b.WriteByte(' ')
	b.WriteString(strconv.FormatFloat(value, 'f', -1, 64))
	b.WriteByte('\n')
}

func metricHasForbiddenLabel(body string) error {
	for _, forbidden := range []string{"run_id=", "task_id=", "object_key=", "error_message=", "file_path=", "worker_id="} {
		if strings.Contains(body, forbidden) {
			return fmt.Errorf("forbidden metric label %s", forbidden)
		}
	}
	return nil
}
