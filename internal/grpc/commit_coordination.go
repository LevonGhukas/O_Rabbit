package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"golang.org/x/sync/errgroup"
)

// ReconcileCommittingRuns resumes durable publication after a master restart.
// It is intentionally repeatable and never re-extracts source data.
func (s *Server) ReconcileCommittingRuns(ctx context.Context) error {
	if err := s.requireLeadership(ctx); err != nil {
		return err
	}
	ctx, cancelLeadership := s.leadershipContext(ctx)
	defer cancelLeadership()
	ids, err := s.st.ListCommittingRunIDsAt(ctx, s.nowFn())
	if err != nil {
		return err
	}

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(10) // Limit concurrency to avoid spikes

	for _, runID := range ids {
		id := runID
		g.Go(func() error {
			if err := s.finalizeRunCommit(groupCtx, id); err != nil {
				s.log.Error("commit reconciliation failed", slog.String("run_id", id), slog.String("err", err.Error()))
				return nil // Continue processing other runs
			}
			s.log.Info("commit reconciliation succeeded", slog.String("run_id", id))
			s.launchIcebergRegistration(id)
			return nil
		})
	}
	return g.Wait()
}

func (s *Server) finalizeRunCommit(ctx context.Context, runID string) error {
	if run, err := s.st.GetRun(ctx, runID); err == nil && run.Status == "SUCCEEDED" {
		return nil
	}
	if err := s.commitRunFn(ctx, runID); err != nil {
		class, retryable, operator, component := classifyCommitError(err)
		_ = s.st.RecordCommitReconciliationFailure(ctx, runID, class, err.Error(), retryable, operator, s.nowFn(), db.CommitReconciliationPolicy{
			MaxAttempts: 5,
			BackoffBase: time.Second,
			BackoffMax:  time.Minute,
		})
		s.log.Error(
			"commit reconciliation classified",
			slog.String("run_id", runID),
			slog.String("failure_class", class),
			slog.String("identity_component", component),
			slog.Bool("retryable", retryable),
			slog.Bool("operator_action_required", operator),
		)
		s.recordArtifactIntegrityFailure(ctx, runID, err)
		return err
	}
	if err := s.completeRunCommitFn(ctx, runID); err != nil {
		fields, _ := json.Marshal(map[string]any{
			"event_type": "RUN_COMPLETION_PENDING_RECOVERY",
			"error":      err.Error(),
		})
		_ = s.st.InsertEventOnce(ctx, db.Event{
			ID:         "commit-completion-pending-" + runID,
			RunID:      runID,
			TS:         time.Now().UTC().Format(time.RFC3339Nano),
			Level:      "WARN",
			Message:    "run completion pending recovery",
			FieldsJSON: fields,
		})
		return err
	}
	if s.bc != nil {
		run, err := s.st.GetRun(ctx, runID)
		if err == nil {
			fields, _ := json.Marshal(map[string]any{"commit_id": run.CommitID, "finalization_phase": "COMPLETE"})
			s.bc.Publish(db.Event{
				ID:         "commit-" + run.CommitID,
				RunID:      runID,
				TS:         time.Now().UTC().Format(time.RFC3339Nano),
				Level:      "INFO",
				Message:    "run committed",
				FieldsJSON: fields,
			})
		}
	}
	return nil
}

func (s *Server) recordArtifactIntegrityFailure(ctx context.Context, runID string, commitErr error) {
	message := commitErr.Error()
	if !strings.Contains(message, "artifact") && !strings.Contains(message, "required object") {
		return
	}
	classification := "ARTIFACT_INTEGRITY_FAILURE"
	switch {
	case strings.Contains(message, "missing") || strings.Contains(message, "required object"):
		classification = "ARTIFACT_MISSING"
	case strings.Contains(message, "size mismatch"):
		classification = "ARTIFACT_SIZE_MISMATCH"
	case strings.Contains(message, "sha256 mismatch"):
		classification = "ARTIFACT_DIGEST_MISMATCH"
	}
	digest := sha256.Sum256([]byte(runID + "\x00" + classification + "\x00" + message))
	eventFields := map[string]any{"event_type": "ARTIFACT_INTEGRITY_REJECTED", "classification": classification, "error": message}
	if records, err := s.st.ListArtifactsForRun(ctx, runID); err == nil {
		for _, record := range records {
			if strings.Contains(message, record.ObjectKey) {
				eventFields["task_id"] = record.TaskID
				eventFields["attempt_id"] = record.AttemptID
				eventFields["attempt_number"] = record.AttemptNumber
				eventFields["object_key"] = record.ObjectKey
				eventFields["expected_byte_size"] = record.ByteSize
				eventFields["expected_sha256"] = record.SHA256
				break
			}
		}
	}
	fields, _ := json.Marshal(eventFields)
	e := db.Event{ID: "artifact-integrity-" + hex.EncodeToString(digest[:]), RunID: runID, TS: s.nowFn().UTC().Format(time.RFC3339Nano), Level: "ERROR", Message: "artifact integrity verification failed", FieldsJSON: fields}
	_ = s.st.InsertEvent(ctx, e)
}
