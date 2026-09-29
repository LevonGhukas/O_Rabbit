package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"golang.org/x/sync/errgroup"
)

const (
	// commitClaimLease bounds how long a crashed committer blocks a run; a
	// live committer renews its claim every third of it.
	commitClaimLease = 2 * time.Minute
)

// ReconcileCommittingRuns publishes every eligible COMMITTING run. It is the
// retry path for commits started by ReportTaskResult and resumes publication
// after a master restart. It is repeatable and never re-extracts source data;
// the per-run commit claim keeps concurrent callers from duplicating work.
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
			s.commitClaimedRun(groupCtx, id)
			return nil // Continue processing other runs
		})
	}
	return g.Wait()
}

// launchCommit starts publishing a run that just became COMMITTING. It runs on
// the master's own context, not the reporting worker's RPC, so a worker
// timing out cannot cancel the commit.
func (s *Server) launchCommit(runID string) {
	go func() {
		defer RecoverPanic(s.log, "run commit")
		base := context.Background()
		if leader, ok := s.leadership.(interface{ WorkContext() context.Context }); ok && leader.WorkContext() != nil {
			base = leader.WorkContext()
		}
		s.commitClaimedRun(base, runID)
	}()
}

// commitClaimedRun publishes runID if it can claim it and reports whether the
// run committed. Only the claim holder commits; the claim is renewed while
// the commit runs and released afterwards.
func (s *Server) commitClaimedRun(ctx context.Context, runID string) bool {
	token := newID()
	claimed, err := s.st.ClaimCommittingRun(ctx, runID, token, s.nowFn(), commitClaimLease)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("claim committing run failed", slog.String("run_id", runID), slog.String("err", err.Error()))
		}
		return false
	}
	if !claimed {
		return false // another committer holds it, or it is not yet eligible
	}
	commitCtx, cancel := context.WithTimeout(ctx, s.commitCatalog.CommitTimeout)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		defer RecoverPanic(s.log, "commit claim renewal")
		ticker := time.NewTicker(commitClaimLease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-commitCtx.Done():
				return
			case <-ticker.C:
				if err := s.st.RenewCommitClaim(commitCtx, runID, token, s.nowFn(), commitClaimLease); err != nil {
					if commitCtx.Err() == nil {
						s.log.Warn("commit claim lost; abandoning commit", slog.String("run_id", runID), slog.String("err", err.Error()))
					}
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-renewDone
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer releaseCancel()
		if err := s.st.ReleaseCommitClaim(releaseCtx, runID, token); err != nil {
			s.log.Warn("release commit claim failed", slog.String("run_id", runID), slog.String("err", err.Error()))
		}
	}()

	if err := s.finalizeRunCommit(commitCtx, runID); err != nil {
		s.log.Error("run commit failed", slog.String("run_id", runID), slog.String("err", err.Error()))
		return false
	}
	s.log.Info("run commit succeeded", slog.String("run_id", runID))
	s.launchIcebergRegistration(runID)
	return true
}

func (s *Server) finalizeRunCommit(ctx context.Context, runID string) error {
	if run, err := s.st.GetRun(ctx, runID); err == nil && run.Status == "SUCCEEDED" {
		return nil
	}
	if err := s.commitRunFn(ctx, runID); err != nil {
		if ctx.Err() != nil {
			// Interrupted (shutdown, lost claim, or timeout), not a commit
			// failure: leave the run COMMITTING without consuming an attempt.
			return fmt.Errorf("commit interrupted: %w", err)
		}
		class, retryable, operator, component := classifyCommitError(err)
		_ = s.st.RecordCommitReconciliationFailure(ctx, runID, class, err.Error(), retryable, operator, s.nowFn(), db.CommitReconciliationPolicy{
			MaxAttempts: s.commitCatalog.CommitMaxAttempts,
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
		if run, getErr := s.st.GetRun(ctx, runID); getErr == nil && run.Status == "FAILED" {
			// Terminal: tell run watchers, which stop on "run FAILED".
			e := db.Event{ID: "commit-failed-" + runID, RunID: runID, TS: db.FormatTimestamp(s.nowFn()), Level: "ERROR", Message: "run FAILED", FieldsJSON: []byte(`{"event_type":"RUN_COMMIT_FAILED"}`)}
			if s.st.InsertEventOnce(ctx, e) == nil && s.bc != nil {
				s.bc.Publish(e)
			}
		}
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
			TS:         db.FormatTimestamp(time.Now()),
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
				TS:         db.FormatTimestamp(time.Now()),
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
	e := db.Event{ID: "artifact-integrity-" + hex.EncodeToString(digest[:]), RunID: runID, TS: db.FormatTimestamp(s.nowFn()), Level: "ERROR", Message: "artifact integrity verification failed", FieldsJSON: fields}
	_ = s.st.InsertEvent(ctx, e)
}
