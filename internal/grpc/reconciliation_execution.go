package grpcapi

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	"github.com/LevonGhukas/O_Rabbit/internal/telemetry"
)

const (
	// reconciliationObservationBackoff delays the next observation after one
	// that proved nothing.
	reconciliationObservationBackoff = time.Second
	// reconciliationMaxAmbiguities bounds absence-proof cycles before a
	// registration is quarantined.
	reconciliationMaxAmbiguities = 2
)

type catalogInspector interface {
	InspectCatalog(context.Context, icebergreg.InspectionRequest) (icebergreg.CatalogObservation, error)
}

// catalogObservation is what one bounded reconciliation attempt saw.
type catalogObservation struct {
	obs      icebergreg.CatalogObservation
	expected []icebergreg.ExpectedFile
	err      error
	// interrupted is set when the attempt context ended for any reason
	// (deadline, lost lease, leadership); timedOut only for its own deadline.
	interrupted bool
	timedOut    bool
}

// ProcessReconciliationOnce claims at most one registration awaiting catalog
// reconciliation, observes the catalog within ReconciliationTimeout and
// records the outcome.
func (s *Server) ProcessReconciliationOnce(ctx context.Context) (bool, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return false, err
	}
	release, admitted := s.tryAcquireCatalogWork()
	if !admitted {
		return false, nil
	}
	defer release()
	r, a, ok, err := s.st.ClaimReconciliation(ctx, s.nowFn(), s.commitCatalog.ReconciliationLease)
	if err != nil || !ok {
		return ok, err
	}
	s.log.Info(
		"catalog reconciliation claimed",
		slog.String("run_id", r.RunID),
		slog.String("registration_id", r.ID),
		slog.Int("reconciliation_attempt", a.AttemptNumber),
		slog.String("commit_id", r.CommitID),
		slog.String("catalog_status", r.Status),
		slog.String("error_class", r.LastErrorClass),
		slog.String("executor", "master-owned-reconciliation-loop"),
	)
	inspector, ok := s.icebergRegistrar.(catalogInspector)
	if !ok {
		return true, s.st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, icebergreg.OutcomeInsufficientEvidence, "", "", "", "", "", 0, 0, s.nowFn(), reconciliationMaxAmbiguities)
	}
	observation, err := s.observeCatalog(ctx, r, a, inspector)
	if err != nil {
		// Lease expiry recovers an attempt whose inputs could not be read.
		return true, err
	}
	persistCtx, cancel := registrationPersistContext(ctx)
	defer cancel()
	return true, s.recordReconciliation(persistCtx, r, a, observation)
}

// observeCatalog inspects the catalog under the attempt deadline while
// renewing the attempt lease. Renewal has stopped when it returns.
func (s *Server) observeCatalog(ctx context.Context, r db.Registration, a db.ReconciliationAttempt, inspector catalogInspector) (catalogObservation, error) {
	attemptCtx, cancel := catalogAttemptContext(ctx, s.commitCatalog.ReconciliationTimeout)
	defer cancel()
	stopRenewal := s.renewReconciliationLease(attemptCtx, cancel, r, a)
	defer stopRenewal()

	req, expected, err := s.reconciliationInspection(attemptCtx, r)
	if err != nil {
		if catalogAttemptTimedOut(attemptCtx) {
			return catalogObservation{err: err, interrupted: true, timedOut: true}, nil
		}
		return catalogObservation{}, err
	}
	obs, inspectErr := inspector.InspectCatalog(attemptCtx, req)
	return catalogObservation{
		obs:         obs,
		expected:    expected,
		err:         inspectErr,
		interrupted: attemptCtx.Err() != nil,
		timedOut:    catalogAttemptTimedOut(attemptCtx),
	}, nil
}

// renewReconciliationLease keeps the attempt lease alive until attemptCtx
// ends. A failed renewal cancels the attempt. The returned func cancels the
// attempt and waits for renewal to stop.
func (s *Server) renewReconciliationLease(attemptCtx context.Context, cancel context.CancelFunc, r db.Registration, a db.ReconciliationAttempt) func() {
	lease := s.commitCatalog.ReconciliationLease
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer RecoverPanic(s.log, "reconciliation lease renewal")
		ticker := time.NewTicker(lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-attemptCtx.Done():
				return
			case <-ticker.C:
				if err := s.st.RenewReconciliationLease(attemptCtx, r.ID, a.ID, a.FencingToken, s.nowFn(), lease); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// reconciliationInspection builds the catalog inspection request and the
// files the registration is expected to have committed.
func (s *Server) reconciliationInspection(ctx context.Context, r db.Registration) (icebergreg.InspectionRequest, []icebergreg.ExpectedFile, error) {
	run, err := s.st.GetRun(ctx, r.RunID)
	if err != nil {
		return icebergreg.InspectionRequest{}, nil, err
	}
	resolved, err := s.resolveRun(ctx, run)
	if err != nil {
		return icebergreg.InspectionRequest{}, nil, err
	}
	reg, err := icebergreg.ParseRunConfig(run.RegistrationConfigJSON)
	if err != nil {
		return icebergreg.InspectionRequest{}, nil, err
	}
	artifacts, err := s.st.ListArtifactsForRun(ctx, r.RunID)
	if err != nil {
		return icebergreg.InspectionRequest{}, nil, err
	}
	bucket := resolved.target.Bucket
	expected := make([]icebergreg.ExpectedFile, 0, len(artifacts))
	for _, x := range artifacts {
		expected = append(expected, icebergreg.ExpectedFile{Path: "s3://" + bucket + "/" + x.ObjectKey, Size: x.ByteSize, Records: x.RowCount, SHA256: x.SHA256, SchemaFingerprint: x.SchemaFingerprint})
	}
	table := reg.Table
	if table == "" {
		table = icebergreg.DefaultTable(resolved.config.SourceEngine, strings.TrimSpace(resolved.opts.Table))
	}
	req := icebergreg.InspectionRequest{
		Registration:  reg,
		Table:         table,
		DatasetBucket: bucket,
		DatasetPrefix: resolved.prefix,
		DatasetS3:     s3io.Config{Endpoint: resolved.target.Endpoint, Region: resolved.target.Region, Bucket: bucket, ForcePathStyle: resolved.target.ForcePathStyle},
	}
	return req, expected, nil
}

// recordReconciliation persists the outcome of one observation.
func (s *Server) recordReconciliation(ctx context.Context, r db.Registration, a db.ReconciliationAttempt, o catalogObservation) error {
	if o.timedOut {
		return s.recordReconciliationTimeout(ctx, r, a, o.err)
	}
	op := icebergreg.OperationIdentity{RegistrationID: r.ID, RunID: r.RunID, CommitID: r.CommitID, ArtifactSetDigest: r.ArtifactSetDigest, ManifestKey: r.ManifestKey}
	if o.err != nil {
		// Evidence from an interrupted inspection is incomplete and must not
		// decide the outcome.
		if !o.interrupted && o.obs.TableExists && o.obs.MetadataStart != "" {
			if decision, err := icebergreg.DecideReconciliation(op, o.expected, o.obs); err == nil {
				return s.st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, decision.Outcome, decision.EvidenceDigest, o.obs.MetadataStart, o.obs.MetadataEnd, decision.SnapshotID, "", decision.MatchedFiles, decision.ExpectedFiles, s.nowFn(), reconciliationMaxAmbiguities)
			}
		}
		return s.retryReconciliation(ctx, r, a, "CATALOG_OBSERVATION_UNAVAILABLE", o.err.Error())
	}
	decision, err := icebergreg.DecideReconciliation(op, o.expected, o.obs)
	if err != nil {
		return s.retryReconciliation(ctx, r, a, "INSUFFICIENT_HISTORY", err.Error())
	}
	receipt := ""
	if decision.Outcome == icebergreg.OutcomeExactlyCommitted {
		receipt, err = db.ReconciliationReceipt(r, decision, s.nowFn())
		if err != nil {
			return err
		}
	}
	s.log.Info(
		"catalog reconciliation decision",
		slog.String("run_id", r.RunID),
		slog.String("registration_id", r.ID),
		slog.Int("reconciliation_attempt", a.AttemptNumber),
		slog.String("commit_id", r.CommitID),
		slog.String("catalog_status", r.Status),
		slog.String("decision", decision.Outcome),
		slog.Bool("operator_action_required", decision.OperatorActionRequired),
	)
	return s.st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, decision.Outcome, decision.EvidenceDigest, o.obs.MetadataStart, o.obs.MetadataEnd, decision.SnapshotID, receipt, decision.MatchedFiles, decision.ExpectedFiles, s.nowFn(), reconciliationMaxAmbiguities)
}

// recordReconciliationTimeout schedules another observation. Evidence read
// before the deadline is incomplete, so it never decides the outcome.
func (s *Server) recordReconciliationTimeout(ctx context.Context, r db.Registration, a db.ReconciliationAttempt, cause error) error {
	timeout := s.commitCatalog.ReconciliationTimeout
	telemetry.ObserveCatalogAttemptTimeout(catalogOperationReconciliation)
	message := fmt.Sprintf("catalog observation exceeded %s", timeout)
	if cause != nil {
		message = fmt.Sprintf("%s: %s", message, cause.Error())
	}
	s.log.Warn(
		"catalog reconciliation attempt timed out",
		slog.String("run_id", r.RunID),
		slog.String("registration_id", r.ID),
		slog.Int("reconciliation_attempt", a.AttemptNumber),
		slog.String("commit_id", r.CommitID),
		slog.String("error_class", reconciliationTimeoutClass),
		slog.Duration("timeout", timeout),
	)
	return s.retryReconciliation(ctx, r, a, reconciliationTimeoutClass, message)
}

func (s *Server) retryReconciliation(ctx context.Context, r db.Registration, a db.ReconciliationAttempt, class, message string) error {
	return s.st.RetryReconciliationObservation(ctx, r.ID, a.ID, a.FencingToken, class, message, s.nowFn(), reconciliationObservationBackoff, s.commitCatalog.ReconciliationMaxAttempts)
}
