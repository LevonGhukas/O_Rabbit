package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

type catalogInspector interface {
	InspectCatalog(context.Context, icebergreg.InspectionRequest) (icebergreg.CatalogObservation, error)
}

func (s *Server) ProcessReconciliationOnce(ctx context.Context) (bool, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return false, err
	}
	release, admitted := s.tryAcquireCatalogWork()
	if !admitted {
		return false, nil
	}
	defer release()
	reconciliationLease := s.commitCatalog.ReconciliationLease
	r, a, ok, err := s.st.ClaimReconciliation(ctx, s.nowFn(), reconciliationLease)
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
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		defer RecoverPanic(s.log, "reconciliation lease renewal")
		ticker := time.NewTicker(reconciliationLease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-attemptCtx.Done():
				return
			case <-ticker.C:
				if err := s.st.RenewReconciliationLease(attemptCtx, r.ID, a.ID, a.FencingToken, s.nowFn(), reconciliationLease); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	inspector, ok := s.icebergRegistrar.(catalogInspector)
	if !ok {
		return true, s.st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, icebergreg.OutcomeInsufficientEvidence, "", "", "", "", "", 0, 0, s.nowFn(), 2)
	}
	run, err := s.st.GetRun(ctx, r.RunID)
	if err != nil {
		return true, err
	}
	resolved, err := s.resolveRun(ctx, run)
	if err != nil {
		return true, err
	}
	reg, err := icebergreg.ParseRunConfig(run.RegistrationConfigJSON)
	if err != nil {
		return true, err
	}
	bucket, opts, prefix := resolved.target.Bucket, resolved.opts, resolved.prefix
	artifacts, err := s.st.ListArtifactsForRun(ctx, r.RunID)
	if err != nil {
		return true, err
	}
	expected := make([]icebergreg.ExpectedFile, 0, len(artifacts))
	for _, x := range artifacts {
		expected = append(expected, icebergreg.ExpectedFile{Path: "s3://" + bucket + "/" + x.ObjectKey, Size: x.ByteSize, Records: x.RowCount, SHA256: x.SHA256, SchemaFingerprint: x.SchemaFingerprint})
	}
	table := reg.Table
	if table == "" {
		table = icebergreg.DefaultTable(resolved.config.SourceEngine, strings.TrimSpace(opts.Table))
	}
	obs, err := inspector.InspectCatalog(attemptCtx, icebergreg.InspectionRequest{Registration: reg, Table: table, DatasetBucket: bucket, DatasetPrefix: prefix, DatasetS3: s3io.Config{Endpoint: resolved.target.Endpoint, Region: resolved.target.Region, Bucket: bucket, ForcePathStyle: resolved.target.ForcePathStyle}})
	if err != nil {
		if obs.TableExists && obs.MetadataStart != "" {
			op := icebergreg.OperationIdentity{RegistrationID: r.ID, RunID: r.RunID, CommitID: r.CommitID, ArtifactSetDigest: r.ArtifactSetDigest, ManifestKey: r.ManifestKey}
			decision, decisionErr := icebergreg.DecideReconciliation(op, expected, obs)
			if decisionErr == nil {
				return true, s.st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, decision.Outcome, decision.EvidenceDigest, obs.MetadataStart, obs.MetadataEnd, decision.SnapshotID, "", decision.MatchedFiles, decision.ExpectedFiles, s.nowFn(), 2)
			}
		}
		return true, s.st.RetryReconciliationObservation(ctx, r.ID, a.ID, a.FencingToken, "CATALOG_OBSERVATION_UNAVAILABLE", err.Error(), s.nowFn(), time.Second, s.commitCatalog.ReconciliationMaxAttempts)
	}
	op := icebergreg.OperationIdentity{RegistrationID: r.ID, RunID: r.RunID, CommitID: r.CommitID, ArtifactSetDigest: r.ArtifactSetDigest, ManifestKey: r.ManifestKey}
	decision, err := icebergreg.DecideReconciliation(op, expected, obs)
	if err != nil {
		return true, s.st.RetryReconciliationObservation(ctx, r.ID, a.ID, a.FencingToken, "INSUFFICIENT_HISTORY", err.Error(), s.nowFn(), time.Second, s.commitCatalog.ReconciliationMaxAttempts)
	}
	receipt := ""
	if decision.Outcome == icebergreg.OutcomeExactlyCommitted {
		receipt, err = db.ReconciliationReceipt(r, decision, s.nowFn())
		if err != nil {
			return true, err
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
	return true, s.st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, decision.Outcome, decision.EvidenceDigest, obs.MetadataStart, obs.MetadataEnd, decision.SnapshotID, receipt, decision.MatchedFiles, decision.ExpectedFiles, s.nowFn(), 2)
}

func (s *Server) ProcessMultipartCleanupOnce(ctx context.Context) (bool, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return false, err
	}
	if _, err := s.st.ExpireMultipartCleanupClaims(ctx, s.nowFn()); err != nil {
		return false, err
	}
	record, ok, err := s.st.ClaimMultipartCleanup(ctx, s.nowFn(), s.multipartCleanupGrace, 30*time.Second)
	if err != nil || !ok {
		return ok, err
	}
	finish := func(outcome, class string, err error) (bool, error) {
		message := ""
		if err != nil {
			message = class
		}
		return true, s.st.FinishMultipartCleanup(ctx, record.ID, record.CleanupToken, outcome, class, message, s.nowFn(), s.multipartCleanupRetry, s.multipartCleanupMax)
	}
	run, err := s.st.GetRun(ctx, record.RunID)
	if err != nil {
		return finish("RETRY", "MULTIPART_TRACKING_FAILED", err)
	}
	resolved, err := s.resolveRun(ctx, run)
	if err != nil {
		return finish("RETRY", "MULTIPART_TRACKING_FAILED", err)
	}
	cfg, err := s.targetS3Config(ctx, resolved)
	if err != nil {
		return finish("RETRY", "MULTIPART_TRACKING_FAILED", err)
	}
	uploader, err := s.newMultipartCleanerFn(ctx, cfg)
	if err != nil {
		return finish("RETRY", "MULTIPART_TRACKING_FAILED", err)
	}
	expectedMeta := map[string]string{"run_id": record.RunID, "task_id": record.TaskID, "attempt_id": record.AttemptID, "file_index": fmt.Sprintf("%03d", record.FileIndex), "sha256": record.ObjectSHA256, "byte_size": fmt.Sprint(record.ObjectSize)}
	exists, verifyErr := uploader.VerifyTrackedFinalObject(ctx, record.ObjectKey, record.ObjectSize, record.ObjectSHA256, expectedMeta)
	if exists {
		if verifyErr != nil {
			return finish("REVIEW", "MULTIPART_FINAL_OBJECT_CONFLICT", verifyErr)
		}
		return finish("COMPLETED", "", nil)
	}
	if verifyErr != nil {
		return finish("RETRY", "MULTIPART_ABORT_AMBIGUOUS", verifyErr)
	}
	uploadID := record.ProviderUploadID
	if uploadID == "" {
		items, listErr := uploader.ListManagedMultipartUploads(ctx, record.ManagedPrefix, 100)
		if listErr != nil {
			return finish("RETRY", "MULTIPART_UNKNOWN_OWNERSHIP", listErr)
		}
		var matches []s3io.MultipartUploadInfo
		for _, item := range items {
			if item.Key == record.ObjectKey {
				matches = append(matches, item)
			}
		}
		switch len(matches) {
		case 0:
			return finish("ABORTED", "", nil)
		case 1:
			uploadID = matches[0].UploadID
			if err := s.st.AdoptMultipartUploadForCleanup(ctx, record.ID, record.CleanupToken, uploadID, s.nowFn()); err != nil {
				return true, err
			}
		default:
			return finish("REVIEW", "MULTIPART_DISCOVERY_CONFLICT", errors.New("multiple managed uploads match"))
		}
	}
	abortErr := uploader.AbortTrackedMultipart(ctx, record.ObjectKey, uploadID)
	if abortErr == nil {
		return finish("ABORTED", "", nil)
	}
	stillExists, inspectErr := uploader.MultipartUploadExists(ctx, record.ManagedPrefix, record.ObjectKey, uploadID)
	if inspectErr != nil {
		return finish("RETRY", "MULTIPART_ABORT_AMBIGUOUS", inspectErr)
	}
	if !stillExists {
		return finish("ABORTED", "", nil)
	}
	return finish("RETRY", "MULTIPART_ABORT_FAILED", abortErr)
}

func (s *Server) ProcessCanceledObjectCleanupOnce(ctx context.Context) (bool, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return false, err
	}
	if _, err := s.st.ExpireCanceledObjectCleanupAttempts(ctx, s.nowFn()); err != nil {
		return false, err
	}
	candidate, attempt, ok, err := s.st.ClaimCanceledObjectCleanup(ctx, s.nowFn(), 30*time.Second)
	if err != nil || !ok {
		return ok, err
	}
	finish := func(outcome, class string, dryRun bool) (bool, error) {
		return true, s.st.FinishCanceledObjectCleanup(ctx, candidate.ID, attempt.ID, attempt.FencingToken, outcome, class, s.nowFn(), s.canceledObjectRetry, s.canceledObjectMax, dryRun)
	}
	run, err := s.st.GetRun(ctx, candidate.RunID)
	if err != nil {
		return finish("FAILED", "CLEANUP_REFERENCE_AMBIGUOUS", false)
	}
	resolved, err := s.resolveRun(ctx, run)
	if err != nil {
		return finish("FAILED", "CLEANUP_REFERENCE_AMBIGUOUS", false)
	}
	cfg, err := s.targetS3Config(ctx, resolved)
	if err != nil {
		return finish("FAILED", "CLEANUP_REFERENCE_AMBIGUOUS", false)
	}
	cleaner, err := s.newCanceledObjectCleanerFn(ctx, cfg)
	if err != nil {
		return finish("FAILED", "CLEANUP_OBJECT_VERIFICATION_FAILED", false)
	}
	expectedMeta := map[string]string{"run_id": candidate.RunID, "task_id": candidate.TaskID, "attempt_id": candidate.AttemptID, "sha256": candidate.ExpectedSHA256, "byte_size": fmt.Sprint(candidate.ExpectedSize)}
	observation, observeErr := cleaner.ObserveExactObject(ctx, candidate.ObjectKey, candidate.ExpectedSize, candidate.ExpectedSHA256, expectedMeta)
	if observeErr != nil {
		if observation.Exists {
			return finish("CONFLICT", "CLEANUP_OBJECT_IDENTITY_CONFLICT", false)
		}
		return finish("FAILED", "CLEANUP_OBJECT_VERIFICATION_FAILED", false)
	}
	if !observation.Exists {
		return finish("MISSING", "", false)
	}
	if !observation.Matches {
		return finish("CONFLICT", "CLEANUP_OBJECT_IDENTITY_CONFLICT", false)
	}
	if err := s.st.AuthorizeCanceledObjectDelete(ctx, candidate.ID, attempt.ID, attempt.FencingToken, observation.Identity, observation.VersionID, s.nowFn()); err != nil {
		return true, err
	}
	if s.canceledObjectDryRun {
		return finish("DRY_RUN", "", true)
	}
	deleteErr := cleaner.DeleteExactObject(ctx, candidate.ObjectKey, observation.VersionID)
	after, inspectErr := cleaner.ObserveExactObject(ctx, candidate.ObjectKey, candidate.ExpectedSize, candidate.ExpectedSHA256, expectedMeta)
	if inspectErr == nil && !after.Exists {
		return finish("DELETED", "", false)
	}
	if inspectErr == nil && after.Exists && !after.Matches {
		return finish("CONFLICT", "CLEANUP_OBJECT_IDENTITY_CONFLICT", false)
	}
	if inspectErr != nil {
		return finish("AMBIGUOUS", "CLEANUP_DELETE_AMBIGUOUS", false)
	}
	if deleteErr != nil || after.Exists {
		return finish("FAILED", "CLEANUP_DELETE_FAILED", false)
	}
	return finish("FAILED", "CLEANUP_DELETE_FAILED", false)
}
