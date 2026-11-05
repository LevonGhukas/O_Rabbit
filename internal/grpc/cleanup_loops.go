package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

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
