package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

func (s *Server) launchIcebergRegistration(runID string) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return
	}
	go func() {
		base := context.Background()
		if leader, ok := s.leadership.(interface{ WorkContext() context.Context }); ok && leader.WorkContext() != nil {
			base = leader.WorkContext()
		}
		ctx, cancel := context.WithTimeout(base, 30*time.Minute)
		defer cancel()
		if _, err := s.ProcessRegistrationOnce(ctx); err != nil {
			if ctx.Err() == nil {
				s.log.Error("durable iceberg registration worker failed", slog.String("run_id", runID), slog.String("err", err.Error()))
			}
		}
	}()
}

func (s *Server) runIcebergRegistration(ctx context.Context, runID string) (bool, icebergreg.RunResult, error) {
	return s.runIcebergRegistrationWithHooks(ctx, runID, "", "", false, "", nil, nil, nil, nil, nil)
}

func (s *Server) runIcebergRegistrationWithHooks(ctx context.Context, runID, registrationID, artifactDigest string, alreadyCommitted bool, receipt string, receiptFactory func() (string, error), before func() error, committed, noOp func(string) error, iceStateWriting func() error) (bool, icebergreg.RunResult, error) {
	run, err := s.st.GetRun(ctx, runID)
	if err != nil {
		return false, icebergreg.RunResult{}, err
	}
	job, err := s.st.GetJob(ctx, run.JobID)
	if err != nil {
		return false, icebergreg.RunResult{}, err
	}

	regCfg, err := icebergreg.ParseRunConfig(run.RegistrationConfigJSON)
	if err != nil {
		return false, icebergreg.RunResult{}, err
	}
	// A manual retry may carry a durable, parsed override.  It is selected here,
	// inside the existing claimed/leased execution flow, never by the HTTP API.
	if registrationID != "" {
		reg, regErr := s.st.GetRegistrationForRun(ctx, runID)
		if regErr != nil {
			return false, icebergreg.RunResult{}, regErr
		}
		if len(reg.RetryOverrideConfigJSON) != 0 {
			regCfg, err = icebergreg.ParseRunConfig(reg.RetryOverrideConfigJSON)
			if err != nil {
				return false, icebergreg.RunResult{}, err
			}
		}
	}
	jobCfg, err := icebergreg.ParseJobConfig(job.OptionsJSON)
	if err != nil {
		return false, icebergreg.RunResult{}, err
	}
	if !regCfg.Enabled {
		if len(run.RegistrationConfigJSON) == 0 && jobCfg.Enabled {
			return true, icebergreg.RunResult{}, fmt.Errorf("missing persisted run registration config snapshot; resubmit the run after the master upgrade")
		}
		return false, icebergreg.RunResult{}, nil
	}
	if s.icebergRegistrar == nil {
		return true, icebergreg.RunResult{}, fmt.Errorf("master iceberg registrar is not configured")
	}

	srcConn, err := s.st.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		return false, icebergreg.RunResult{}, err
	}
	tgtConn, err := s.st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return false, icebergreg.RunResult{}, err
	}

	srcSecret, err := crypto.Decrypt(s.k, srcConn.SecretEncBlob, []byte(srcConn.ID))
	if err != nil {
		return false, icebergreg.RunResult{}, err
	}
	var src map[string]any
	_ = json.Unmarshal(srcSecret, &src)
	sourceDSN, _ := src["dsn"].(string)

	tgtSecret, err := crypto.Decrypt(s.k, tgtConn.SecretEncBlob, []byte(tgtConn.ID))
	if err != nil {
		return false, icebergreg.RunResult{}, err
	}
	var tgt map[string]any
	_ = json.Unmarshal(tgtSecret, &tgt)
	accessKey, _ := tgt["access_key_id"].(string)
	secretKey, _ := tgt["secret_access_key"].(string)
	sessionToken, _ := tgt["session_token"].(string)

	var tgtMeta map[string]any
	_ = json.Unmarshal(tgtConn.MetadataJSON, &tgtMeta)
	endpoint, _ := tgtMeta["endpoint"].(string)
	region, _ := tgtMeta["region"].(string)
	bucket, _ := tgtMeta["bucket"].(string)
	forcePathStyle := true
	if v, ok := tgtMeta["force_path_style"].(bool); ok {
		forcePathStyle = v
	}
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	if region == "" {
		region = "us-east-1"
	}
	if strings.TrimSpace(bucket) == "" {
		return true, icebergreg.RunResult{}, fmt.Errorf("target connection metadata missing bucket")
	}

	opts, _ := jobopts.Parse(job.OptionsJSON)
	sourceQuery := strings.TrimSpace(opts.Query)
	if opts.NormalizedSourceMode() == "query" && sourceQuery == "" {
		sourceQuery = strings.TrimSpace(job.SourceSQL)
	}
	datasetPrefix := datasetPrefixForJob(job, srcConn.Engine, opts, tgtMeta)
	var intent durableCommitIntent
	var exactArtifacts []artifact.Record
	if run.CommitID != "" {
		exactArtifacts, err = s.st.ListArtifactsForRun(ctx, runID)
		if err != nil {
			return true, icebergreg.RunResult{}, err
		}
		tasks, taskErr := s.st.ListTasksForRun(ctx, runID)
		if taskErr != nil {
			return true, icebergreg.RunResult{}, taskErr
		}
		currentDestination := durableCommitDestination{Endpoint: endpoint, Region: region, Bucket: bucket, Prefix: datasetPrefix, ForcePathStyle: forcePathStyle}
		intent, currentDestination, err = validatePersistedCommitIntent(run, run.CommitIntentJSON, currentDestination, collectParquetKeys(tasks), exactArtifacts)
		if err != nil {
			return true, icebergreg.RunResult{}, err
		}
		endpoint = currentDestination.Endpoint
		region = currentDestination.Region
		bucket = currentDestination.Bucket
		forcePathStyle = currentDestination.ForcePathStyle
		datasetPrefix = currentDestination.Prefix
	}
	req := icebergreg.RunRequest{
		RunID:         runID,
		Registration:  regCfg,
		SourceEngine:  srcConn.Engine,
		SourceDSN:     sourceDSN,
		SourceMode:    opts.NormalizedSourceMode(),
		SourceTable:   strings.TrimSpace(opts.Table),
		SourceQuery:   sourceQuery,
		QueryHash:     strings.TrimSpace(opts.QueryHash),
		Incremental:   job.Incremental,
		WriteMode:     job.WriteMode,
		DatasetPrefix: datasetPrefix,
		DatasetS3: s3io.Config{
			Endpoint:        endpoint,
			Region:          region,
			Bucket:          bucket,
			ForcePathStyle:  forcePathStyle,
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			SessionToken:    sessionToken,
		},
		CommitID:                 run.CommitID,
		RegistrationID:           registrationID,
		ArtifactSetDigest:        artifactDigest,
		ExactArtifactSetVerified: run.CommitID != "",
		DurableIcebergSchema:     intent.IcebergSchema,
		BeforeExternalCommit:     before,
		CatalogCommitted:         committed,
		CatalogNoOp:              noOp,
		CatalogAlreadyCommitted:  alreadyCommitted,
		CatalogReceipt:           receipt,
		CatalogReceiptFactory:    receiptFactory,
		IceStateWriting:          iceStateWriting,
	}
	if run.CommitID != "" {
		req.ManifestKey = intent.ManifestKey
		req.ExactArtifacts = exactArtifacts
	}

	result, err := s.icebergRegistrar.RegisterRun(ctx, req)
	return true, result, err
}

// ProcessRegistrationOnce claims at most one durable ordered registration.
// It is suitable for a bounded startup/background worker and deterministic tests.
func (s *Server) ProcessRegistrationOnce(ctx context.Context) (bool, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return false, err
	}
	release, admitted := s.tryAcquireCatalogWork()
	if !admitted {
		return false, nil
	}
	defer release()
	ctx, cancelLeadership := s.leadershipContext(ctx)
	defer cancelLeadership()
	policy := db.RegistrationPolicy{LeaseDuration: 30 * time.Second, MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Minute}
	r, a, ok, err := s.st.ClaimRegistration(ctx, s.nowFn(), policy)
	if err != nil || !ok {
		return ok, err
	}
	run, err := s.st.GetRun(ctx, r.RunID)
	if err != nil {
		return true, err
	}
	if run.Status != "SUCCEEDED" || run.CommitPhase != "COMPLETE" || run.CommitID != r.CommitID {
		return true, s.st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "ARTIFACT_VERIFICATION_FAILED", "durable run/commit identity mismatch", false, true, s.nowFn(), policy)
	}
	var intent durableCommitIntent
	if err := json.Unmarshal(run.CommitIntentJSON, &intent); err != nil || intent.ManifestKey != r.ManifestKey {
		return true, s.st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "ARTIFACT_VERIFICATION_FAILED", "durable manifest identity mismatch", false, true, s.nowFn(), policy)
	}
	var manifest struct {
		SchemaVersion int               `json:"schema_version"`
		RunID         string            `json:"run_id"`
		Artifacts     []json.RawMessage `json:"artifacts"`
	}
	if err := json.Unmarshal(intent.Manifest, &manifest); err != nil || manifest.SchemaVersion != 2 || manifest.RunID != r.RunID {
		return true, s.st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "ARTIFACT_VERIFICATION_FAILED", "verified schema-v2 manifest unavailable", false, true, s.nowFn(), policy)
	}
	h := sha256.New()
	for _, raw := range manifest.Artifacts {
		h.Write(raw)
		h.Write([]byte{0})
	}
	if hex.EncodeToString(h.Sum(nil)) != r.ArtifactSetDigest {
		return true, s.st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "ARTIFACT_VERIFICATION_FAILED", "artifact-set digest mismatch", false, true, s.nowFn(), policy)
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(policy.LeaseDuration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-attemptCtx.Done():
				return
			case <-ticker.C:
				if err := s.st.RenewRegistrationLease(attemptCtx, r.ID, a.ID, a.FencingToken, s.nowFn(), policy.LeaseDuration); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	externalStarted := false
	before := func() error {
		err := s.st.AdvanceRegistrationPhase(attemptCtx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", s.nowFn())
		if err == nil {
			externalStarted = true
		}
		return err
	}
	receiptValue := r.Receipt
	var receiptFactory func() (string, error)
	if strings.TrimSpace(receiptValue) == "" {
		receiptFactory = func() (string, error) {
			receipt := icebergreg.CatalogReceipt{Version: icebergreg.ReceiptVersion, Backend: r.BackendType, Namespace: r.CatalogNamespace, Table: r.TableIdentifier, RegistrationID: r.ID, CommitID: r.CommitID, ArtifactSetDigest: r.ArtifactSetDigest, DefiniteAt: s.nowFn().UTC().Format(time.RFC3339Nano), IdentityAvailable: false}
			body, receiptErr := receipt.MarshalDeterministic()
			return string(body), receiptErr
		}
	}
	committed := func(receipt string) error {
		return s.st.PersistCatalogReceipt(attemptCtx, r.ID, a.ID, a.FencingToken, receipt, s.nowFn())
	}
	noOp := func(receipt string) error {
		return s.st.PersistCatalogNoOpReceipt(attemptCtx, r.ID, a.ID, a.FencingToken, receipt, s.nowFn())
	}
	iceStateWriting := func() error {
		return s.st.AdvanceRegistrationPhase(attemptCtx, r.ID, a.ID, a.FencingToken, "CATALOG_COMMITTED", "ICE_STATE_WRITING", s.nowFn())
	}
	alreadyCommitted := strings.TrimSpace(r.Receipt) != ""
	if alreadyCommitted {
		if err := s.st.AdvanceRegistrationPhase(attemptCtx, r.ID, a.ID, a.FencingToken, "PREPARED", "CATALOG_COMMITTED", s.nowFn()); err != nil {
			return true, err
		}
	}
	ran, _, runErr := s.runIcebergRegistrationWithHooks(attemptCtx, r.RunID, r.ID, r.ArtifactSetDigest, alreadyCommitted, receiptValue, receiptFactory, before, committed, noOp, iceStateWriting)
	cancel()
	<-renewDone
	if runErr == nil && !ran {
		runErr = fmt.Errorf("registration configuration is unavailable")
	}
	if runErr != nil {
		failure := icebergreg.ClassifyFailure(runErr, externalStarted)
		s.log.Warn(
			"iceberg registration attempt failed",
			slog.String("run_id", r.RunID),
			slog.String("registration_id", r.ID),
			slog.Int("registration_attempt", a.AttemptNumber),
			slog.String("commit_id", r.CommitID),
			slog.String("catalog_status", r.Status),
			slog.String("error_class", string(failure.Class)),
			slog.String("error_message", runErr.Error()),
			slog.Bool("external_commit_started", externalStarted),
		)
		err := s.st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, string(failure.Class), runErr.Error(), failure.Retryable, failure.DefiniteRejection, s.nowFn(), policy)
		if errors.Is(err, db.ErrRegistrationFenced) {
			_ = s.st.RecordRegistrationStaleResult(ctx, r.ID, a.ID, string(failure.Class), s.nowFn())
			return true, nil
		}
		return true, err
	}
	err = s.st.CompleteRegistration(ctx, r.ID, a.ID, a.FencingToken, s.nowFn())
	if errors.Is(err, db.ErrRegistrationFenced) {
		_ = s.st.RecordRegistrationStaleResult(ctx, r.ID, a.ID, "STALE_COMPLETION", s.nowFn())
		return true, nil
	}
	return true, err
}
