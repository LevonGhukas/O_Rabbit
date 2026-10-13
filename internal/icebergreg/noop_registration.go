package icebergreg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	iceberg "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
)

func completeNoOpRegistration(req RunRequest, reason string, evidence []byte) (RunResult, error) {
	if req.CatalogAlreadyCommitted {
		if req.IceStateWriting != nil {
			if err := req.IceStateWriting(); err != nil {
				return RunResult{}, err
			}
		}
		return RunResult{}, nil
	}
	if req.CatalogReceiptFactory == nil || req.CatalogNoOp == nil {
		return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("durable no-op receipt callbacks are required"))
	}
	raw, err := req.CatalogReceiptFactory()
	if err != nil {
		return RunResult{}, err
	}
	receipt, err := ParseCatalogReceipt(raw)
	if err != nil {
		return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("invalid no-op catalog receipt base: %w", err))
	}
	digest := sha256.Sum256(evidence)
	receipt.NoOp = true
	receipt.NoOpReason = reason
	receipt.NoOpEvidenceDigest = hex.EncodeToString(digest[:])
	body, err := receipt.MarshalDeterministic()
	if err != nil {
		return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, err)
	}
	if err := req.CatalogNoOp(string(body)); err != nil {
		return RunResult{}, err
	}
	if req.IceStateWriting != nil {
		if err := req.IceStateWriting(); err != nil {
			return RunResult{}, err
		}
	}
	return RunResult{}, nil
}

func (m *Manager) registerVerifiedEmpty(ctx context.Context, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, basePrefix string, uploader *s3io.Uploader, iceKey string, cur iceState, ds datasetState) (RunResult, error) {
	cat, ident, err := openRESTCatalog(ctx, req, reg, regS3, table)
	if err != nil {
		return RunResult{}, err
	}
	tbl, loadErr := cat.LoadTable(ctx, ident)
	tableExists := loadErr == nil
	if loadErr != nil && !errors.Is(loadErr, icecatalog.ErrNoSuchTable) {
		return RunResult{}, loadErr
	}
	action, err := decideVerifiedEmptyAction(tableExists, req.Incremental, req.WriteMode, len(req.DurableIcebergSchema) > 0)
	if err != nil {
		return RunResult{}, err
	}

	if action == "NO_OP" {
		evidence := []byte(strings.Join([]string{req.RunID, req.RegistrationID, req.CommitID, req.ManifestKey, req.ArtifactSetDigest, "incremental-existing-table"}, "\x00"))
		receipt, err := buildNoOpReceipt(req, "VERIFIED_EMPTY_INCREMENTAL_EXISTING_TABLE", evidence)
		if err != nil {
			return RunResult{}, err
		}
		if !req.CatalogAlreadyCommitted {
			if req.CatalogNoOp == nil {
				return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("durable no-op receipt callback is required"))
			}
			if err := req.CatalogNoOp(receipt); err != nil {
				return RunResult{}, err
			}
		}
		return persistEmptyIceState(ctx, req, uploader, iceKey, cur, ds, receipt)
	}

	if !req.CatalogAlreadyCommitted {
		if req.BeforeExternalCommit == nil {
			return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("durable external-commit boundary is required"))
		}
		if err := req.BeforeExternalCommit(); err != nil {
			return RunResult{}, err
		}
		if action == "REPLACE_EMPTY" {
			if err := replaceRESTGoTableWithEmpty(ctx, tbl, req, reg, table); err != nil {
				return RunResult{}, err
			}
		} else {
			if _, err := createRESTGoTable(ctx, cat, ident, req, reg, basePrefix); err != nil {
				return RunResult{}, err
			}
		}
		if req.CatalogReceiptFactory == nil || req.CatalogCommitted == nil {
			return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("durable catalog receipt callbacks are required"))
		}
		receipt, err := req.CatalogReceiptFactory()
		if err != nil {
			return RunResult{}, err
		}
		if err := req.CatalogCommitted(receipt); err != nil {
			return RunResult{}, err
		}
		reason := "EMPTY_TABLE_CREATED"
		if tableExists {
			reason = "EMPTY_FULL_REFRESH_REPLACED"
		}
		m.log.Info("empty iceberg registration committed", slog.String("run_id", req.RunID), slog.String("table", table), slog.String("outcome", reason))
		return persistEmptyIceState(ctx, req, uploader, iceKey, cur, ds, receipt)
	}

	return persistEmptyIceState(ctx, req, uploader, iceKey, cur, ds, req.CatalogReceipt)
}

func decideVerifiedEmptyAction(tableExists, incremental bool, writeMode string, durableSchemaAvailable bool) (string, error) {
	if tableExists && incremental && !strings.EqualFold(strings.TrimSpace(writeMode), "overwrite") {
		return "NO_OP", nil
	}
	if tableExists {
		return "REPLACE_EMPTY", nil
	}
	if !durableSchemaAvailable {
		return "", failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("cannot create empty Iceberg table: durable source/query schema is unavailable"))
	}
	return "CREATE_EMPTY", nil
}

func buildNoOpReceipt(req RunRequest, reason string, evidence []byte) (string, error) {
	if req.CatalogReceiptFactory == nil {
		return "", failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("durable no-op receipt factory is required"))
	}
	raw, err := req.CatalogReceiptFactory()
	if err != nil {
		return "", err
	}
	receipt, err := ParseCatalogReceipt(raw)
	if err != nil {
		return "", failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("invalid no-op catalog receipt base: %w", err))
	}
	digest := sha256.Sum256(evidence)
	receipt.NoOp = true
	receipt.NoOpReason = reason
	receipt.NoOpEvidenceDigest = hex.EncodeToString(digest[:])
	body, err := receipt.MarshalDeterministic()
	return string(body), err
}

func persistEmptyIceState(ctx context.Context, req RunRequest, uploader *s3io.Uploader, iceKey string, cur iceState, ds datasetState, receipt string) (RunResult, error) {
	if req.IceStateWriting != nil {
		if err := req.IceStateWriting(); err != nil {
			return RunResult{}, err
		}
	}
	if strings.TrimSpace(receipt) == "" {
		return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("catalog receipt is required for empty registration"))
	}
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(receipt), &raw); err != nil {
		return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("invalid empty registration receipt: %w", err))
	}
	cur = nextIceState(cur, ds.commitCheckpoint(), req.RunID, cur.LastInsertedPart)
	cur.CatalogReceipt = raw
	body, _ := json.MarshalIndent(cur, "", "  ")
	body = append(body, '\n')
	if err := uploader.PutObjectBytes(ctx, iceKey, body, "application/json", nil); err != nil {
		return RunResult{}, failure.NewFailure(failure.FailureIceStateWrite, true, true, err)
	}
	verified, found, err := uploader.GetObjectBytes(ctx, iceKey)
	if err != nil || !found || !bytes.Equal(verified, body) {
		return RunResult{}, failure.NewFailure(failure.FailureIceStateVerify, true, true, fmt.Errorf("_ice_state.json verification failed: %w", err))
	}
	return RunResult{}, nil
}

func replaceRESTGoTableWithEmpty(ctx context.Context, tbl *icetable.Table, req RunRequest, reg RunConfig, table string) error {
	var existingFiles []string
	if snap := tbl.CurrentSnapshot(); snap != nil {
		fs, err := tbl.FS(ctx)
		if err != nil {
			return fmt.Errorf("iceberg full refresh: open table fs: %w", err)
		}
		manifests, err := snap.Manifests(fs)
		if err != nil {
			return fmt.Errorf("iceberg full refresh: list manifests: %w", err)
		}
		for _, manifest := range manifests {
			entries, err := manifest.FetchEntries(fs, true)
			if err != nil {
				return fmt.Errorf("iceberg full refresh: fetch manifest entries: %w", err)
			}
			for _, entry := range entries {
				existingFiles = append(existingFiles, entry.DataFile().FilePath())
			}
		}
	}
	schemaTx := tbl.NewTransaction()
	var sourceSchema *iceberg.Schema
	var err error
	if reg.SchemaEvolution == "additive" {
		sourceSchema, err = inferRunIcebergSchema(ctx, req, table)
		if err != nil {
			return err
		}
	}
	if err := applySchemaOptions(schemaTx, tbl.Schema(), sourceSchema, reg); err != nil {
		return err
	}
	tbl, err = schemaTx.Commit(ctx)
	if err != nil {
		return err
	}
	tx := tbl.NewTransaction()
	currentSpec := tbl.Spec()
	if err := applyPartitionSpec(tx, &currentSpec, reg.PartitionSpec, tbl.Schema()); err != nil {
		return err
	}
	if err := tx.SetProperties(tableOptionProperties(reg)); err != nil {
		return err
	}
	if err := applyMetadataRetention(tx, reg.MetadataRetention); err != nil {
		return err
	}
	identity := OperationIdentity{RegistrationID: req.RegistrationID, RunID: req.RunID, CommitID: req.CommitID, ArtifactSetDigest: req.ArtifactSetDigest, ManifestKey: req.ManifestKey}
	if err := applyRESTGoFileMutation(ctx, tx, true, existingFiles, nil, iceberg.Properties(identity.Properties())); err != nil {
		return err
	}
	_, err = tx.Commit(ctx)
	return err
}

func requireNoOpReconciliation(req RunRequest) (RunResult, error) {
	if req.BeforeExternalCommit == nil {
		return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("durable reconciliation boundary is required for unverified already-applied registration"))
	}
	if err := req.BeforeExternalCommit(); err != nil {
		return RunResult{}, err
	}
	return RunResult{}, failure.NewFailure(failure.FailureExternalAmbiguous, false, false, fmt.Errorf("already-applied registration lacks durable _ice_state.json receipt evidence; reconciliation required"))
}
