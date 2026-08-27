package icebergreg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"time"

	iceberg "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	restcatalog "github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

type Manager struct {
	log         *slog.Logger
	iceBinary   string
	execCommand iceCommandFactory
}

type ManagerConfig struct {
	IceBinary string
}

type RunRequest struct {
	RunID        string
	Registration RunConfig

	SourceEngine string
	SourceDSN    string
	SourceMode   string
	SourceTable  string
	SourceQuery  string
	ColumnTypes  map[string]string
	RecordPath   string
	FileFormat   string
	SelectColumns []string
	QueryHash    string
	Incremental  bool
	WriteMode    string

	DatasetPrefix            string
	DatasetS3                s3io.Config
	CommitID                 string
	ManifestKey              string
	RegistrationID           string
	ArtifactSetDigest        string
	ExactArtifacts           []artifact.Record
	ExactArtifactSetVerified bool
	DurableIcebergSchema     json.RawMessage
	BeforeExternalCommit     func() error
	CatalogCommitted         func(receipt string) error
	CatalogNoOp              func(receipt string) error
	CatalogAlreadyCommitted  bool
	CatalogReceipt           string
	CatalogReceiptFactory    func() (string, error)
	IceStateWriting          func() error
}

type RunResult struct {
	Objects int
}

func documentFilter(engine, recordPath, fileFormat string) map[string]any {
	if connectors.NormalizeSourceEngine(engine) != "s3" {
		return nil
	}
	filter := map[string]any{}
	if recordPath = strings.TrimSpace(recordPath); recordPath != "" {
		filter["record_path"] = recordPath
	}
	if fileFormat = strings.TrimSpace(fileFormat); fileFormat != "" {
		filter["format"] = fileFormat
	}
	if len(filter) == 0 {
		return nil
	}
	return filter
}

type datasetState struct {
	Bucket            string   `json:"bucket"`
	Prefix            string   `json:"prefix"`
	LastCommittedRun  string   `json:"last_committed_run_id"`
	CommittedAt       string   `json:"committed_at"`
	MaxHWMValue       string   `json:"max_hwm_value"`
	MaxPart           int      `json:"max_part"`
	NextPart          int      `json:"next_part"`
	LastRunObjects    []string `json:"last_run_objects"`
	LastRunObjectsV2  []string `json:"last_run_objects_v2"`
	LastCommittedKeys []string `json:"last_committed_objects"`
}

type commitCheckpoint struct {
	RunID       string
	CommittedAt string
}

type iceState struct {
	LastInsertedPart int             `json:"last_inserted_part"`
	LastRunID        string          `json:"last_run_id,omitempty"`
	UpdatedAt        string          `json:"updated_at"`
	CatalogReceipt   json.RawMessage `json:"catalog_receipt"`
}

type icebergObj struct {
	key   string
	part  int
	rows  int64
	bytes int64
}

func NewManager(log *slog.Logger, cfgs ...ManagerConfig) *Manager {
	if log == nil {
		log = slog.Default()
	}
	cfg := ManagerConfig{}
	if len(cfgs) != 0 {
		cfg = cfgs[0]
	}
	iceBinary := strings.TrimSpace(cfg.IceBinary)
	if iceBinary == "" {
		iceBinary = DefaultIceBinary
	}
	return &Manager{
		log:         log,
		iceBinary:   iceBinary,
		execCommand: exec.CommandContext,
	}
}

func (m *Manager) RegisterRun(ctx context.Context, req RunRequest) (RunResult, error) {
	reg := req.Registration.Normalize()
	if !reg.Enabled {
		return RunResult{}, nil
	}
	sourceMode := normalizedRunRequestSourceMode(req.SourceMode)
	table := reg.Table
	if table == "" {
		if sourceMode == "query" {
			return RunResult{}, fmt.Errorf("iceberg.table is required for query-mode registration")
		}
		table = DefaultTable(req.SourceEngine, req.SourceTable)
	}
	if err := validateTableIdentifier(table); err != nil {
		return RunResult{}, err
	}

	regS3 := req.DatasetS3
	if v := strings.TrimSpace(reg.S3.Endpoint); v != "" {
		regS3.Endpoint = v
	}
	if v := strings.TrimSpace(reg.S3.Region); v != "" {
		regS3.Region = v
	}
	regS3.ForcePathStyle = reg.S3.PathStyleAccess
	if v := strings.TrimSpace(reg.S3.AccessKeyID); v != "" {
		regS3.AccessKeyID = v
	}
	if v := strings.TrimSpace(reg.S3.SecretAccessKey); v != "" {
		regS3.SecretAccessKey = v
	}
	u, err := s3io.New(ctx, regS3)
	if err != nil {
		return RunResult{}, err
	}

	basePrefix := strings.TrimSuffix(strings.TrimSpace(req.DatasetPrefix), "/")
	if basePrefix == "" {
		return RunResult{}, fmt.Errorf("empty dataset prefix")
	}

	stateKey := basePrefix + "/_state.json"
	ds, err := loadDatasetState(ctx, u, stateKey, req.RunID)
	if err != nil {
		return RunResult{}, err
	}
	if p := strings.TrimSuffix(strings.TrimSpace(ds.Prefix), "/"); p != "" && p != basePrefix {
		basePrefix = p
		stateKey = basePrefix + "/_state.json"
		ds, err = loadDatasetState(ctx, u, stateKey, req.RunID)
		if err != nil {
			return RunResult{}, err
		}
	}

	iceKey := basePrefix + "/_ice_state.json"
	ib, ok, err := u.GetObjectBytes(ctx, iceKey)
	if err != nil {
		return RunResult{}, err
	}
	cur := iceState{}
	if ok {
		if err := json.Unmarshal(ib, &cur); err != nil {
			return RunResult{}, failure.NewFailure(failure.FailureIceStateVerify, false, true, fmt.Errorf("decode existing _ice_state.json: %w", err))
		}
		if len(cur.CatalogReceipt) > 0 && strings.TrimSpace(req.CatalogReceipt) != "" {
			var expected any
			var actual any
			if json.Unmarshal([]byte(req.CatalogReceipt), &expected) != nil || json.Unmarshal(cur.CatalogReceipt, &actual) != nil || !reflect.DeepEqual(expected, actual) {
				return RunResult{}, failure.NewFailure(failure.FailureIceStateVerify, false, true, fmt.Errorf("conflicting _ice_state.json catalog receipt"))
			}
		}
	}

	objStats := make(map[string]struct{ rows, bytes int64 })
	keys := make([]string, 0, len(req.ExactArtifacts))
	if len(req.ExactArtifacts) > 0 {
		for _, record := range req.ExactArtifacts {
			if err := record.Validate(); err != nil || record.RunID != req.RunID {
				return RunResult{}, fmt.Errorf("exact registration artifact invalid: %w", err)
			}
			keys = append(keys, record.ObjectKey)
			objStats[record.ObjectKey] = struct{ rows, bytes int64 }{record.RowCount, record.ByteSize}
		}
	} else {
		keys, err = collectCommittedKeys(ctx, u, basePrefix, ds, objStats)
		if err != nil {
			return RunResult{}, err
		}
	}

	seen := make(map[string]struct{}, len(keys))
	objs := make([]icebergObj, 0, len(keys))
	validObjects := 0
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		if !strings.HasPrefix(key, basePrefix+"/") || strings.Contains(key, "/_staging/") || !strings.HasSuffix(key, ".parquet") {
			continue
		}
		part, parsed := parsePartNum(key)
		if !parsed {
			continue
		}
		validObjects++
		if part <= cur.LastInsertedPart {
			continue
		}
		st := objStats[key]
		objs = append(objs, icebergObj{key: key, part: part, rows: st.rows, bytes: st.bytes})
	}
	if len(objs) == 0 {
		if len(keys) == 0 && req.ExactArtifactSetVerified {
			return m.registerVerifiedEmpty(ctx, req, reg, table, regS3, basePrefix, u, iceKey, cur, ds)
		}
		if validObjects == 0 {
			return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("no valid parquet artifacts are eligible for registration"))
		}
		if !ok || len(cur.CatalogReceipt) == 0 {
			return requireNoOpReconciliation(req)
		}
		return completeNoOpRegistration(req, "ALL_ARTIFACTS_ALREADY_APPLIED", ib)
	}

	sort.Slice(objs, func(i, j int) bool {
		if objs[i].part == objs[j].part {
			ii, iok := parsePartFileIndex(objs[i].key)
			jj, jok := parsePartFileIndex(objs[j].key)
			if iok && jok && ii != jj {
				return ii < jj
			}
			return objs[i].key < objs[j].key
		}
		return objs[i].part < objs[j].part
	})
	if err := waitForObject(ctx, u, req.DatasetS3.Bucket, objs[0].key, stateKey); err != nil {
		return RunResult{}, err
	}

	if !req.CatalogAlreadyCommitted && req.BeforeExternalCommit == nil && strings.TrimSpace(req.CommitID) != "" {
		return RunResult{}, fmt.Errorf("durable external-commit boundary is required")
	}
	if !req.CatalogAlreadyCommitted && req.BeforeExternalCommit != nil {
		if err := req.BeforeExternalCommit(); err != nil {
			return RunResult{}, err
		}
	}
	if !req.CatalogAlreadyCommitted {
		if err := m.executeRegistrationEngine(ctx, req, reg, table, regS3, basePrefix, objs); err != nil {
			return RunResult{}, err
		}
		if req.CatalogReceiptFactory != nil {
			receipt, err := req.CatalogReceiptFactory()
			if err != nil {
				return RunResult{}, err
			}
			req.CatalogReceipt = receipt
		}
		if req.CatalogCommitted == nil && strings.TrimSpace(req.CommitID) != "" {
			return RunResult{}, fmt.Errorf("durable catalog receipt callback is required")
		}
		if req.CatalogCommitted != nil {
			if err := req.CatalogCommitted(req.CatalogReceipt); err != nil {
				return RunResult{}, err
			}
		}
	}

	if req.IceStateWriting != nil {
		if err := req.IceStateWriting(); err != nil {
			return RunResult{}, err
		}
	}
	cur = nextIceState(cur, ds.commitCheckpoint(), req.RunID, objs[len(objs)-1].part)
	if strings.TrimSpace(req.CatalogReceipt) == "" && strings.TrimSpace(req.CommitID) != "" {
		return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("catalog receipt is required"))
	}
	if strings.TrimSpace(req.CatalogReceipt) != "" {
		var receipt json.RawMessage
		if err := json.Unmarshal([]byte(req.CatalogReceipt), &receipt); err != nil {
			return RunResult{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("invalid catalog receipt: %w", err))
		}
		cur.CatalogReceipt = receipt
	}
	jb, _ := json.MarshalIndent(cur, "", "  ")
	jb = append(jb, '\n')
	if err := u.PutObjectBytes(ctx, iceKey, jb, "application/json", nil); err != nil {
		return RunResult{}, failure.NewFailure(failure.FailureIceStateWrite, true, true, err)
	}
	verified, found, err := u.GetObjectBytes(ctx, iceKey)
	if err != nil || !found || !bytes.Equal(verified, jb) {
		return RunResult{}, failure.NewFailure(failure.FailureIceStateVerify, true, true, fmt.Errorf("_ice_state.json verification failed: %w", err))
	}

	m.log.Info("iceberg registration SUCCEEDED",
		slog.String("run_id", req.RunID),
		slog.String("table", table),
		slog.Int("objects", len(objs)),
		slog.Int("last_inserted_part", cur.LastInsertedPart),
		slog.String("state_key", iceKey),
	)
	return RunResult{Objects: len(objs)}, nil
}

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

func openRESTCatalog(ctx context.Context, req RunRequest, reg RunConfig, regS3 s3io.Config, table string) (*restcatalog.Catalog, icetable.Identifier, error) {
	uri := normalizeLocalhost(reg.URI)
	if uri == "" {
		return nil, nil, fmt.Errorf("missing iceberg rest uri in persisted run registration config")
	}
	if parsed, err := url.Parse(uri); err == nil {
		parsed.Path = strings.TrimSuffix(strings.TrimSuffix(parsed.Path, "/v1"), "/")
		uri = parsed.String()
	}
	cat, err := restcatalog.NewCatalog(ctx, "rest", uri,
		restcatalog.WithOAuthToken(strings.TrimSpace(reg.BearerToken)),
		restcatalog.WithWarehouseLocation("s3://"+req.DatasetS3.Bucket),
		restcatalog.WithAdditionalProps(icebergRegistrationS3Props(regS3, reg.CredentialVending.Required)),
	)
	if err != nil {
		return nil, nil, err
	}
	parts := strings.Split(table, ".")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	if len(parts) < 2 {
		return nil, nil, fmt.Errorf("invalid iceberg table %q (expected namespace.table)", table)
	}
	ident := icetable.Identifier(parts)
	ns := ident[:len(ident)-1]
	if exists, err := cat.CheckNamespaceExists(ctx, ns); err != nil {
		return nil, nil, err
	} else if !exists {
		if err := cat.CreateNamespace(ctx, ns, iceberg.Properties{}); err != nil {
			return nil, nil, err
		}
	}
	return cat, ident, nil
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

func (m *Manager) executeRegistrationEngine(ctx context.Context, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, basePrefix string, objs []icebergObj) error {
	isFullRefresh := !req.Incremental || strings.EqualFold(req.WriteMode, "overwrite")
	switch reg.Engine {
	case "rest-go":
		return runRESTGoRegister(ctx, m.log, req, reg, table, regS3, basePrefix, objs)
	case "ice":
		tbl, err := prepareRESTGoTable(ctx, m.log, req, reg, table, regS3, basePrefix)
		if err != nil {
			return err
		}
		expectedLocation := strings.TrimSuffix(restGoTableLocation(req.DatasetS3.Bucket, basePrefix), "/")
		if got := strings.TrimSuffix(strings.TrimSpace(tbl.Location()), "/"); got != "" && got != expectedLocation {
			return fmt.Errorf("iceberg table %s location %q does not match dataset location %q; ice insert requires parquet files under the table location", table, got, expectedLocation)
		}

		if isFullRefresh && tbl.CurrentSnapshot() != nil {
			// Full Refresh: drop and recreate the table so that the subsequent
			// ice insert starts from an empty snapshot (no previous rows remain).
			m.log.Info("iceberg full refresh: dropping table before ice insert",
				slog.String("run_id", req.RunID),
				slog.String("table", table),
			)
			if err := dropAndRecreateCatalogTable(ctx, m.log, req, reg, table, regS3, basePrefix); err != nil {
				return fmt.Errorf("iceberg full refresh drop-recreate: %w", err)
			}
		}

		return runIceCLIRegister(ctx, m.execCommand, m.iceBinary, req, reg, table, regS3, objs)
	default:
		return fmt.Errorf("iceberg engine %q is not supported by master", reg.Engine)
	}
}

func collectCommittedKeys(ctx context.Context, u *s3io.Uploader, basePrefix string, ds datasetState, objStats map[string]struct{ rows, bytes int64 }) ([]string, error) {
	keys := []string{}
	if commitKeys, err := u.ListKeys(ctx, basePrefix+"/_commits/"); err == nil && len(commitKeys) != 0 {
		type commitManifest struct {
			Objects   []string         `json:"objects"`
			ObjectsV2 []map[string]any `json:"objects_v2"`
		}
		for _, ck := range commitKeys {
			b, ok, err := u.GetObjectBytes(ctx, ck)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			var cm commitManifest
			if err := json.Unmarshal(b, &cm); err != nil {
				continue
			}
			keys = append(keys, cm.Objects...)
			for _, ov := range cm.ObjectsV2 {
				key, _ := ov["key"].(string)
				key = strings.TrimSpace(key)
				if key == "" {
					continue
				}
				var rows int64
				var bytes int64
				if v, ok := ov["rows"].(float64); ok {
					rows = int64(v)
				}
				if v, ok := ov["bytes"].(float64); ok {
					bytes = int64(v)
				}
				if rows != 0 || bytes != 0 {
					objStats[key] = struct{ rows, bytes int64 }{rows: rows, bytes: bytes}
				}
			}
		}
	}
	if len(keys) == 0 {
		keys = ds.LastRunObjects
		if len(keys) == 0 {
			keys = ds.LastRunObjectsV2
		}
		if len(keys) == 0 {
			keys = ds.LastCommittedKeys
		}
	}
	if len(keys) != 0 {
		return keys, nil
	}

	listed, err := u.ListKeys(ctx, basePrefix+"/part-")
	if err != nil {
		return nil, fmt.Errorf("list parquet keys: %w", err)
	}
	return listed, nil
}

func waitForObject(ctx context.Context, u *s3io.Uploader, bucket, key, stateKey string) error {
	deadline := time.Now().Add(1 * time.Second)
	for {
		if _, err := u.Head(ctx, key); err == nil {
			return nil
		} else if !isNotFoundErr(err) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dataset state references missing object: s3://%s/%s (state %s)", bucket, key, stateKey)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s datasetState) commitCheckpoint() commitCheckpoint {
	return commitCheckpoint{
		RunID:       strings.TrimSpace(s.LastCommittedRun),
		CommittedAt: strings.TrimSpace(s.CommittedAt),
	}
}

func nextIceState(prev iceState, cp commitCheckpoint, fallbackRunID string, lastInsertedPart int) iceState {
	runID := strings.TrimSpace(cp.RunID)
	if runID == "" {
		runID = strings.TrimSpace(fallbackRunID)
	}
	updatedAt := strings.TrimSpace(cp.CommittedAt)
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	prev.LastInsertedPart = lastInsertedPart
	prev.LastRunID = runID
	prev.UpdatedAt = updatedAt
	return prev
}

func parsePartNum(key string) (int, bool) {
	i := strings.LastIndex(key, "/part-")
	if i < 0 {
		return 0, false
	}
	s := key[i+len("/part-"):]
	if !strings.HasSuffix(s, ".parquet") {
		return 0, false
	}
	s = strings.TrimSuffix(s, ".parquet")
	if s == "" {
		return 0, false
	}
	if dash := strings.IndexByte(s, '-'); dash >= 0 {
		suffix := s[dash+1:]
		if suffix == "" {
			return 0, false
		}
		for _, r := range suffix {
			if r < '0' || r > '9' {
				return 0, false
			}
		}
		s = s[:dash]
		if s == "" {
			return 0, false
		}
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
	}
	return n, true
}

func parsePartFileIndex(key string) (int, bool) {
	i := strings.LastIndex(key, "/part-")
	if i < 0 {
		return 0, false
	}
	s := key[i+len("/part-"):]
	if !strings.HasSuffix(s, ".parquet") {
		return 0, false
	}
	s = strings.TrimSuffix(s, ".parquet")
	if s == "" {
		return 0, false
	}
	if dash := strings.IndexByte(s, '-'); dash >= 0 {
		s = s[dash+1:]
		if s == "" {
			return 0, false
		}
		n := 0
		for _, r := range s {
			if r < '0' || r > '9' {
				return 0, false
			}
			n = n*10 + int(r-'0')
		}
		return n, true
	}
	return 0, true
}

func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	le := strings.ToLower(err.Error())
	return strings.Contains(le, "notfound") ||
		strings.Contains(le, "no such key") ||
		strings.Contains(le, "nosuchkey") ||
		strings.Contains(le, "not found") ||
		strings.Contains(le, "status code: 404")
}

func loadDatasetState(ctx context.Context, u *s3io.Uploader, stateKey string, wantRunID string) (datasetState, error) {
	deadline := time.Now().Add(2 * time.Second)
	backoff := 25 * time.Millisecond
	var lastErr error

	for {
		b, ok, err := u.GetObjectBytes(ctx, stateKey)
		if err != nil {
			return datasetState{}, err
		}
		if ok {
			var ds datasetState
			if err := json.Unmarshal(b, &ds); err != nil {
				return datasetState{}, fmt.Errorf("parse dataset state: %w", err)
			}
			if strings.TrimSpace(ds.LastCommittedRun) == "" || strings.EqualFold(ds.LastCommittedRun, wantRunID) {
				return ds, nil
			}
			lastErr = fmt.Errorf("dataset state last_committed_run_id=%s (want %s)", ds.LastCommittedRun, wantRunID)
		} else {
			lastErr = fmt.Errorf("missing dataset state: %s", stateKey)
		}

		if time.Now().After(deadline) {
			return datasetState{}, lastErr
		}
		select {
		case <-ctx.Done():
			return datasetState{}, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 300*time.Millisecond {
			backoff *= 2
			if backoff > 300*time.Millisecond {
				backoff = 300 * time.Millisecond
			}
		}
	}
}

func normalizeLocalhost(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if strings.EqualFold(parsed.Hostname(), "localhost") {
		parsed.Host = strings.Replace(parsed.Host, "localhost", "127.0.0.1", 1)
		return parsed.String()
	}
	return raw
}

func icebergRegistrationS3Props(regS3 s3io.Config, credentialVending bool) iceberg.Properties {
	props := iceberg.Properties{}
	if ep := strings.TrimSuffix(strings.TrimSpace(regS3.Endpoint), "/"); ep != "" {
		props["s3.endpoint"] = normalizeLocalhost(ep)
	}
	if region := strings.TrimSpace(regS3.Region); region != "" {
		props["s3.region"] = region
	}
	if !credentialVending {
		if accessKey := strings.TrimSpace(regS3.AccessKeyID); accessKey != "" {
			props["s3.access-key-id"] = accessKey
		}
		if secretKey := strings.TrimSpace(regS3.SecretAccessKey); secretKey != "" {
			props["s3.secret-access-key"] = secretKey
		}
	}
	if regS3.ForcePathStyle {
		props["s3.force-virtual-addressing"] = "false"
	} else {
		props["s3.force-virtual-addressing"] = "true"
	}
	return props
}

func prepareRESTGoTable(ctx context.Context, log *slog.Logger, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, basePrefix string) (*icetable.Table, error) {
	if strings.TrimSpace(os.Getenv("AWS_EC2_METADATA_DISABLED")) == "" {
		_ = os.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	}
	cat, ident, err := openRESTCatalog(ctx, req, reg, regS3, table)
	if err != nil {
		return nil, err
	}
	return loadOrCreateRESTGoTable(ctx, log, cat, ident, req, reg, table, basePrefix)
}

// dropAndRecreateCatalogTable drops the Iceberg table via the REST catalog and
// immediately recreates it empty. Used during Full Refresh for the ice engine
// so that the subsequent ice insert appends into a clean snapshot.
func dropAndRecreateCatalogTable(ctx context.Context, log *slog.Logger, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, basePrefix string) error {
	cat, ident, err := openRESTCatalog(ctx, req, reg, regS3, table)
	if err != nil {
		return err
	}
	if dropErr := cat.DropTable(ctx, ident); dropErr != nil && !errors.Is(dropErr, icecatalog.ErrNoSuchTable) {
		return fmt.Errorf("drop iceberg table %s: %w", table, dropErr)
	}
	log.Info("iceberg full refresh: table dropped, recreating",
		slog.String("run_id", req.RunID),
		slog.String("table", table),
	)
	_, err = createRESTGoTable(ctx, cat, ident, req, reg, basePrefix)
	return err
}

func runRESTGoRegister(ctx context.Context, log *slog.Logger, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, basePrefix string, objs []icebergObj) error {
	tbl, err := prepareRESTGoTable(ctx, log, req, reg, table, regS3, basePrefix)
	if err != nil {
		return err
	}

	newFiles := make([]string, 0, len(objs))
	for _, obj := range objs {
		newFiles = append(newFiles, fmt.Sprintf("s3://%s/%s", req.DatasetS3.Bucket, obj.key))
	}

	isFullRefresh := !req.Incremental || strings.EqualFold(req.WriteMode, "overwrite")

	log.Info("iceberg registration files",
		slog.String("run_id", req.RunID),
		slog.String("table", table),
		slog.Int("new_objects", len(newFiles)),
		slog.Bool("full_refresh", isFullRefresh),
	)

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
	snapshotProperties := iceberg.Properties(identity.Properties())
	if reg.Upsert.Enabled && !isFullRefresh && tbl.CurrentSnapshot() != nil {
		filter, err := buildUpsertDeleteFilter(ctx, tbl, newFiles, reg.Upsert.Keys)
		if err != nil {
			return err
		}
		if !filter.Equals(iceberg.AlwaysFalse{}) {
			if err := tx.Delete(ctx, filter, snapshotProperties); err != nil {
				return fmt.Errorf("iceberg upsert delete existing rows: %w", err)
			}
		}
	}
	var existingFiles []string
	if isFullRefresh {
		// Collect all existing data files from the current snapshot so we can
		// atomically replace them with the new files (true snapshot overwrite).
		if snap := tbl.CurrentSnapshot(); snap != nil {
			fs, fsErr := tbl.FS(ctx)
			if fsErr != nil {
				return fmt.Errorf("iceberg full refresh: open table fs: %w", fsErr)
			}
			manifests, mErr := snap.Manifests(fs)
			if mErr != nil {
				return fmt.Errorf("iceberg full refresh: list manifests: %w", mErr)
			}
			for _, mf := range manifests {
				entries, eErr := mf.FetchEntries(fs, true)
				if eErr != nil {
					return fmt.Errorf("iceberg full refresh: fetch manifest entries: %w", eErr)
				}
				for _, e := range entries {
					existingFiles = append(existingFiles, e.DataFile().FilePath())
				}
			}
		}

		log.Info("iceberg registration full refresh",
			slog.String("run_id", req.RunID),
			slog.String("table", table),
			slog.Int("files_to_delete", len(existingFiles)),
			slog.Int("files_to_add", len(newFiles)),
		)
	}
	partitionedTable := !currentSpec.IsUnpartitioned() || len(reg.PartitionSpec) > 0
	if partitionedTable {
		fs, err := tbl.FS(ctx)
		if err != nil {
			return err
		}
		reader, err := newParquetRecordReader(ctx, fs, newFiles)
		if err != nil {
			return err
		}
		defer reader.Release()
		if isFullRefresh {
			if err := tx.Overwrite(ctx, reader, snapshotProperties); err != nil {
				return fmt.Errorf("iceberg partitioned full refresh: %w", err)
			}
		} else if err := tx.Append(ctx, reader, snapshotProperties); err != nil {
			return fmt.Errorf("iceberg partitioned append: %w", err)
		}
	} else if err := applyRESTGoFileMutation(ctx, tx, isFullRefresh, existingFiles, newFiles, snapshotProperties); err != nil {
		return err
	}
	_, err = tx.Commit(ctx)
	return err
}

type restGoFileMutation interface {
	AddFiles(context.Context, []string, iceberg.Properties, bool) error
	ReplaceDataFiles(context.Context, []string, []string, iceberg.Properties) error
}

func applyRESTGoFileMutation(ctx context.Context, tx restGoFileMutation, fullRefresh bool, existingFiles, newFiles []string, properties iceberg.Properties) error {
	if fullRefresh {
		if err := tx.ReplaceDataFiles(ctx, existingFiles, newFiles, properties); err != nil {
			return fmt.Errorf("iceberg full refresh replace: %w", err)
		}
		return nil
	}
	if err := tx.AddFiles(ctx, newFiles, properties, false); err != nil {
		return fmt.Errorf("iceberg incremental append: %w", err)
	}
	return nil
}

func loadOrCreateRESTGoTable(ctx context.Context, log *slog.Logger, cat *restcatalog.Catalog, ident icetable.Identifier, req RunRequest, reg RunConfig, table, basePrefix string) (*icetable.Table, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err == nil {
		if reg.Upsert.Enabled && tbl.Metadata().Version() < 2 {
			return nil, fmt.Errorf("upsert requires Iceberg format version 2 or newer")
		}
		var sourceSchema *iceberg.Schema
		if reg.SchemaEvolution == "additive" {
			sourceSchema, err = inferRunIcebergSchema(ctx, req, table)
			if err != nil {
				return nil, err
			}
		}
		schemaTx := tbl.NewTransaction()
		if err := applySchemaOptions(schemaTx, tbl.Schema(), sourceSchema, reg); err != nil {
			return nil, err
		}
		tbl, err = schemaTx.Commit(ctx)
		if err != nil {
			return nil, err
		}
		return applySortOrder(ctx, cat, ident, tbl, reg.SortOrder)
	}
	if errors.Is(err, icecatalog.ErrNoSuchTable) {
		return createRESTGoTable(ctx, cat, ident, req, reg, basePrefix)
	}

	tableLoc := restGoTableLocation(req.DatasetS3.Bucket, basePrefix)
	if !isBrokenRESTMetadataErr(err, tableLoc) {
		return nil, err
	}

	log.Warn("iceberg registration found broken table metadata; recreating table",
		slog.String("run_id", req.RunID),
		slog.String("table", table),
		slog.String("location", tableLoc),
	)
	if dropErr := cat.DropTable(ctx, ident); dropErr != nil && !errors.Is(dropErr, icecatalog.ErrNoSuchTable) {
		return nil, fmt.Errorf("drop broken iceberg table %s: %w", table, dropErr)
	}
	return createRESTGoTable(ctx, cat, ident, req, reg, basePrefix)
}

func createRESTGoTable(ctx context.Context, cat *restcatalog.Catalog, ident icetable.Identifier, req RunRequest, reg RunConfig, basePrefix string) (*icetable.Table, error) {
	iceSchema, err := inferRunIcebergSchema(ctx, req, strings.Join(ident, "."))
	if err != nil {
		return nil, err
	}

	if reg.Upsert.Enabled {
		iceSchema, err = schemaWithIdentifierFields(iceSchema, reg.Upsert.Keys)
		if err != nil {
			return nil, err
		}
	}
	spec, err := buildPartitionSpec(iceSchema, reg.PartitionSpec)
	if err != nil {
		return nil, err
	}
	order, err := buildSortOrder(iceSchema, reg.SortOrder, icetable.InitialSortOrderID)
	if err != nil {
		return nil, err
	}
	props := tableOptionProperties(reg)
	props["format-version"] = "2"
	loc := restGoTableLocation(req.DatasetS3.Bucket, basePrefix)
	return cat.CreateTable(ctx, ident, iceSchema,
		icecatalog.WithLocation(loc),
		icecatalog.WithPartitionSpec(&spec),
		icecatalog.WithSortOrder(order),
		icecatalog.WithProperties(props),
	)
}

func inferRunIcebergSchema(ctx context.Context, req RunRequest, tableName string) (*iceberg.Schema, error) {
	mode := normalizedRunRequestSourceMode(req.SourceMode)
	if mode == "query" {
		if !connectors.SupportsQueryMode(req.SourceEngine) {
			engine := connectors.NormalizeSourceEngine(req.SourceEngine)
			if engine == "" {
				engine = strings.TrimSpace(req.SourceEngine)
			}
			return nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: query mode is not supported for %s", engine)
		}
	} else if !connectors.SupportsOrderedCursor(req.SourceEngine) && !connectors.SupportsDocumentReader(req.SourceEngine) {
		return nil, fmt.Errorf("auto-create Iceberg table is only implemented for SQL ordered-cursor and document sources; create %s before running registration", tableName)
	}

	var iceSchema *iceberg.Schema
	if len(req.DurableIcebergSchema) > 0 {
		var persisted iceberg.Schema
		if err := json.Unmarshal(req.DurableIcebergSchema, &persisted); err != nil {
			return nil, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("invalid durable source schema: %w", err))
		}
		if len(persisted.Fields()) == 0 {
			return nil, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("durable source schema has no fields"))
		}
		iceSchema = &persisted
	}

	var arrSchema *arrow.Schema

	if iceSchema == nil && connectors.SupportsDocumentReader(req.SourceEngine) {
		db, err := connectors.OpenDocumentReader(ctx, req.SourceEngine, req.SourceDSN)
		if err != nil {
			return nil, fmt.Errorf("open document source for iceberg auto-create: %w", err)
		}
		defer db.Close()

		it, err := db.StreamDocuments(ctx, req.SourceTable, documentFilter(req.SourceEngine, req.RecordPath, req.FileFormat), 1000)
		if err != nil {
			return nil, fmt.Errorf("stream documents for iceberg auto-create: %w", err)
		}
		defer it.Close()
		var fieldOrder []string
		if ordered, ok := it.(connectors.OrderedDocumentIterator); ok {
			fieldOrder = ordered.FieldOrder()
		}

		var docBuf []map[string]any
		for it.Next(ctx) {
			doc, err := it.Decode()
			if err != nil {
				return nil, fmt.Errorf("decode document for iceberg auto-create: %w", err)
			}
			docBuf = append(docBuf, doc)
			if len(docBuf) >= 1000 {
				break
			}
		}

		arrSchema, err = arrowio.InferMongoSchemaWithFieldOrder(docBuf, fieldOrder)
		if err != nil {
			return nil, fmt.Errorf("infer document schema for iceberg auto-create: %w", err)
		}
	} else if iceSchema == nil {
		db, err := connectors.OpenIntRangeReader(ctx, req.SourceEngine, req.SourceDSN)
		if err != nil {
			return nil, fmt.Errorf("open source for iceberg auto-create: %w", err)
		}
		defer db.Close()

		cols, colTypes, err := describeSourceSchemaForAutoCreate(ctx, db, req)
		if err != nil {
			return nil, err
		}
		_, arrSchema, err = arrowio.PlansFromSQLEngineWithOverrides(req.SourceEngine, cols, colTypes, req.ColumnTypes)
		if err != nil {
			return nil, fmt.Errorf("sql->arrow schema: %w", err)
		}
	}

	if iceSchema == nil {
		var err error
		iceSchema, err = icetable.ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
		if err != nil {
			return nil, fmt.Errorf("arrow->iceberg schema: %w", err)
		}
	}

	return iceSchema, nil
}

// InferDurableIcebergSchema snapshots the source/query schema in Iceberg's
// stable JSON representation before a zero-artifact run enters its durable
// commit boundary.
func InferDurableIcebergSchema(ctx context.Context, engine, dsn, mode, table, query, recordPath, fileFormat string) (json.RawMessage, error) {
	if connectors.SupportsDocumentReader(engine) {
		reader, err := connectors.OpenDocumentReader(ctx, engine, dsn)
		if err != nil {
			return nil, fmt.Errorf("open document source for durable schema: %w", err)
		}
		defer reader.Close()
		it, err := reader.StreamDocuments(ctx, table, documentFilter(engine, recordPath, fileFormat), 1000)
		if err != nil {
			return nil, fmt.Errorf("stream documents for durable schema: %w", err)
		}
		defer it.Close()
		var fieldOrder []string
		if ordered, ok := it.(connectors.OrderedDocumentIterator); ok {
			fieldOrder = ordered.FieldOrder()
		}
		var docs []map[string]any
		for it.Next(ctx) && len(docs) < 1000 {
			doc, decodeErr := it.Decode()
			if decodeErr != nil {
				return nil, fmt.Errorf("decode document for durable schema: %w", decodeErr)
			}
			docs = append(docs, doc)
		}
		arrSchema, err := arrowio.InferMongoSchemaWithFieldOrder(docs, fieldOrder)
		if err != nil {
			return nil, fmt.Errorf("infer document durable schema: %w", err)
		}
		iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
		if err != nil {
			return nil, fmt.Errorf("arrow->iceberg durable schema: %w", err)
		}
		return json.Marshal(iceSchema)
	}
	reader, err := connectors.OpenIntRangeReader(ctx, engine, dsn)
	if err != nil {
		return nil, fmt.Errorf("open source for durable schema: %w", err)
	}
	defer reader.Close()
	req := RunRequest{SourceEngine: engine, SourceMode: mode, SourceTable: table, SourceQuery: query}
	cols, columnTypes, err := describeSourceSchemaForAutoCreate(ctx, reader, req)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("durable source schema has no columns")
	}
	_, arrSchema, err := arrowio.PlansFromSQLEngine(engine, cols, columnTypes)
	if err != nil {
		return nil, fmt.Errorf("sql->arrow durable schema: %w", err)
	}
	iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	if err != nil {
		return nil, fmt.Errorf("arrow->iceberg durable schema: %w", err)
	}
	return json.Marshal(iceSchema)
}

func normalizedRunRequestSourceMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "table":
		return "table"
	case "query", "sql":
		return "query"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

func describeSourceSchemaForAutoCreate(ctx context.Context, reader connectors.TableReader, req RunRequest) ([]string, []*sql.ColumnType, error) {
	mode := normalizedRunRequestSourceMode(req.SourceMode)
	if mode == "query" {
		query := strings.TrimSpace(req.SourceQuery)
		if query == "" {
			return nil, nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: source query is empty")
		}
		queryReader, ok := reader.(connectors.SourceQueryReader)
		if !ok {
			engine := connectors.NormalizeSourceEngine(req.SourceEngine)
			if engine == "" {
				engine = strings.TrimSpace(req.SourceEngine)
			}
			return nil, nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: query mode is not supported for %s", engine)
		}
		cols, colTypes, err := queryReader.DescribeQuery(ctx, query)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: describe query result: %w", err)
		}
		if len(cols) == 0 {
			return nil, nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: query returned no columns")
		}
		return cols, colTypes, nil
	}

	cols, colTypes, err := reader.DescribeTable(ctx, req.SourceTable)
	if err != nil {
		return nil, nil, fmt.Errorf("describe table for iceberg auto-create: %w", err)
	}
	if len(req.SelectColumns) > 0 {
		selMap := make(map[string]int)
		for idx, c := range cols {
			selMap[strings.ToLower(c)] = idx
		}
		filteredCols := make([]string, 0, len(req.SelectColumns))
		filteredColTypes := make([]*sql.ColumnType, 0, len(req.SelectColumns))
		for _, sc := range req.SelectColumns {
			scClean := strings.TrimSpace(sc)
			if idx, ok := selMap[strings.ToLower(scClean)]; ok {
				filteredCols = append(filteredCols, cols[idx])
				filteredColTypes = append(filteredColTypes, colTypes[idx])
			}
		}
		if len(filteredCols) > 0 {
			return filteredCols, filteredColTypes, nil
		}
	}
	return cols, colTypes, nil
}

func restGoTableLocation(bucket, basePrefix string) string {
	return fmt.Sprintf("s3://%s/%s", strings.TrimSpace(bucket), strings.Trim(strings.TrimSpace(basePrefix), "/"))
}

func isBrokenRESTMetadataErr(err error, tableLoc string) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if !strings.Contains(msg, "location does not exist:") || !strings.Contains(msg, "/metadata/") {
		return false
	}
	tableLoc = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(tableLoc), "/"))
	if tableLoc == "" {
		return false
	}
	return strings.Contains(msg, tableLoc+"/metadata/")
}
