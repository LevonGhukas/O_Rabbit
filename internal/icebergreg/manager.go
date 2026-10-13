package icebergreg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"reflect"
	"sort"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	_ "github.com/apache/iceberg-go/io/gocloud"
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

	SourceEngine  string
	SourceDSN     string
	SourceMode    string
	SourceTable   string
	SourceQuery   string
	ColumnTypes   map[string]string
	RecordPath    string
	FileFormat    string
	SelectColumns []string
	QueryHash     string
	Incremental   bool
	WriteMode     string

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
