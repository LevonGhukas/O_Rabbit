package orabbitcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"

	"gopkg.in/yaml.v3"
)

var ErrRunSubmitSpecInvalid = errors.New("invalid run submit config")

type runSubmitFile struct {
	Master runSubmitMasterSpec `json:"master" yaml:"master"`
	Source runSubmitSourceSpec `json:"source" yaml:"source"`
	Target runSubmitTargetSpec `json:"target" yaml:"target"`
	Job    runSubmitJobSpec    `json:"job" yaml:"job"`
}

type runSubmitMasterSpec struct {
	HTTP string `json:"http" yaml:"http"`
}

type runSubmitSourceSpec struct {
	Name   string `json:"name" yaml:"name"`
	Engine string `json:"engine" yaml:"engine"`
	DSN    string `json:"dsn" yaml:"dsn"`
	SQL    string `json:"sql,omitempty" yaml:"sql,omitempty"`
}

type runSubmitTargetSpec struct {
	Name            string `json:"name" yaml:"name"`
	Endpoint        string `json:"endpoint" yaml:"endpoint"`
	Region          string `json:"region" yaml:"region"`
	Bucket          string `json:"bucket" yaml:"bucket"`
	Prefix          string `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	ForcePathStyle  bool   `json:"force_path_style" yaml:"force_path_style"`
	AccessKeyID     string `json:"access_key_id" yaml:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key" yaml:"secret_access_key"`
}

type runSubmitJobSpec struct {
	Name              string `json:"name" yaml:"name"`
	TargetNamespace   string `json:"target_namespace" yaml:"target_namespace"`
	TargetTable       string `json:"target_table" yaml:"target_table"`
	WriteMode         string `json:"write_mode" yaml:"write_mode"`
	Incremental       *bool  `json:"incremental" yaml:"incremental"`
	Table             string `json:"table" yaml:"table"`
	IDColumn          string `json:"id_column,omitempty" yaml:"id_column,omitempty"`
	AutoTune          *bool  `json:"auto_tune" yaml:"auto_tune"`
	MaxInFlightTasks  int    `json:"max_in_flight_tasks,omitempty" yaml:"max_in_flight_tasks,omitempty"`
	TargetRowsPerTask int64  `json:"target_rows_per_task,omitempty" yaml:"target_rows_per_task,omitempty"`
	PlannedTasks      int    `json:"planned_tasks,omitempty" yaml:"planned_tasks,omitempty"`
	ChunkSize         int64  `json:"chunk_size,omitempty" yaml:"chunk_size,omitempty"`
	FetchLimit        int    `json:"fetch_limit,omitempty" yaml:"fetch_limit,omitempty"`
}

func invalidRunSubmitf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrRunSubmitSpecInvalid, fmt.Sprintf(format, args...))
}

func loadRunSubmitFile(path string) (runSubmitFile, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return runSubmitFile{}, invalidRunSubmitf("--file is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return runSubmitFile{}, invalidRunSubmitf("read %q: %v", path, err)
	}

	var spec runSubmitFile
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		if err := decodeRunSubmitJSON(raw, &spec); err != nil {
			return runSubmitFile{}, invalidRunSubmitf("parse %q as JSON: %v", path, err)
		}
	case ".yaml", ".yml":
		if err := decodeRunSubmitYAML(raw, &spec); err != nil {
			return runSubmitFile{}, invalidRunSubmitf("parse %q as YAML: %v", path, err)
		}
	default:
		return runSubmitFile{}, invalidRunSubmitf("unsupported file extension %q for %q; use .json, .yaml, or .yml", filepath.Ext(path), path)
	}

	if err := spec.validate(); err != nil {
		return runSubmitFile{}, err
	}
	return spec, nil
}

func decodeRunSubmitJSON(raw []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func decodeRunSubmitYAML(raw []byte, out any) error {
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	return dec.Decode(out)
}

func (s *runSubmitFile) validate() error {
	if s == nil {
		return invalidRunSubmitf("empty spec")
	}

	s.Master.HTTP = strings.TrimSpace(s.Master.HTTP)
	if s.Master.HTTP != "" {
		s.Master.HTTP = normalizeHTTPBase(s.Master.HTTP)
	}

	s.Source.Name = strings.TrimSpace(s.Source.Name)
	s.Source.Engine = connectors.NormalizeSourceEngine(s.Source.Engine)
	s.Source.DSN = strings.TrimSpace(s.Source.DSN)
	s.Source.SQL = strings.TrimSpace(s.Source.SQL)

	s.Target.Name = strings.TrimSpace(s.Target.Name)
	s.Target.Endpoint = strings.TrimSpace(s.Target.Endpoint)
	s.Target.Region = strings.TrimSpace(s.Target.Region)
	s.Target.Bucket = strings.TrimSpace(s.Target.Bucket)
	s.Target.Prefix = strings.TrimSpace(s.Target.Prefix)
	s.Target.AccessKeyID = strings.TrimSpace(s.Target.AccessKeyID)
	s.Target.SecretAccessKey = strings.TrimSpace(s.Target.SecretAccessKey)

	s.Job.Name = strings.TrimSpace(s.Job.Name)
	s.Job.TargetNamespace = strings.TrimSpace(s.Job.TargetNamespace)
	s.Job.TargetTable = strings.TrimSpace(s.Job.TargetTable)
	s.Job.WriteMode = strings.TrimSpace(s.Job.WriteMode)
	s.Job.Table = strings.TrimSpace(s.Job.Table)
	s.Job.IDColumn = strings.TrimSpace(s.Job.IDColumn)

	if s.Source.Name == "" {
		return invalidRunSubmitf("source.name is required")
	}
	if s.Source.Engine == "" {
		return invalidRunSubmitf("source.engine is required")
	}
	if !connectors.IsKnownSourceEngine(s.Source.Engine) {
		return invalidRunSubmitf("source.engine %q is not supported in this build (known: %s)", s.Source.Engine, strings.Join(connectors.KnownSourceEngines(), ", "))
	}
	if s.Source.DSN == "" {
		return invalidRunSubmitf("source.dsn is required")
	}

	if s.Target.Name == "" {
		return invalidRunSubmitf("target.name is required")
	}
	if s.Target.Endpoint == "" {
		return invalidRunSubmitf("target.endpoint is required")
	}
	if s.Target.Region == "" {
		return invalidRunSubmitf("target.region is required")
	}
	if s.Target.Bucket == "" {
		return invalidRunSubmitf("target.bucket is required")
	}
	if s.Target.AccessKeyID == "" {
		return invalidRunSubmitf("target.access_key_id is required")
	}
	if s.Target.SecretAccessKey == "" {
		return invalidRunSubmitf("target.secret_access_key is required")
	}

	if s.Job.Name == "" {
		return invalidRunSubmitf("job.name is required")
	}
	if s.Job.TargetNamespace == "" {
		return invalidRunSubmitf("job.target_namespace is required")
	}
	if s.Job.TargetTable == "" {
		return invalidRunSubmitf("job.target_table is required")
	}
	if s.Job.WriteMode == "" {
		return invalidRunSubmitf("job.write_mode is required")
	}
	if s.Job.Table == "" {
		return invalidRunSubmitf("job.table is required")
	}
	if s.Job.Incremental == nil {
		return invalidRunSubmitf("job.incremental is required")
	}
	if s.Job.AutoTune == nil {
		return invalidRunSubmitf("job.auto_tune is required")
	}
	if s.Job.MaxInFlightTasks < 0 {
		return invalidRunSubmitf("job.max_in_flight_tasks must be >= 0")
	}
	if s.Job.TargetRowsPerTask < 0 {
		return invalidRunSubmitf("job.target_rows_per_task must be >= 0")
	}
	if s.Job.PlannedTasks < 0 {
		return invalidRunSubmitf("job.planned_tasks must be >= 0")
	}
	if s.Job.ChunkSize < 0 {
		return invalidRunSubmitf("job.chunk_size must be >= 0")
	}
	if s.Job.FetchLimit < 0 {
		return invalidRunSubmitf("job.fetch_limit must be >= 0")
	}

	switch {
	case s.Source.Engine == "flightsql":
		if s.Source.SQL == "" {
			return invalidRunSubmitf("source.sql is required for FlightSQL submissions")
		}
		if *s.Job.Incremental {
			return invalidRunSubmitf("job.incremental=false is required for FlightSQL submissions")
		}
		if s.Job.IDColumn != "" {
			return invalidRunSubmitf("job.id_column is not supported for FlightSQL submissions")
		}
		if *s.Job.AutoTune {
			return invalidRunSubmitf("job.auto_tune=false is required for FlightSQL submissions")
		}
		if s.Job.MaxInFlightTasks > 0 || s.Job.TargetRowsPerTask > 0 || s.Job.PlannedTasks > 0 || s.Job.ChunkSize > 0 || s.Job.FetchLimit > 0 {
			return invalidRunSubmitf("FlightSQL submissions do not accept manual planning fields; keep max_in_flight_tasks, target_rows_per_task, planned_tasks, chunk_size, and fetch_limit unset or zero")
		}
	case connectors.SupportsOrderedCursor(s.Source.Engine):
		if s.Source.SQL != "" {
			return invalidRunSubmitf("source.sql is not supported for %s run submit; use job.table and job.id_column instead", s.Source.Engine)
		}
		if s.Job.IDColumn == "" {
			return invalidRunSubmitf("job.id_column is required for %s submissions", s.Source.Engine)
		}
		if *s.Job.AutoTune {
			if s.Job.PlannedTasks > 0 || s.Job.ChunkSize > 0 || s.Job.FetchLimit > 0 {
				return invalidRunSubmitf("manual planning fields planned_tasks, chunk_size, and fetch_limit must be omitted when job.auto_tune=true")
			}
		} else {
			if s.Job.MaxInFlightTasks < 1 {
				return invalidRunSubmitf("job.max_in_flight_tasks must be >= 1 when job.auto_tune=false")
			}
			if s.Job.FetchLimit < 1 {
				return invalidRunSubmitf("job.fetch_limit must be >= 1 when job.auto_tune=false")
			}
			if s.Job.PlannedTasks < 1 && s.Job.ChunkSize < 1 {
				return invalidRunSubmitf("job.planned_tasks or job.chunk_size must be set when job.auto_tune=false")
			}
		}
	default:
		return invalidRunSubmitf("source.engine %q is known but not yet supported by run submit in this build", s.Source.Engine)
	}

	return nil
}

func (s runSubmitFile) toRanConfig(masterHTTPOverride string) ranConfig {
	base := strings.TrimSpace(masterHTTPOverride)
	if base == "" {
		base = s.Master.HTTP
	}
	if base == "" {
		base = localHTTPBase(defaultHTTPAddr)
	}

	return ranConfig{
		HTTPBase:          normalizeHTTPBase(base),
		StartStack:        false,
		StartLocalWorkers: false,
		Workers:           0,

		AutoIceberg: false,

		SourceName:   s.Source.Name,
		SourceEngine: s.Source.Engine,
		SourceDSN:    s.Source.DSN,
		SourceSQL:    s.Source.SQL,

		TargetName:        s.Target.Name,
		S3Endpoint:        s.Target.Endpoint,
		S3Region:          s.Target.Region,
		S3Bucket:          s.Target.Bucket,
		S3Prefix:          s.Target.Prefix,
		S3ForcePathStyle:  s.Target.ForcePathStyle,
		S3AccessKeyID:     s.Target.AccessKeyID,
		S3SecretAccessKey: s.Target.SecretAccessKey,

		JobName:         s.Job.Name,
		TargetNamespace: s.Job.TargetNamespace,
		TargetTable:     s.Job.TargetTable,
		WriteMode:       s.Job.WriteMode,
		Incremental:     *s.Job.Incremental,
		Table:           s.Job.Table,
		IDColumn:        s.Job.IDColumn,
		PlannedTasks:    s.Job.PlannedTasks,
		ChunkSize:       s.Job.ChunkSize,
		FetchLimit:      s.Job.FetchLimit,

		AutoTune:          *s.Job.AutoTune,
		MaxInFlightTasks:  s.Job.MaxInFlightTasks,
		TargetRowsPerTask: s.Job.TargetRowsPerTask,
	}
}

func validateRunConfigForSubmission(cfg ranConfig) error {
	engine := connectors.NormalizeSourceEngine(cfg.SourceEngine)

	if strings.TrimSpace(cfg.HTTPBase) == "" {
		return invalidRunSubmitf("master HTTP base URL is required")
	}
	if strings.TrimSpace(cfg.SourceName) == "" {
		return invalidRunSubmitf("source.name is required")
	}
	if engine == "" {
		return invalidRunSubmitf("source.engine is required")
	}
	if !connectors.IsKnownSourceEngine(engine) {
		return invalidRunSubmitf("source.engine %q is not supported in this build (known: %s)", engine, strings.Join(connectors.KnownSourceEngines(), ", "))
	}
	if strings.TrimSpace(cfg.SourceDSN) == "" {
		return invalidRunSubmitf("source.dsn is required")
	}
	if strings.TrimSpace(cfg.TargetName) == "" {
		return invalidRunSubmitf("target.name is required")
	}
	if strings.TrimSpace(cfg.S3Endpoint) == "" || strings.TrimSpace(cfg.S3Region) == "" || strings.TrimSpace(cfg.S3Bucket) == "" {
		return invalidRunSubmitf("target.endpoint, target.region, and target.bucket are required")
	}
	if strings.TrimSpace(cfg.S3AccessKeyID) == "" || strings.TrimSpace(cfg.S3SecretAccessKey) == "" {
		return invalidRunSubmitf("target.access_key_id and target.secret_access_key are required")
	}
	if strings.TrimSpace(cfg.JobName) == "" {
		return invalidRunSubmitf("job.name is required")
	}
	if strings.TrimSpace(cfg.TargetNamespace) == "" || strings.TrimSpace(cfg.TargetTable) == "" {
		return invalidRunSubmitf("job.target_namespace and job.target_table are required")
	}
	if strings.TrimSpace(cfg.WriteMode) == "" {
		return invalidRunSubmitf("job.write_mode is required")
	}
	if strings.TrimSpace(cfg.Table) == "" {
		return invalidRunSubmitf("job.table is required")
	}

	switch {
	case engine == "flightsql":
		if strings.TrimSpace(cfg.SourceSQL) == "" {
			return invalidRunSubmitf("source.sql is required for FlightSQL submissions")
		}
		if cfg.Incremental {
			return invalidRunSubmitf("job.incremental=false is required for FlightSQL submissions")
		}
		if strings.TrimSpace(cfg.IDColumn) != "" {
			return invalidRunSubmitf("job.id_column is not supported for FlightSQL submissions")
		}
		if cfg.AutoTune {
			return invalidRunSubmitf("job.auto_tune=false is required for FlightSQL submissions")
		}
		if cfg.MaxInFlightTasks > 0 || cfg.TargetRowsPerTask > 0 || cfg.PlannedTasks > 0 || cfg.ChunkSize > 0 || cfg.FetchLimit > 0 {
			return invalidRunSubmitf("FlightSQL submissions do not accept manual planning fields; keep them zero")
		}
	case connectors.SupportsOrderedCursor(engine):
		if strings.TrimSpace(cfg.SourceSQL) != "" {
			return invalidRunSubmitf("source.sql is not supported for %s run submit; use job.table and job.id_column instead", engine)
		}
		if strings.TrimSpace(cfg.IDColumn) == "" {
			return invalidRunSubmitf("job.id_column is required for %s submissions", engine)
		}
		if cfg.MaxInFlightTasks < 0 || cfg.TargetRowsPerTask < 0 || cfg.PlannedTasks < 0 || cfg.ChunkSize < 0 || cfg.FetchLimit < 0 {
			return invalidRunSubmitf("planning values must be >= 0")
		}
		if cfg.AutoTune {
			if cfg.PlannedTasks > 0 || cfg.ChunkSize > 0 || cfg.FetchLimit > 0 {
				return invalidRunSubmitf("manual planning fields planned_tasks, chunk_size, and fetch_limit must be omitted when job.auto_tune=true")
			}
		} else {
			if cfg.MaxInFlightTasks < 1 {
				return invalidRunSubmitf("job.max_in_flight_tasks must be >= 1 when job.auto_tune=false")
			}
			if cfg.FetchLimit < 1 {
				return invalidRunSubmitf("job.fetch_limit must be >= 1 when job.auto_tune=false")
			}
			if cfg.PlannedTasks < 1 && cfg.ChunkSize < 1 {
				return invalidRunSubmitf("job.planned_tasks or job.chunk_size must be set when job.auto_tune=false")
			}
		}
	default:
		return invalidRunSubmitf("source.engine %q is known but not yet supported by run submit in this build", engine)
	}

	return nil
}

func buildJobPayloadFromConfig(cfg ranConfig, sourceConnectionID, targetConnectionID string) (jobPayload, error) {
	engine := connectors.NormalizeSourceEngine(cfg.SourceEngine)
	if err := validateRunConfigForSubmission(cfg); err != nil {
		return jobPayload{}, err
	}

	optionsJSON := map[string]any{
		"table": cfg.Table,
	}
	sourceSQL := strings.TrimSpace(cfg.SourceSQL)
	partitionStrategy := "single"
	incremental := false
	hwmColumn := ""

	switch {
	case engine == "flightsql":
		optionsJSON["auto_tune"] = false
		optionsJSON["max_in_flight_tasks"] = 1
	case connectors.SupportsOrderedCursor(engine):
		partitionStrategy = "ordered_cursor"
		incremental = cfg.Incremental
		hwmColumn = cfg.IDColumn
		optionsJSON["auto_tune"] = cfg.AutoTune
		optionsJSON["max_in_flight_tasks"] = cfg.MaxInFlightTasks
		optionsJSON["target_rows_per_task"] = cfg.TargetRowsPerTask
		optionsJSON["cursor_column"] = cfg.IDColumn
		optionsJSON["id_column"] = cfg.IDColumn
		optionsJSON["planned_tasks"] = cfg.PlannedTasks
		optionsJSON["chunk_size"] = cfg.ChunkSize
		optionsJSON["fetch_limit_rows"] = cfg.FetchLimit
	default:
		return jobPayload{}, invalidRunSubmitf("source.engine %q is known but not yet supported by run submit in this build", engine)
	}
	optionsJSON["partition_strategy"] = partitionStrategy
	optionsJSON = icebergreg.MergeJobConfig(optionsJSON, icebergreg.JobConfig{
		Enabled: cfg.AutoIceberg,
		Engine:  cfg.IcebergEngine,
		Table:   effectiveIceTable(cfg),
	})

	return jobPayload{
		Name:               cfg.JobName,
		SourceConnectionID: sourceConnectionID,
		TargetConnectionID: targetConnectionID,
		SourceSQL:          sourceSQL,
		TargetNamespace:    cfg.TargetNamespace,
		TargetTable:        cfg.TargetTable,
		WriteMode:          cfg.WriteMode,
		Incremental:        incremental,
		HWMColumn:          hwmColumn,
		OptionsJSON:        optionsJSON,
	}, nil
}

func prepareRunPlan(ctx context.Context, cfg ranConfig) (string, error) {
	if err := validateRunConfigForSubmission(cfg); err != nil {
		return "", err
	}

	srcID, err := upsertConnection(ctx, cfg.HTTPBase, connectionPayload{
		Name:     cfg.SourceName,
		Kind:     "source",
		Engine:   cfg.SourceEngine,
		Metadata: map[string]any{},
		Secret:   map[string]any{"dsn": cfg.SourceDSN},
	})
	if err != nil {
		return "", err
	}

	tgtID, err := upsertConnection(ctx, cfg.HTTPBase, connectionPayload{
		Name:   cfg.TargetName,
		Kind:   "target",
		Engine: "s3",
		Metadata: map[string]any{
			"endpoint":         cfg.S3Endpoint,
			"region":           cfg.S3Region,
			"bucket":           cfg.S3Bucket,
			"prefix":           cfg.S3Prefix,
			"force_path_style": cfg.S3ForcePathStyle,
		},
		Secret: map[string]any{
			"access_key_id":     cfg.S3AccessKeyID,
			"secret_access_key": cfg.S3SecretAccessKey,
		},
	})
	if err != nil {
		return "", err
	}

	jp, err := buildJobPayloadFromConfig(cfg, srcID, tgtID)
	if err != nil {
		return "", err
	}
	if cfg.AutoIceberg {
		_, iceCfg, err := readIceConfig(cfg.IceConfig)
		if err != nil {
			return "", err
		}
		if iceCfg.TargetFileSize > 0 {
			jp.OptionsJSON["target_file_bytes"] = iceCfg.TargetFileSize
		}
	}

	return upsertJob(ctx, cfg.HTTPBase, jp)
}

func submitRunPlan(ctx context.Context, cfg ranConfig) (jobID, runID string, taskCount int, err error) {
	jobID, err = prepareRunPlan(ctx, cfg)
	if err != nil {
		return "", "", 0, err
	}
	registrationConfig, err := buildIcebergRegistrationSnapshot(cfg)
	if err != nil {
		return "", "", 0, err
	}
	runID, taskCount, err = startRun(ctx, cfg.HTTPBase, jobID, registrationConfig)
	if err != nil {
		return "", "", 0, err
	}
	return jobID, runID, taskCount, nil
}
