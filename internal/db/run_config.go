package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// RunConfig is the non-secret configuration a run was planned with: its job
// (including the planner's tuned options) and the source engine and target
// metadata of its connections. It is captured when the run's tasks are
// created, so later edits to the job or connections do not change where or
// how an in-flight run writes and commits. Credentials are not part of it;
// they are always read from the connections so rotation takes effect.
type RunConfig struct {
	Job            Job             `json:"job"`
	SourceEngine   string          `json:"source_engine"`
	TargetMetadata json.RawMessage `json:"target_metadata"`
}

// snapshotRunConfigTx records the run's configuration as it is now.
func snapshotRunConfigTx(ctx context.Context, tx *sql.Tx, runID string) error {
	var cfg RunConfig
	var options, targetMeta string
	var inc int
	err := tx.QueryRowContext(ctx, `SELECT j.id, j.name, j.source_connection_id, j.target_connection_id, j.source_sql, j.target_namespace, j.target_table, j.write_mode, j.incremental, j.hwm_column, j.options_json, j.created_at, j.updated_at, src.engine, tgt.metadata_json
		FROM runs r JOIN jobs j ON j.id=r.job_id
		JOIN connections src ON src.id=j.source_connection_id
		JOIN connections tgt ON tgt.id=j.target_connection_id
		WHERE r.id=?`, runID).Scan(&cfg.Job.ID, &cfg.Job.Name, &cfg.Job.SourceConnectionID, &cfg.Job.TargetConnectionID, &cfg.Job.SourceSQL, &cfg.Job.TargetNamespace, &cfg.Job.TargetTable, &cfg.Job.WriteMode, &inc, &cfg.Job.HWMColumn, &options, &cfg.Job.CreatedAt, &cfg.Job.UpdatedAt, &cfg.SourceEngine, &targetMeta)
	if err != nil {
		return fmt.Errorf("snapshot run configuration: %w", err)
	}
	cfg.Job.Incremental = inc != 0
	cfg.Job.OptionsJSON = json.RawMessage(options)
	cfg.TargetMetadata = json.RawMessage(targetMeta)
	b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE runs SET config_snapshot_json=? WHERE id=?`, string(b), runID)
	return err
}

// RunConfig returns the configuration run was planned with. Runs created
// before snapshots existed fall back to the current job and connections.
func (s *Store) RunConfig(ctx context.Context, run Run) (RunConfig, error) {
	if len(run.ConfigSnapshotJSON) > 0 {
		var cfg RunConfig
		if err := json.Unmarshal(run.ConfigSnapshotJSON, &cfg); err != nil {
			return RunConfig{}, fmt.Errorf("run %s configuration snapshot is invalid: %w", run.ID, err)
		}
		return cfg, nil
	}
	job, err := s.GetJob(ctx, run.JobID)
	if err != nil {
		return RunConfig{}, err
	}
	src, err := s.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		return RunConfig{}, err
	}
	tgt, err := s.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return RunConfig{}, err
	}
	return RunConfig{Job: job, SourceEngine: src.Engine, TargetMetadata: tgt.MetadataJSON}, nil
}
