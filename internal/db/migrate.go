package db

import (
	"context"
	"database/sql"
	"fmt"
)

type migration struct {
	version int
	sql     string
	apply   func(context.Context, migrationExecutor) error
}

type migrationExecutor interface {
	queryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

var migrations = []migration{
	{version: 1, sql: schemaV1},
	{version: 2, sql: schemaV2},
	{version: 3, sql: schemaV3},
	{version: 4, sql: schemaV4},
	{version: 5, apply: ensureRunRegistrationConfigColumn},
	{version: 6, sql: schemaV6},
	{version: 7, sql: schemaV7},
	{version: 8, sql: schemaV8},
	{version: 9, sql: schemaV9},
	{version: 10, sql: schemaV10},
	{version: 11, sql: schemaV11},
	{version: 12, sql: schemaV12},
	{version: 13, sql: schemaV13},
	{version: 14, sql: schemaV14},
	{version: 15, sql: schemaV15},
	{version: 16, sql: schemaV16},
	{version: 17, sql: schemaV17},
	{version: 18, sql: schemaV18},
	{version: 19, sql: schemaV19},
	{version: 20, sql: schemaV20},
	{version: 21, sql: schemaV21},
	{version: 22, sql: schemaV22},
}

const schemaV21 = `
ALTER TABLE iceberg_registrations ADD COLUMN retry_override_config_json TEXT NOT NULL DEFAULT '';
`

const schemaV22 = `
ALTER TABLE iceberg_registrations ADD COLUMN manual_retry_budget INTEGER NOT NULL DEFAULT 0;
`

const schemaV20 = `
ALTER TABLE runs ADD COLUMN commit_reconciliation_status TEXT NOT NULL DEFAULT '';
ALTER TABLE runs ADD COLUMN commit_reconciliation_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN commit_reconciliation_next_eligible_at TEXT;
ALTER TABLE runs ADD COLUMN operator_action_required INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_runs_commit_reconciliation_eligible
  ON runs(status,commit_reconciliation_status,commit_reconciliation_next_eligible_at,started_at);
`

const schemaV19 = `
CREATE TABLE IF NOT EXISTS worker_instances (
  boot_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  hostname TEXT NOT NULL,
  pid INTEGER NOT NULL,
  version TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  last_heartbeat TEXT NOT NULL,
  FOREIGN KEY(worker_id) REFERENCES workers(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_worker_instances_worker_id ON worker_instances(worker_id);
ALTER TABLE task_attempts ADD COLUMN worker_boot_id TEXT NOT NULL DEFAULT '';
`

const schemaV18 = `
ALTER TABLE runs ADD COLUMN failure_class TEXT NOT NULL DEFAULT '';
`

const schemaV17 = `
CREATE TABLE IF NOT EXISTS upload_capacity_leases (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL UNIQUE,
  worker_id TEXT NOT NULL,
  lease_token TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK(status IN ('ACTIVE','RELEASED','EXPIRED')),
  lease_deadline TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  released_at TEXT,
  FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
  FOREIGN KEY(attempt_id) REFERENCES task_attempts(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_upload_capacity_active ON upload_capacity_leases(status,lease_deadline);
`

const schemaV16 = `
UPDATE iceberg_registrations
SET reconciliation_status='PENDING'
WHERE status='RECONCILING' AND reconciliation_status='';
`

const schemaV15 = `
CREATE TABLE canceled_object_candidates (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL DEFAULT '',
  dataset_id TEXT NOT NULL DEFAULT '',
  object_key TEXT NOT NULL,
  expected_size INTEGER NOT NULL,
  expected_sha256 TEXT NOT NULL,
  object_version TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL CHECK(status IN ('QUARANTINED','BLOCKED_REFERENCED','BLOCKED_AMBIGUOUS','OPERATOR_REVIEW','DELETE_PENDING','DELETING','DELETE_AMBIGUOUS','DELETED','DELETE_FAILED','CANCELED_CLEANUP')),
  eligibility_reason TEXT NOT NULL,
  reference_decision TEXT NOT NULL DEFAULT '',
  reference_evidence_digest TEXT NOT NULL DEFAULT '',
  discovered_at TEXT NOT NULL,
  quarantine_until TEXT NOT NULL,
  last_verified_at TEXT,
  delete_requested_at TEXT,
  deleted_at TEXT,
  operator_action_required INTEGER NOT NULL DEFAULT 0,
  dry_run_result TEXT NOT NULL DEFAULT '',
  delete_attempt_count INTEGER NOT NULL DEFAULT 0,
  current_attempt_id TEXT,
  last_error_class TEXT NOT NULL DEFAULT '',
  last_error_message TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
  FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
  FOREIGN KEY(attempt_id) REFERENCES task_attempts(id) ON DELETE CASCADE,
  UNIQUE(attempt_id,object_key,expected_sha256)
);
CREATE TABLE canceled_object_cleanup_attempts (
  id TEXT PRIMARY KEY,
  candidate_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  leader_epoch INTEGER,
  lease_deadline TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('ACTIVE','SUCCEEDED','FAILED','AMBIGUOUS','CANCELED','EXPIRED')),
  fencing_token TEXT NOT NULL UNIQUE,
  evidence_digest TEXT NOT NULL DEFAULT '',
  object_observation_identity TEXT NOT NULL DEFAULT '',
  started_at TEXT NOT NULL,
  finished_at TEXT,
  next_eligible_at TEXT,
  error_class TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(candidate_id) REFERENCES canceled_object_candidates(id) ON DELETE CASCADE,
  UNIQUE(candidate_id,attempt_number)
);
CREATE UNIQUE INDEX idx_canceled_object_one_active_attempt ON canceled_object_cleanup_attempts(candidate_id) WHERE status='ACTIVE';
CREATE INDEX idx_canceled_object_cleanup_eligible ON canceled_object_candidates(status,quarantine_until,updated_at);
`

const schemaV14 = `
CREATE TABLE multipart_uploads (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  file_index INTEGER NOT NULL,
  object_key TEXT NOT NULL,
  managed_prefix TEXT NOT NULL,
  provider_upload_id TEXT UNIQUE,
  status TEXT NOT NULL CHECK(status IN ('PREPARED','ACTIVE','COMPLETING','COMPLETION_AMBIGUOUS','COMPLETED','ABORT_PENDING','ABORTING','ABORTED','ABORT_FAILED','UNKNOWN_REVIEW')),
  worker_id TEXT NOT NULL,
  leader_epoch INTEGER,
  created_at TEXT NOT NULL,
  provider_created_at TEXT,
  last_activity_at TEXT NOT NULL,
  completion_started_at TEXT,
  completed_at TEXT,
  abort_requested_at TEXT,
  aborted_at TEXT,
  next_cleanup_at TEXT,
  cleanup_attempt_count INTEGER NOT NULL DEFAULT 0,
  cleanup_token TEXT,
  cleanup_lease_deadline TEXT,
  last_error_class TEXT NOT NULL DEFAULT '',
  last_error_message TEXT,
  object_sha256 TEXT NOT NULL,
  object_size INTEGER NOT NULL,
  reconciliation_status TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
  FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
  FOREIGN KEY(attempt_id) REFERENCES task_attempts(id) ON DELETE CASCADE,
  UNIQUE(attempt_id,file_index),
  UNIQUE(attempt_id,object_key)
);
CREATE INDEX idx_multipart_cleanup_eligible ON multipart_uploads(status,next_cleanup_at,last_activity_at);
CREATE INDEX idx_multipart_attempt ON multipart_uploads(task_id,attempt_id,status);
`

const schemaV13 = `
CREATE TABLE master_leadership (
  leadership_name TEXT PRIMARY KEY CHECK(leadership_name='master'),
  instance_id TEXT NOT NULL DEFAULT '',
  epoch INTEGER NOT NULL DEFAULT 0 CHECK(epoch >= 0),
  status TEXT NOT NULL DEFAULT 'RELEASED' CHECK(status IN ('ACTIVE','RELEASED')),
  lease_deadline_ms INTEGER NOT NULL DEFAULT 0,
  acquired_at_ms INTEGER NOT NULL DEFAULT 0,
  renewed_at_ms INTEGER NOT NULL DEFAULT 0,
  released_at_ms INTEGER NOT NULL DEFAULT 0,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);
INSERT INTO master_leadership(leadership_name) VALUES('master');
CREATE TABLE master_leadership_history (
  id TEXT PRIMARY KEY,
  instance_id TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  occurred_at_ms INTEGER NOT NULL,
  lease_deadline_ms INTEGER NOT NULL DEFAULT 0,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_master_leadership_history_epoch ON master_leadership_history(epoch,occurred_at_ms);
ALTER TABLE iceberg_registration_attempts ADD COLUMN leader_epoch INTEGER;
ALTER TABLE iceberg_reconciliation_attempts ADD COLUMN leader_epoch INTEGER;
ALTER TABLE task_attempts ADD COLUMN assigned_by_leader_epoch INTEGER;
`

const schemaV12 = `
ALTER TABLE iceberg_registrations ADD COLUMN reconciliation_status TEXT NOT NULL DEFAULT '';
ALTER TABLE iceberg_registrations ADD COLUMN reconciliation_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE iceberg_registrations ADD COLUMN current_reconciliation_attempt_id TEXT;
ALTER TABLE iceberg_registrations ADD COLUMN reconciliation_outcome TEXT NOT NULL DEFAULT '';
ALTER TABLE iceberg_registrations ADD COLUMN reconciliation_error_class TEXT NOT NULL DEFAULT '';
ALTER TABLE iceberg_registrations ADD COLUMN reconciliation_next_eligible_at TEXT;
ALTER TABLE iceberg_registrations ADD COLUMN observed_snapshot_id TEXT NOT NULL DEFAULT '';
ALTER TABLE iceberg_registrations ADD COLUMN observed_metadata_identity TEXT NOT NULL DEFAULT '';
ALTER TABLE iceberg_registrations ADD COLUMN matched_file_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE iceberg_registrations ADD COLUMN expected_file_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE iceberg_registrations ADD COLUMN reconciliation_evidence_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE iceberg_registrations ADD COLUMN ambiguity_retry_count INTEGER NOT NULL DEFAULT 0;
CREATE TABLE iceberg_reconciliation_attempts (
 id TEXT PRIMARY KEY,
 registration_id TEXT NOT NULL,
 attempt_number INTEGER NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('ACTIVE','RETRY_REQUIRED','SUCCEEDED','FAILED','CANCELED','EXPIRED')),
 fencing_token TEXT NOT NULL UNIQUE,
 lease_deadline TEXT NOT NULL,
 observation_start_identity TEXT NOT NULL DEFAULT '',
 observation_end_identity TEXT NOT NULL DEFAULT '',
 outcome TEXT NOT NULL DEFAULT '',
 evidence_digest TEXT NOT NULL DEFAULT '',
 error_class TEXT NOT NULL DEFAULT '',
 next_eligible_at TEXT,
 started_at TEXT NOT NULL,
 finished_at TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 FOREIGN KEY(registration_id) REFERENCES iceberg_registrations(id) ON DELETE CASCADE,
 UNIQUE(registration_id,attempt_number)
);
CREATE UNIQUE INDEX idx_iceberg_reconciliation_one_active ON iceberg_reconciliation_attempts(registration_id) WHERE status='ACTIVE';
CREATE INDEX idx_iceberg_reconciliation_expiry ON iceberg_reconciliation_attempts(status,lease_deadline);
CREATE INDEX idx_iceberg_reconciliation_eligible ON iceberg_registrations(status,reconciliation_status,reconciliation_next_eligible_at,dataset_id,target_key,dataset_sequence);
UPDATE iceberg_registrations SET reconciliation_status='PENDING' WHERE status='RECONCILING' AND reconciliation_status='';
`

const schemaV11 = `
CREATE TABLE iceberg_registrations (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  dataset_sequence INTEGER NOT NULL CHECK(dataset_sequence > 0),
  target_key TEXT NOT NULL,
  commit_id TEXT NOT NULL CHECK(length(commit_id) = 64),
  manifest_key TEXT NOT NULL CHECK(length(trim(manifest_key)) > 0),
  artifact_set_digest TEXT NOT NULL CHECK(length(artifact_set_digest) = 64),
  backend_type TEXT NOT NULL,
  catalog_namespace TEXT NOT NULL DEFAULT '',
  table_identifier TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL CHECK(status IN ('PENDING','REGISTERING','RETRY_REQUIRED','RECONCILING','REGISTERED','FAILED','QUARANTINED','CANCELED','BLOCKED')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
  current_attempt_id TEXT,
  next_eligible_at TEXT,
  last_error_class TEXT NOT NULL DEFAULT '',
  last_error_message TEXT,
  registered_snapshot_or_metadata_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  registered_at TEXT,
  FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
  UNIQUE(run_id, target_key),
  UNIQUE(dataset_id, target_key, dataset_sequence)
);
CREATE TABLE iceberg_registration_attempts (
  id TEXT PRIMARY KEY,
  registration_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL CHECK(attempt_number > 0),
  status TEXT NOT NULL CHECK(status IN ('ACTIVE','RETRY_REQUIRED','RECONCILING','SUCCEEDED','FAILED','CANCELED','EXPIRED')),
  fencing_token TEXT NOT NULL UNIQUE,
  lease_deadline TEXT NOT NULL,
  last_renewed_at TEXT NOT NULL,
  phase TEXT NOT NULL CHECK(phase IN ('PREPARED','EXTERNAL_COMMIT_STARTED','CATALOG_COMMITTED','ICE_STATE_WRITING','VERIFIED')),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  failure_class TEXT NOT NULL DEFAULT '',
  failure_message TEXT,
  next_eligible_at TEXT,
  catalog_receipt TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(registration_id) REFERENCES iceberg_registrations(id) ON DELETE CASCADE,
  UNIQUE(registration_id, attempt_number)
);
CREATE UNIQUE INDEX idx_iceberg_registration_one_active_attempt
  ON iceberg_registration_attempts(registration_id) WHERE status='ACTIVE';
CREATE UNIQUE INDEX idx_iceberg_registration_one_active_dataset
  ON iceberg_registrations(dataset_id, target_key) WHERE status='REGISTERING';
CREATE INDEX idx_iceberg_registration_eligible
  ON iceberg_registrations(status, next_eligible_at, dataset_id, target_key, dataset_sequence);
CREATE INDEX idx_iceberg_registration_order
  ON iceberg_registrations(dataset_id, target_key, dataset_sequence, status);
CREATE INDEX idx_iceberg_registration_attempt_expiry
  ON iceberg_registration_attempts(status, lease_deadline);
`

const schemaV10 = `
ALTER TABLE task_artifacts RENAME TO task_artifacts_v9;
CREATE TABLE task_artifacts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  file_index INTEGER NOT NULL CHECK(file_index >= 0),
  object_key TEXT NOT NULL CHECK(length(trim(object_key)) > 0),
  byte_size INTEGER NOT NULL CHECK(byte_size > 0),
  sha256 TEXT NOT NULL CHECK(length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
  row_count INTEGER NOT NULL CHECK(row_count >= 0),
  schema_fingerprint TEXT NOT NULL CHECK(length(schema_fingerprint) = 64 AND schema_fingerprint NOT GLOB '*[^0-9a-f]*'),
  run_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL CHECK(attempt_number > 0),
  format_version INTEGER NOT NULL CHECK(format_version = 1),
  verification_method TEXT NOT NULL CHECK(verification_method IN ('PORTABLE_FULL_SHA256','PROVIDER_SHA256_AND_PORTABLE_FULL_SHA256')),
  verification_status TEXT NOT NULL CHECK(verification_status = 'VERIFIED'),
  verified_at TEXT NOT NULL,
  max_hwm TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
  FOREIGN KEY(attempt_id) REFERENCES task_attempts(id) ON DELETE CASCADE,
  UNIQUE(attempt_id, file_index),
  UNIQUE(object_key)
);
INSERT INTO task_artifacts SELECT * FROM task_artifacts_v9;
DROP TABLE task_artifacts_v9;
CREATE INDEX idx_task_artifacts_task ON task_artifacts(task_id, file_index);
CREATE INDEX idx_task_artifacts_attempt ON task_artifacts(attempt_id, file_index);
`

const schemaV9 = `
CREATE TABLE task_artifacts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  file_index INTEGER NOT NULL CHECK(file_index >= 0),
  object_key TEXT NOT NULL CHECK(length(trim(object_key)) > 0),
  byte_size INTEGER NOT NULL CHECK(byte_size > 0),
  sha256 TEXT NOT NULL CHECK(length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
  row_count INTEGER NOT NULL CHECK(row_count >= 0),
  schema_fingerprint TEXT NOT NULL CHECK(length(schema_fingerprint) = 64 AND schema_fingerprint NOT GLOB '*[^0-9a-f]*'),
  run_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL CHECK(attempt_number > 0),
  format_version INTEGER NOT NULL CHECK(format_version = 1),
  verification_method TEXT NOT NULL CHECK(verification_method = 'PORTABLE_FULL_SHA256'),
  verification_status TEXT NOT NULL CHECK(verification_status = 'VERIFIED'),
  verified_at TEXT NOT NULL,
  max_hwm TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
  FOREIGN KEY(attempt_id) REFERENCES task_attempts(id) ON DELETE CASCADE,
  UNIQUE(attempt_id, file_index),
  UNIQUE(object_key)
);
CREATE INDEX idx_task_artifacts_task ON task_artifacts(task_id, file_index);
CREATE INDEX idx_task_artifacts_attempt ON task_artifacts(attempt_id, file_index);
`

const schemaV8 = `
ALTER TABLE tasks ADD COLUMN current_attempt_id TEXT;
ALTER TABLE tasks ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN next_eligible_at TEXT;

CREATE TABLE task_attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  worker_id TEXT NOT NULL,
  fencing_token TEXT NOT NULL,
  status TEXT NOT NULL,
  assigned_at TEXT NOT NULL,
  lease_deadline TEXT NOT NULL,
  last_renewed_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  failure_class TEXT NOT NULL DEFAULT '',
  failure_message TEXT,
  result_digest TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
  UNIQUE(task_id, attempt_number),
  UNIQUE(fencing_token)
);
CREATE UNIQUE INDEX idx_task_attempts_one_active
  ON task_attempts(task_id) WHERE status='ACTIVE';
CREATE INDEX idx_task_attempts_expiry ON task_attempts(status, lease_deadline);
CREATE INDEX idx_tasks_retry_eligible ON tasks(status, next_eligible_at, run_id, task_index);

-- No legacy worker possesses fenced attempt credentials. Recover active legacy
-- tasks for a fresh assignment instead of inventing ownership after upgrade.
UPDATE tasks
SET status='PENDING', worker_id=NULL, started_at=NULL,
    current_attempt_id=NULL, next_eligible_at=NULL
WHERE status='RUNNING';
`

const schemaV7 = `
ALTER TABLE runs ADD COLUMN commit_id TEXT NOT NULL DEFAULT '';
ALTER TABLE runs ADD COLUMN commit_intent_json TEXT NOT NULL DEFAULT '';
ALTER TABLE runs ADD COLUMN commit_phase TEXT NOT NULL DEFAULT '';
DROP INDEX IF EXISTS idx_runs_dataset_active;
CREATE UNIQUE INDEX idx_runs_dataset_active
  ON runs(dataset_key)
  WHERE status IN ('PLANNING','RUNNING','COMMITTING') AND dataset_key <> '';
`

const schemaV1 = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS connections (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  engine TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  secret_enc_blob BLOB NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_connections_name ON connections(name);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  source_connection_id TEXT NOT NULL,
  target_connection_id TEXT NOT NULL,
  source_sql TEXT NOT NULL,
  target_namespace TEXT NOT NULL,
  target_table TEXT NOT NULL,
  write_mode TEXT NOT NULL,
  incremental INTEGER NOT NULL,
  hwm_column TEXT,
  options_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  status TEXT NOT NULL,
  correlation_id TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  error_summary TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_job_id_started_at ON runs(job_id, started_at);

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  task_index INTEGER NOT NULL,
  partition_spec_json TEXT NOT NULL,
  worker_id TEXT,
  status TEXT NOT NULL,
  rows_read INTEGER NOT NULL,
  bytes_read INTEGER NOT NULL,
  bytes_written INTEGER NOT NULL,
  parquet_objects_json TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  error_message TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_run_task_index ON tasks(run_id, task_index);
CREATE INDEX IF NOT EXISTS idx_tasks_run_status ON tasks(run_id, status);

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  task_id TEXT,
  ts TEXT NOT NULL,
  level TEXT NOT NULL,
  message TEXT NOT NULL,
  fields_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_run_ts ON events(run_id, ts);

CREATE TABLE IF NOT EXISTS hwm (
  job_id TEXT PRIMARY KEY,
  hwm_value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
  id TEXT PRIMARY KEY,
  addr TEXT NOT NULL,
  status TEXT NOT NULL,
  last_heartbeat DATETIME NOT NULL,
  capabilities_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workers_status_heartbeat ON workers(status, last_heartbeat);
`

const schemaV2 = `
ALTER TABLE runs ADD COLUMN dataset_key TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_runs_status_started_at ON runs(status, started_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_dataset_active
  ON runs(dataset_key)
  WHERE status IN ('PLANNING','RUNNING') AND dataset_key <> '';

CREATE INDEX IF NOT EXISTS idx_tasks_status_run_task ON tasks(status, run_id, task_index);
`

const schemaV3 = `
CREATE TABLE IF NOT EXISTS audit_log (
  id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,
  actor_type TEXT NOT NULL,
  actor_id TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  request_id TEXT NOT NULL DEFAULT '',
  before_json TEXT,
  after_json TEXT,
  metadata_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_log_ts ON audit_log(ts);
CREATE INDEX IF NOT EXISTS idx_audit_log_resource ON audit_log(resource_type, resource_id, ts);
CREATE INDEX IF NOT EXISTS idx_audit_log_action_ts ON audit_log(action, ts);
`

const schemaV4 = `
ALTER TABLE runs ADD COLUMN registration_config_json TEXT NOT NULL DEFAULT '';
`

const schemaV6 = `
CREATE TABLE IF NOT EXISTS servers (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  host TEXT NOT NULL,
  ssh_port INTEGER NOT NULL,
  ssh_user TEXT NOT NULL,
  project_dir TEXT NOT NULL,
  role_hints_json TEXT NOT NULL DEFAULT '[]',
  labels_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_seen_at TEXT,
  last_error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_servers_name ON servers(name);
CREATE INDEX IF NOT EXISTS idx_servers_host_port ON servers(host, ssh_port);

CREATE TABLE IF NOT EXISTS server_credentials (
  id TEXT PRIMARY KEY,
  server_id TEXT NOT NULL,
  auth_type TEXT NOT NULL,
  username TEXT NOT NULL,
  private_key_enc BLOB,
  password_enc BLOB,
  passphrase_enc BLOB,
  host_key_fingerprint TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_server_credentials_server_id ON server_credentials(server_id);

CREATE TABLE IF NOT EXISTS command_executions (
  id TEXT PRIMARY KEY,
  server_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  allowlist_id TEXT NOT NULL,
  params_json TEXT NOT NULL,
  status TEXT NOT NULL,
  exit_code INTEGER,
  started_at TEXT,
  finished_at TEXT,
  stdout_tail TEXT NOT NULL DEFAULT '',
  stderr_tail TEXT NOT NULL DEFAULT '',
  error TEXT,
  requested_by TEXT NOT NULL DEFAULT '',
  FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_command_executions_server_started ON command_executions(server_id, started_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_command_executions_status_started ON command_executions(status, started_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS deployments (
  id TEXT PRIMARY KEY,
  server_id TEXT NOT NULL,
  component TEXT NOT NULL,
  script_id TEXT NOT NULL,
  status TEXT NOT NULL,
  execution_id TEXT NOT NULL DEFAULT '',
  started_at TEXT,
  finished_at TEXT,
  error TEXT,
  FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_deployments_server_started ON deployments(server_id, started_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_deployments_status_started ON deployments(status, started_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS config_versions (
  id TEXT PRIMARY KEY,
  server_id TEXT NOT NULL,
  config_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  content_enc BLOB NOT NULL,
  created_at TEXT NOT NULL,
  validation_status TEXT NOT NULL,
  validation_errors_json TEXT NOT NULL,
  FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_config_versions_server_config_version ON config_versions(server_id, config_id, version);
CREATE INDEX IF NOT EXISTS idx_config_versions_server_config_created ON config_versions(server_id, config_id, created_at DESC, id DESC);
`

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func runTableHasColumn(ctx context.Context, q queryer, column string) (bool, error) {
	rows, err := q.QueryContext(ctx, `PRAGMA table_info(runs);`)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			typ        string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultVal, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func ensureRunRegistrationConfigColumn(ctx context.Context, exec migrationExecutor) error {
	hasColumn, err := runTableHasColumn(ctx, exec, "registration_config_json")
	if err != nil {
		return err
	}
	if hasColumn {
		return nil
	}
	_, err = exec.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN registration_config_json TEXT NOT NULL DEFAULT '';`)
	return err
}

func Migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON;"); err != nil {
		return err
	}

	// Ensure schema_migrations exists
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);`); err != nil {
		return err
	}

	applied := map[int]bool{}
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations;`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", m.version, err)
		}
		switch {
		case m.apply != nil:
			if err := m.apply(ctx, tx); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("apply migration %d: %w", m.version, err)
			}
		default:
			if _, err := tx.ExecContext(ctx, m.sql); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("apply migration %d: %w", m.version, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ','now'));`, m.version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %d: %w", m.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.version, err)
		}
	}
	return nil
}
