package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
)

const legacySchemaV4 = `
CREATE TABLE IF NOT EXISTS run_registrations (
  run_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  engine TEXT NOT NULL,
  iceberg_table TEXT NOT NULL,
  config_json TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  started_at TEXT,
  finished_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
`

func openRawTestDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "migrate-test.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigrateAddsRegistrationConfigColumnForLegacyVersion4DB(t *testing.T) {
	ctx := context.Background()
	db := openRawTestDB(t)

	for _, stmt := range []string{schemaV1, schemaV2, schemaV3, legacySchemaV4} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed legacy schema: %v", err)
		}
	}
	for _, version := range []int{1, 2, 3, 4} {
		if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES (?, '2026-04-17T00:00:00Z');`, version); err != nil {
			t.Fatalf("insert schema migration %d: %v", version, err)
		}
	}

	hasColumn, err := runTableHasColumn(ctx, db, "registration_config_json")
	if err != nil {
		t.Fatalf("check column before migrate: %v", err)
	}
	if hasColumn {
		t.Fatal("registration_config_json already present before migration")
	}

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate second pass: %v", err)
	}

	hasColumn, err = runTableHasColumn(ctx, db, "registration_config_json")
	if err != nil {
		t.Fatalf("check column after migrate: %v", err)
	}
	if !hasColumn {
		t.Fatal("registration_config_json column missing after migration")
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version=5;`).Scan(&count); err != nil {
		t.Fatalf("count schema migration 5: %v", err)
	}
	if count != 1 {
		t.Fatalf("migration version 5 count=%d want 1", count)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version=6;`).Scan(&count); err != nil {
		t.Fatalf("count schema migration 6: %v", err)
	}
	if count != 1 {
		t.Fatalf("migration version 6 count=%d want 1", count)
	}
}

func TestMigrationV5ConvergesWhenColumnExistsBeforeVersionRecorded(t *testing.T) {
	ctx := context.Background()
	raw := openRawTestDB(t)
	for _, stmt := range []string{schemaV1, schemaV2, schemaV3, legacySchemaV4} {
		if _, err := raw.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	for version := 1; version <= 4; version++ {
		if _, err := raw.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES (?, '2026-04-17T00:00:00Z');`, version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN registration_config_json TEXT NOT NULL DEFAULT '';`); err != nil {
		t.Fatal(err)
	}

	if err := Migrate(ctx, raw); err != nil {
		t.Fatalf("resume partially applied migration: %v", err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	var versions int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version=5`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 1 {
		t.Fatalf("version 5 rows=%d want 1", versions)
	}
}

func TestMigrationV7PreservesRunsAndLocksCommittingDatasets(t *testing.T) {
	ctx := context.Background()
	db := openRawTestDB(t)
	for _, stmt := range []string{schemaV1, schemaV2, schemaV3, schemaV4, schemaV6} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed v6 schema: %v", err)
		}
	}
	for version := 1; version <= 6; version++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES (?, '2026-07-22T00:00:00Z')`, version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json) VALUES ('r1','j','dataset','RUNNING','c','2026-07-22T00:00:00Z','')`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate v7: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE runs SET status='COMMITTING' WHERE id='r1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json) VALUES ('r2','j','dataset','PLANNING','c2','2026-07-22T00:00:01Z','')`); err == nil {
		t.Fatal("expected v7 active-dataset uniqueness violation")
	}
	var status, commitID string
	if err := db.QueryRowContext(ctx, `SELECT status,commit_id FROM runs WHERE id='r1'`).Scan(&status, &commitID); err != nil {
		t.Fatal(err)
	}
	if status != "COMMITTING" || commitID != "" {
		t.Fatalf("status=%q commit_id=%q", status, commitID)
	}
}

func TestMigrationV8PreservesTerminalTasksAndRecoversLegacyActiveTasks(t *testing.T) {
	ctx := context.Background()
	db := openRawTestDB(t)
	for _, stmt := range []string{schemaV1, schemaV2, schemaV3, schemaV4, schemaV6, schemaV7} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed v7 schema: %v", err)
		}
	}
	for version := 1; version <= 7; version++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations(version,applied_at) VALUES(?,'2026-07-22T00:00:00Z')`, version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO jobs(id,name,source_connection_id,target_connection_id,source_sql,target_namespace,target_table,write_mode,incremental,hwm_column,options_json,created_at,updated_at) VALUES('j','j','s','t','select 1','n','t','append',0,'','{}','x','x')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json) VALUES('r','j','d','RUNNING','c','x','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO tasks(id,run_id,task_index,partition_spec_json,status,worker_id,started_at,rows_read,bytes_read,bytes_written,parquet_objects_json) VALUES('active','r',1,'{}','RUNNING','legacy-worker','x',0,0,0,'[]'),('done','r',2,'{}','SUCCEEDED','legacy-worker','x',1,2,3,'[{"key":"old-layout.parquet"}]')`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate v8: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate v8 second pass: %v", err)
	}
	var activeStatus string
	var activeWorker sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status,worker_id FROM tasks WHERE id='active'`).Scan(&activeStatus, &activeWorker); err != nil {
		t.Fatal(err)
	}
	if activeStatus != "PENDING" || activeWorker.Valid {
		t.Fatalf("legacy active status=%q worker=%v", activeStatus, activeWorker)
	}
	var doneStatus, objects string
	if err := db.QueryRowContext(ctx, `SELECT status,parquet_objects_json FROM tasks WHERE id='done'`).Scan(&doneStatus, &objects); err != nil {
		t.Fatal(err)
	}
	if doneStatus != "SUCCEEDED" || objects != `[{"key":"old-layout.parquet"}]` {
		t.Fatalf("terminal task status=%q objects=%q", doneStatus, objects)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO task_attempts(id,task_id,attempt_number,worker_id,fencing_token,status,assigned_at,lease_deadline,last_renewed_at,created_at,updated_at) VALUES('a1','active',1,'w','t1','ACTIVE','x','x','x','x','x'),('a2','active',2,'w','t2','ACTIVE','x','x','x','x','x')`); err == nil {
		t.Fatal("expected one-active-attempt uniqueness violation")
	}
}

func TestMigrationV9PreservesHistoricalOutputsAsUnverified(t *testing.T) {
	ctx := context.Background()
	db := openRawTestDB(t)
	for _, stmt := range []string{schemaV1, schemaV2, schemaV3, schemaV4, schemaV6, schemaV7, schemaV8} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	for version := 1; version <= 8; version++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations(version,applied_at) VALUES(?,'2026-07-22T00:00:00Z')`, version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO jobs(id,name,source_connection_id,target_connection_id,source_sql,target_namespace,target_table,write_mode,incremental,hwm_column,options_json,created_at,updated_at) VALUES('j9','j','s','t','select 1','n','t','append',0,'','{}','x','x')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json,commit_id,commit_intent_json,commit_phase) VALUES('r9','j9','d9','SUCCEEDED','c','x','','legacy','{}','VERIFIED')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO tasks(id,run_id,task_index,partition_spec_json,status,rows_read,bytes_read,bytes_written,parquet_objects_json) VALUES('t9','r9',1,'{}','SUCCEEDED',1,0,10,'[{"key":"legacy.parquet"}]')`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	var artifacts int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_artifacts`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if artifacts != 0 {
		t.Fatalf("invented historical artifacts=%d", artifacts)
	}
	var objects string
	if err := db.QueryRowContext(ctx, `SELECT parquet_objects_json FROM tasks WHERE id='t9'`).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if objects != `[{"key":"legacy.parquet"}]` {
		t.Fatalf("historical output changed: %s", objects)
	}
}

func TestMigrationV10PreservesArtifactsAndAllowsProviderVerification(t *testing.T) {
	ctx := context.Background()
	raw := openRawTestDB(t)
	for _, m := range migrations[:9] {
		if m.sql != "" {
			if _, err := raw.ExecContext(ctx, m.sql); err != nil {
				t.Fatal(err)
			}
		} else if m.apply != nil {
			if err := m.apply(ctx, raw); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := raw.ExecContext(ctx, `INSERT INTO schema_migrations(version,applied_at) VALUES(?,'2026-07-23T00:00:00Z')`, m.version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO jobs(id,name,source_connection_id,target_connection_id,source_sql,target_namespace,target_table,write_mode,incremental,hwm_column,options_json,created_at,updated_at) VALUES('j10','j','s','t','q','n','t','append',0,'','{}','x','x')`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json) VALUES('r10','j10','d10','RUNNING','c','x','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO tasks(id,run_id,task_index,partition_spec_json,status,rows_read,bytes_read,bytes_written,parquet_objects_json,attempt_count) VALUES('t10','r10',1,'{}','RUNNING',0,0,1,'[]',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO task_attempts(id,task_id,attempt_number,worker_id,fencing_token,status,assigned_at,lease_deadline,last_renewed_at,created_at,updated_at) VALUES('a10','t10',1,'w','token','ACTIVE','x','x','x','x','x')`); err != nil {
		t.Fatal(err)
	}
	args := []any{"art10", "t10", "a10", 0, "key", 1, strings.Repeat("a", 64), 0, strings.Repeat("b", 64), "r10", 1, 1, artifact.VerificationPortable, "VERIFIED", "x", "", "x"}
	if _, err := raw.ExecContext(ctx, `INSERT INTO task_artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, args...); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	var method string
	if err := raw.QueryRowContext(ctx, `SELECT verification_method FROM task_artifacts WHERE id='art10'`).Scan(&method); err != nil || method != artifact.VerificationPortable {
		t.Fatalf("method=%s err=%v", method, err)
	}
	if _, err := raw.ExecContext(ctx, `UPDATE task_artifacts SET verification_method=? WHERE id='art10'`, artifact.VerificationProvider); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationV12PreservesAmbiguousRegistrationsWithoutInventingEvidence(t *testing.T) {
	ctx := context.Background()
	raw := openRawTestDB(t)
	for _, m := range migrations[:11] {
		if m.sql != "" {
			if _, err := raw.ExecContext(ctx, m.sql); err != nil {
				t.Fatal(err)
			}
		} else if m.apply != nil {
			if err := m.apply(ctx, raw); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := raw.ExecContext(ctx, `INSERT INTO schema_migrations(version,applied_at) VALUES(?,'2026-07-23T00:00:00Z')`, m.version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO iceberg_registrations(id,run_id,dataset_id,dataset_sequence,target_key,commit_id,manifest_key,artifact_set_digest,backend_type,status,created_at,updated_at) VALUES('reg-v11','run-v11','dataset',1,'target',?, 'manifest.json',?,'rest-go','RECONCILING','x','x')`, strings.Repeat("a", 64), strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	var status, outcome, evidence, snapshot, metadata string
	if err := raw.QueryRowContext(ctx, `SELECT reconciliation_status,reconciliation_outcome,reconciliation_evidence_digest,observed_snapshot_id,observed_metadata_identity FROM iceberg_registrations WHERE id='reg-v11'`).Scan(&status, &outcome, &evidence, &snapshot, &metadata); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || outcome != "" || evidence != "" || snapshot != "" || metadata != "" {
		t.Fatalf("status=%q outcome=%q evidence=%q snapshot=%q metadata=%q", status, outcome, evidence, snapshot, metadata)
	}
	var attempts int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM iceberg_reconciliation_attempts`).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}

func TestMigrationV13PreservesV12StateAndInventsNoLeader(t *testing.T) {
	ctx := context.Background()
	raw := openRawTestDB(t)
	for _, m := range migrations[:12] {
		if m.sql != "" {
			if _, err := raw.ExecContext(ctx, m.sql); err != nil {
				t.Fatal(err)
			}
		} else if m.apply != nil {
			if err := m.apply(ctx, raw); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := raw.ExecContext(ctx, `INSERT INTO schema_migrations(version,applied_at) VALUES(?,'2026-07-23T00:00:00Z')`, m.version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO iceberg_registrations(id,run_id,dataset_id,dataset_sequence,target_key,commit_id,manifest_key,artifact_set_digest,backend_type,status,reconciliation_status,created_at,updated_at) VALUES('reg-v12','run-v12','dataset',1,'target',?,'manifest.json',?,'rest-go','RECONCILING','PENDING','x','x')`, strings.Repeat("a", 64), strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	var instance, status string
	var epoch, deadline int64
	if err := raw.QueryRowContext(ctx, `SELECT instance_id,epoch,status,lease_deadline_ms FROM master_leadership WHERE leadership_name='master'`).Scan(&instance, &epoch, &status, &deadline); err != nil {
		t.Fatal(err)
	}
	if instance != "" || epoch != 0 || status != "RELEASED" || deadline != 0 {
		t.Fatalf("invented leader instance=%q epoch=%d status=%q deadline=%d", instance, epoch, status, deadline)
	}
	var registrationStatus string
	if err := raw.QueryRowContext(ctx, `SELECT status FROM iceberg_registrations WHERE id='reg-v12'`).Scan(&registrationStatus); err != nil || registrationStatus != "RECONCILING" {
		t.Fatalf("registration=%q err=%v", registrationStatus, err)
	}
}

func TestMigrationV14PreservesV13AttemptsAndInventsNoUploads(t *testing.T) {
	ctx := context.Background()
	raw := openRawTestDB(t)
	for _, m := range migrations[:13] {
		if m.sql != "" {
			if _, err := raw.ExecContext(ctx, m.sql); err != nil {
				t.Fatal(err)
			}
		} else if m.apply != nil {
			if err := m.apply(ctx, raw); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := raw.ExecContext(ctx, `INSERT INTO schema_migrations(version,applied_at) VALUES(?,'2026-07-24T00:00:00Z')`, m.version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO task_attempts(id,task_id,attempt_number,worker_id,fencing_token,status,assigned_at,lease_deadline,last_renewed_at,created_at,updated_at,assigned_by_leader_epoch) VALUES('legacy-attempt','legacy-task',1,'worker','token','EXPIRED','x','x','x','x','x',7)`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	var attempts, uploads int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_attempts WHERE id='legacy-attempt' AND assigned_by_leader_epoch=7`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM multipart_uploads`).Scan(&uploads); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || uploads != 0 {
		t.Fatalf("attempts=%d uploads=%d", attempts, uploads)
	}
}

func TestMigrationV15PreservesV14UploadsAndInventsNoCleanupEvidence(t *testing.T) {
	ctx := context.Background()
	raw := openRawTestDB(t)
	for _, m := range migrations[:14] {
		if m.sql != "" {
			if _, err := raw.ExecContext(ctx, m.sql); err != nil {
				t.Fatal(err)
			}
		} else if m.apply != nil {
			if err := m.apply(ctx, raw); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := raw.ExecContext(ctx, `INSERT INTO schema_migrations(version,applied_at) VALUES(?,'2026-07-24T00:00:00Z')`, m.version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO multipart_uploads(id,run_id,task_id,attempt_id,attempt_number,file_index,object_key,managed_prefix,provider_upload_id,status,worker_id,created_at,last_activity_at,object_sha256,object_size,updated_at) VALUES('upload-v14','run-v14','task-v14','attempt-v14',1,0,'managed/file.parquet','managed/','provider-v14','COMPLETED','worker','x','x',?,10,'x')`, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	var uploads, candidates int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM multipart_uploads WHERE id='upload-v14'`).Scan(&uploads); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM canceled_object_candidates`).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if uploads != 1 || candidates != 0 {
		t.Fatalf("uploads=%d candidates=%d", uploads, candidates)
	}
}

func TestMigrationV16RepairsUnclaimableReconciliation(t *testing.T) {
	ctx := context.Background()
	raw := openRawTestDB(t)
	for _, m := range migrations[:12] {
		if m.sql != "" {
			if _, err := raw.ExecContext(ctx, m.sql); err != nil {
				t.Fatal(err)
			}
		} else if m.apply != nil {
			if err := m.apply(ctx, raw); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := raw.ExecContext(ctx, `INSERT INTO schema_migrations(version,applied_at) VALUES(?,'2026-07-29T00:00:00Z')`, m.version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO iceberg_registrations(id,run_id,dataset_id,dataset_sequence,target_key,commit_id,manifest_key,artifact_set_digest,backend_type,status,reconciliation_status,created_at,updated_at) VALUES('reg-stuck','run-stuck','dataset',1,'target',?,'manifest.json',?,'rest-go','RECONCILING','','x','x')`, strings.Repeat("a", 64), strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations[12:15] {
		if m.sql != "" {
			if _, err := raw.ExecContext(ctx, m.sql); err != nil {
				t.Fatal(err)
			}
		} else if m.apply != nil {
			if err := m.apply(ctx, raw); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := raw.ExecContext(ctx, `INSERT INTO schema_migrations(version,applied_at) VALUES(?,'2026-07-29T00:00:00Z')`, m.version); err != nil {
			t.Fatal(err)
		}
	}

	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := raw.QueryRowContext(ctx, `SELECT reconciliation_status FROM iceberg_registrations WHERE id='reg-stuck'`).Scan(&status); err != nil || status != RegistrationPending {
		t.Fatalf("reconciliation_status=%q err=%v", status, err)
	}
}

func TestMigrationV17CanResumeAfterSchemaCreatedBeforeVersionRecorded(t *testing.T) {
	ctx := context.Background()
	raw := openRawTestDB(t)
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version=17`); err != nil {
		t.Fatal(err)
	}

	if err := Migrate(ctx, raw); err != nil {
		t.Fatalf("resume migration v17: %v", err)
	}

	var versions, indexes int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version=17`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_upload_capacity_active'`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if versions != 1 || indexes != 1 {
		t.Fatalf("versions=%d indexes=%d", versions, indexes)
	}
}
