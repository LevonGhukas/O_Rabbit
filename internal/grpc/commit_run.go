package grpcapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// commitObjectStore is the narrow object-storage surface required by durable
// run publication. Keeping it small also makes ambiguous responses and
// read-back failures deterministic to exercise in tests.
type commitObjectStore interface {
	Head(context.Context, string) error
	GetObjectBytes(context.Context, string) ([]byte, bool, error)
	PutObjectBytes(context.Context, string, []byte, string, map[string]string) error
	OpenObject(context.Context, string) (io.ReadCloser, bool, error)
	ObjectChecksum(context.Context, string) (int64, string, bool, error)
}

type s3CommitObjectStore struct{ uploader *s3io.Uploader }

type durableCommitIntent struct {
	CommitID      string                   `json:"commit_id"`
	DatasetID     string                   `json:"dataset_id,omitempty"`
	ManifestKey   string                   `json:"manifest_key"`
	StateKey      string                   `json:"state_key"`
	Destination   durableCommitDestination `json:"destination,omitempty"`
	Manifest      json.RawMessage          `json:"manifest"`
	PreviousState json.RawMessage          `json:"previous_state"`
	ProposedState json.RawMessage          `json:"proposed_state"`
	IcebergSchema json.RawMessage          `json:"iceberg_schema,omitempty"`
}

type durableCommitDestination struct {
	Endpoint       string `json:"endpoint"`
	Region         string `json:"region,omitempty"`
	Bucket         string `json:"bucket"`
	Prefix         string `json:"prefix"`
	ForcePathStyle bool   `json:"force_path_style"`
}

func (s s3CommitObjectStore) Head(ctx context.Context, key string) error {
	_, err := s.uploader.Head(ctx, key)
	return err
}

func (s s3CommitObjectStore) GetObjectBytes(ctx context.Context, key string) ([]byte, bool, error) {
	return s.uploader.GetObjectBytes(ctx, key)
}

func (s s3CommitObjectStore) PutObjectBytes(ctx context.Context, key string, b []byte, contentType string, meta map[string]string) error {
	return s.uploader.PutObjectBytes(ctx, key, b, contentType, meta)
}

func (s s3CommitObjectStore) OpenObject(ctx context.Context, key string) (io.ReadCloser, bool, error) {
	return s.uploader.OpenObject(ctx, key)
}

func (s s3CommitObjectStore) ObjectChecksum(ctx context.Context, key string) (int64, string, bool, error) {
	return s.uploader.ObjectChecksum(ctx, key)
}

// commitRun finalizes a successful run by writing commit metadata and updating dataset state.
// It is called by the master when a run is finalized (all tasks completed successfully).
func (s *Server) commitRun(ctx context.Context, runID string) error {
	startedAt := time.Now()

	run, err := s.st.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status == "SUCCEEDED" {
		return nil
	}
	// Commit where the run was planned to write, per its configuration
	// snapshot, even if the job or connections were edited since.
	resolved, err := s.resolveRun(ctx, run)
	if err != nil {
		return err
	}
	job := resolved.config.Job
	srcConn, err := s.st.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		return err
	}
	srcConn.Engine = resolved.config.SourceEngine
	tasks, err := s.st.ListTasksForRun(ctx, runID)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return fmt.Errorf("commit verification: run %s has no durable tasks", runID)
	}
	for _, task := range tasks {
		if task.Status != "SUCCEEDED" {
			return fmt.Errorf("commit verification: task %s is %s", task.ID, task.Status)
		}
		if task.BytesWritten > 0 && len(collectParquetKeys([]db.Task{task})) == 0 {
			return fmt.Errorf("commit verification: task %s has bytes but no durable object key", task.ID)
		}
	}
	acceptedArtifacts, err := s.st.ListArtifactsForRun(ctx, runID)
	if err != nil {
		return err
	}
	artifactsByTask := make(map[string][]artifact.Record)
	for _, record := range acceptedArtifacts {
		if err := record.Validate(); err != nil {
			return fmt.Errorf("commit artifact %s: %w", record.ObjectKey, err)
		}
		artifactsByTask[record.TaskID] = append(artifactsByTask[record.TaskID], record)
	}
	strictArtifacts := false
	for _, task := range tasks {
		if task.AttemptCount > 0 {
			strictArtifacts = true
		}
		records := artifactsByTask[task.ID]
		if task.AttemptCount > 0 && task.BytesWritten > 0 && len(records) == 0 {
			return fmt.Errorf("commit integrity: task %s lacks durable verified artifacts", task.ID)
		}
		var recordRows, recordBytes int64
		for _, record := range records {
			recordRows += record.RowCount
			recordBytes += record.ByteSize
		}
		if len(records) > 0 && (recordRows != task.RowsRead || recordBytes != task.BytesWritten) {
			return fmt.Errorf("commit integrity: task %s artifact aggregates mismatch", task.ID)
		}
	}
	committedKeys := collectParquetKeys(tasks)
	if strictArtifacts {
		committedKeys = committedKeys[:0]
		for _, record := range acceptedArtifacts {
			committedKeys = append(committedKeys, record.ObjectKey)
		}
	}
	sort.Strings(committedKeys)

	s3cfg, err := s.targetS3Config(ctx, resolved)
	if err != nil {
		return err
	}
	endpoint, region, bucket, forcePathStyle := s3cfg.Endpoint, s3cfg.Region, s3cfg.Bucket, s3cfg.ForcePathStyle
	accessKey, secretKey, sessionToken := s3cfg.AccessKeyID, s3cfg.SecretAccessKey, s3cfg.SessionToken
	tgtMeta := resolved.target.Metadata
	opts := resolved.opts
	if opts.NormalizedSourceMode() == "query" && strings.TrimSpace(opts.QueryHash) == "" {
		sourceQuery := strings.TrimSpace(opts.Query)
		if sourceQuery == "" {
			sourceQuery = strings.TrimSpace(job.SourceSQL)
		}
		if normalized, err := connectors.NormalizeReadOnlySQLQuery(sourceQuery); err == nil {
			opts.Query = normalized
			opts.QueryHash = connectors.QueryHash(normalized)
			if strings.TrimSpace(opts.SourceName) == "" {
				opts.SourceName = "query_" + opts.QueryHash
			}
		}
	}
	basePrefix := datasetPrefixForJob(job, srcConn.Engine, opts, tgtMeta)
	currentDestination := durableCommitDestination{
		Endpoint:       endpoint,
		Region:         region,
		Bucket:         bucket,
		Prefix:         basePrefix,
		ForcePathStyle: forcePathStyle,
	}
	var intent durableCommitIntent
	var cb, b []byte
	var commitID, commitKey, stateKey string
	hasPersistedIntent := len(run.CommitIntentJSON) > 0
	if hasPersistedIntent {
		intent, currentDestination, err = validatePersistedCommitIntent(run, run.CommitIntentJSON, currentDestination, committedKeys, acceptedArtifacts)
		if err != nil {
			return err
		}
		endpoint = currentDestination.Endpoint
		region = currentDestination.Region
		bucket = currentDestination.Bucket
		forcePathStyle = currentDestination.ForcePathStyle
		basePrefix = currentDestination.Prefix
		commitID, commitKey, stateKey = intent.CommitID, intent.ManifestKey, intent.StateKey
		cb, b = intent.Manifest, intent.ProposedState
	} else {
		currentDestination.Endpoint = normalizeEndpoint(currentDestination.Endpoint)
		currentDestination.Bucket = strings.TrimSpace(currentDestination.Bucket)
		currentDestination.Prefix = normalizePrefix(currentDestination.Prefix)
		commitKey = currentDestination.Prefix + "/_commits/run-" + runID + ".json"
		stateKey = currentDestination.Prefix + "/_state.json"
	}

	u, err := s.newCommitObjectStoreFn(ctx, s3io.Config{
		Endpoint:        endpoint,
		Region:          region,
		Bucket:          bucket,
		ForcePathStyle:  forcePathStyle,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
	})
	if err != nil {
		return err
	}

	// Collect uploaded Parquet object keys for this run.
	// Keys are run-scoped (uploaded under <prefix>/_runs/run-<RUN_ID>/...), so commit is a metadata write (no promote/copy).
	for _, key := range committedKeys {
		if err := u.Head(ctx, key); err != nil {
			return fmt.Errorf("commit verification: required object %s: %w", key, err)
		}
	}
	for _, record := range acceptedArtifacts {
		// The store's own checksum, recorded when the worker verified the
		// upload, proves the object is unchanged with a HEAD. Only objects
		// without one are re-read and hashed through the master.
		if record.ProviderChecksumSHA256 != "" {
			size, checksum, found, err := u.ObjectChecksum(ctx, record.ObjectKey)
			if err != nil {
				return fmt.Errorf("commit artifact verification %s: %w", record.ObjectKey, err)
			}
			if !found {
				return fmt.Errorf("commit artifact verification: missing object %s", record.ObjectKey)
			}
			if size != record.ByteSize {
				return fmt.Errorf("commit artifact size mismatch: %s", record.ObjectKey)
			}
			if checksum != "" {
				if checksum != record.ProviderChecksumSHA256 {
					return fmt.Errorf("commit artifact sha256 mismatch: %s", record.ObjectKey)
				}
				continue
			}
			// The store reported no checksum now; fall back to hashing.
		}
		body, found, err := u.OpenObject(ctx, record.ObjectKey)
		if err != nil {
			return fmt.Errorf("commit artifact verification %s: %w", record.ObjectKey, err)
		}
		if !found {
			return fmt.Errorf("commit artifact verification: missing object %s", record.ObjectKey)
		}
		digest, size, hashErr := artifact.StreamSHA256(ctx, body)
		_ = body.Close()
		if hashErr != nil {
			return fmt.Errorf("commit artifact verification %s: %w", record.ObjectKey, hashErr)
		}
		if size != record.ByteSize {
			return fmt.Errorf("commit artifact size mismatch: %s", record.ObjectKey)
		}
		if digest != record.SHA256 {
			return fmt.Errorf("commit artifact sha256 mismatch: %s", record.ObjectKey)
		}
	}

	existingStateBytes, existingStateFound, err := u.GetObjectBytes(ctx, stateKey)
	if err != nil {
		return fmt.Errorf("read dataset state: %w", err)
	}
	existingMaxPart, existingHWM := parseExistingState(existingStateBytes)
	var existingState struct {
		RunID       string `json:"last_committed_run_id"`
		CommitID    string `json:"commit_id"`
		ManifestKey string `json:"manifest_key"`
		CommittedAt string `json:"committed_at"`
	}
	if existingStateFound {
		if err := json.Unmarshal(existingStateBytes, &existingState); err != nil || strings.TrimSpace(existingState.RunID) == "" || strings.TrimSpace(existingState.CommittedAt) == "" {
			return fmt.Errorf("checkpoint integrity conflict: existing dataset state is malformed")
		}
	}

	cursorDomain := resolveCursorDomain(opts, tasks)
	// The high-water mark only moves forward: a run that re-read rows below it
	// (cursor_lookback) and found nothing newer must not move it back. Values
	// of an unknown cursor type cannot be ordered, so they replace the mark.
	maxHWM := existingHWM
	if runMax := deriveMaxCursor(tasks, cursorDomain); strings.TrimSpace(runMax) != "" &&
		(strings.TrimSpace(existingHWM) == "" || cursorDomain == connectors.CursorDomainUnknown ||
			connectors.CompareCursorValues(cursorDomain, runMax, existingHWM) > 0) {
		maxHWM = runMax
	}
	maxPart := maxInt(maxPartNumber(committedKeys), existingMaxPart)

	// The timestamp and object order are stable so retries reconstruct identical intent.
	committedAt := run.StartedAt
	if existingStateFound && !hasPersistedIntent {
		currentTime, currentErr := time.Parse(time.RFC3339Nano, existingState.CommittedAt)
		runTime, runErr := time.Parse(time.RFC3339Nano, committedAt)
		if currentErr != nil || runErr != nil {
			return fmt.Errorf("checkpoint integrity conflict: invalid commit timestamp")
		}
		if existingState.RunID != runID && !currentTime.Before(runTime) {
			return fmt.Errorf("checkpoint fencing conflict: dataset state belongs to run %s", existingState.RunID)
		}
		if existingState.RunID == runID {
			return fmt.Errorf("checkpoint integrity conflict: state for run %s exists without durable intent", runID)
		}
	}

	// Write a per-run commit manifest (source of truth for committed objects).
	// Once present, the authenticated persisted intent above supplies every
	// destination key and byte sequence; mutable job/connection state is not
	// allowed to reconstruct it.
	if !hasPersistedIntent {
		commitObj := map[string]any{
			"schema_version": 1,
			"job_id":         job.ID,
			"job_name":       job.Name,
			"source_mode":    opts.NormalizedSourceMode(),
			"query_hash":     strings.TrimSpace(opts.QueryHash),
			"bucket":         bucket,
			"prefix":         basePrefix,
			"run_id":         runID,
			"committed_at":   committedAt,
			"max_hwm_value":  maxHWM,
			"max_part":       maxPart,
			"objects":        committedKeys,
			"objects_v2":     collectParquetObjectInfos(tasks),
		}
		if strictArtifacts {
			commitObj["schema_version"] = 2
			commitObj["artifacts"] = acceptedArtifacts
		}
		cb, err = json.Marshal(commitObj)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(cb)
		commitID = hex.EncodeToString(digest[:])
		stateObj := map[string]any{
			"job_id":                job.ID,
			"job_name":              job.Name,
			"source_mode":           opts.NormalizedSourceMode(),
			"query_hash":            strings.TrimSpace(opts.QueryHash),
			"bucket":                bucket,
			"prefix":                basePrefix,
			"last_committed_run_id": runID,
			"committed_at":          committedAt,
			"max_hwm_value":         maxHWM,
			"max_part":              maxPart,
			"next_part":             maxPart + 1,
			"last_run_objects":      committedKeys,
			"commit_id":             commitID,
			"manifest_key":          commitKey,
		}
		b, err = json.Marshal(stateObj)
		if err != nil {
			return err
		}
		intent = durableCommitIntent{
			CommitID:      commitID,
			DatasetID:     run.DatasetKey,
			ManifestKey:   commitKey,
			StateKey:      stateKey,
			Destination:   currentDestination,
			Manifest:      cb,
			ProposedState: b,
		}
		if strictArtifacts && len(acceptedArtifacts) == 0 {
			registration, configErr := icebergreg.ParseRunConfig(run.RegistrationConfigJSON)
			if configErr != nil {
				return &classifiedCommitError{class: commitFailureValidation, component: "registration_config", err: fmt.Errorf("empty dataset schema snapshot: %w", configErr)}
			}
			if registration.Enabled {
				sourceSecret, decryptErr := crypto.Decrypt(s.k, srcConn.SecretEncBlob, []byte(srcConn.ID))
				if decryptErr != nil {
					return &classifiedCommitError{class: commitFailureValidation, component: "source_schema", err: fmt.Errorf("empty dataset source schema is unavailable: %w", decryptErr)}
				}
				var sourceCredentials map[string]any
				if json.Unmarshal(sourceSecret, &sourceCredentials) != nil {
					return &classifiedCommitError{class: commitFailureValidation, component: "source_schema", err: fmt.Errorf("empty dataset source schema credentials are malformed")}
				}
				sourceDSN, _ := sourceCredentials["dsn"].(string)
				sourceQuery := strings.TrimSpace(opts.Query)
				if sourceQuery == "" {
					sourceQuery = strings.TrimSpace(job.SourceSQL)
				}
				var warnings []typesystem.TypeWarning
				intent.IcebergSchema, warnings, err = icebergreg.InferDurableIcebergSchemaWithWarnings(ctx, srcConn.Engine, sourceDSN, opts.NormalizedSourceMode(), strings.TrimSpace(opts.Table), sourceQuery, opts.RecordPath, opts.FileFormat, effectiveColumnTypes(tasks, opts.ColumnTypes))
				if err != nil {
					return &classifiedCommitError{class: commitFailureValidation, component: "source_schema", err: fmt.Errorf("empty dataset source schema is unavailable: %w", err)}
				}
				if len(warnings) > 0 {
					if err := s.st.SetRunTypeWarnings(ctx, runID, warnings); err != nil {
						return fmt.Errorf("persist document type warnings: %w", err)
					}
					for _, warning := range warnings {
						s.log.Warn("type mapping fallback", "run_id", runID, "source_engine", srcConn.Engine, "column", warning.Column, "logical_type", warning.LogicalType, "storage_type", warning.StorageType, "mapping_class", warning.Class, "reason", warning.Reason)
					}
				}
			}
		}
		if existingStateFound {
			intent.PreviousState = append([]byte(nil), existingStateBytes...)
		}
		intentJSON, err := json.Marshal(intent)
		if err != nil {
			return err
		}
		if err := s.st.SaveCommitIntent(ctx, runID, commitID, intentJSON); err != nil {
			return err
		}
	}
	if len(b) == 0 {
		return fmt.Errorf("commit integrity conflict: durable intent missing proposed state")
	}
	if got, ok, err := u.GetObjectBytes(ctx, commitKey); err != nil {
		return fmt.Errorf("verify commit manifest: %w", err)
	} else if ok && string(got) != string(cb) {
		return fmt.Errorf("commit integrity conflict: manifest %s already has different content", commitKey)
	} else if !ok {
		if err := u.PutObjectBytes(ctx, commitKey, cb, "application/json", map[string]string{"job_id": job.ID, "run_id": runID, "commit_id": commitID}); err != nil {
			// Verify ambiguity for diagnostics, but never declare this attempt
			// successful solely by interpreting a failed response. The next
			// attempt reuses matching durable content.
			got, found, readErr := u.GetObjectBytes(ctx, commitKey)
			if readErr == nil && found && string(got) != string(cb) {
				return fmt.Errorf("write commit manifest: response error followed by conflicting durable content: %w", err)
			}
			return fmt.Errorf("write commit manifest: ambiguous response (durable_match=%t): %w", readErr == nil && found && string(got) == string(cb), err)
		}
	}
	if got, ok, err := u.GetObjectBytes(ctx, commitKey); err != nil || !ok || string(got) != string(cb) {
		return fmt.Errorf("verify commit manifest: durable content mismatch")
	}
	_ = s.st.SetCommitPhase(ctx, runID, "MANIFEST_VERIFIED")

	if existingStateFound {
		if !bytes.Equal(existingStateBytes, b) && !bytes.Equal(existingStateBytes, intent.PreviousState) {
			return fmt.Errorf("checkpoint fencing conflict: dataset state changed after commit intent")
		}
	} else if len(intent.PreviousState) > 0 && string(intent.PreviousState) != "null" {
		return fmt.Errorf("checkpoint fencing conflict: expected previous dataset state is missing")
	}
	if !existingStateFound || string(existingStateBytes) != string(b) {
		if err := u.PutObjectBytes(ctx, stateKey, b, "application/json", map[string]string{"job_id": job.ID, "run_id": runID, "commit_id": commitID}); err != nil {
			got, found, readErr := u.GetObjectBytes(ctx, stateKey)
			if readErr == nil && found && string(got) != string(b) {
				return fmt.Errorf("write dataset state: response error followed by conflicting durable content: %w", err)
			}
			return fmt.Errorf("write dataset state: ambiguous response (durable_match=%t): %w", readErr == nil && found && string(got) == string(b), err)
		}
	}
	if got, ok, err := u.GetObjectBytes(ctx, stateKey); err != nil || !ok || string(got) != string(b) {
		return fmt.Errorf("verify dataset state: durable content mismatch")
	}
	_ = s.st.SetCommitPhase(ctx, runID, "STATE_VERIFIED")

	fields, _ := json.Marshal(map[string]any{
		"parquet_object_keys": committedKeys,
		"max_hwm_value":       maxHWM,
		"commit_ms":           time.Since(startedAt).Milliseconds(),
		"commit_id":           commitID,
		"manifest_key":        commitKey,
		"dataset_key":         run.DatasetKey,
		"finalization_phase":  "VERIFIED",
	})
	if job.HWMColumn != nil && maxHWM != "" {
		if err := s.upsertHWMFn(ctx, job.ID, maxHWM); err != nil {
			return fmt.Errorf("repair SQLite HWM mirror: %w", err)
		}
	}
	if err := s.st.SetCommitPhase(ctx, runID, "VERIFIED"); err != nil {
		return err
	}
	e := db.Event{ID: "commit-storage-" + commitID, RunID: runID, TS: db.FormatTimestamp(time.Now()), Level: "INFO", Message: "storage publication verified", FieldsJSON: fields}
	_ = s.st.InsertEventOnce(ctx, e)
	return nil
}

// collectParquetObjectInfos extracts Parquet object info maps from completed tasks.
// Each element is expected to contain at least {"key": "..."}. Additional fields like
// "rows" and "bytes" may be present.
func collectParquetObjectInfos(tasks []db.Task) []map[string]any {
	seen := make(map[string]struct{})
	out := make([]map[string]any, 0)
	forEachTaskParquetObject(tasks, func(o map[string]any) {
		k, _ := o["key"].(string)
		if strings.TrimSpace(k) == "" {
			k, _ = o["object_key"].(string)
		}
		k = strings.TrimSpace(k)
		if k == "" {
			return
		}
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		out = append(out, o)
	})
	return out
}

// collectParquetKeys extracts the unique Parquet object keys from the completed tasks of the run.
// effectiveColumnTypes returns the column types the planner verified and
// handed to workers (see planner.resolveEffectiveColumnTypes). Registration
// must use the same types so the Iceberg schema matches the written Parquet.
func effectiveColumnTypes(tasks []db.Task, fallback map[string]string) map[string]string {
	for _, t := range tasks {
		var spec struct {
			ColumnTypes map[string]string `json:"column_types"`
		}
		if json.Unmarshal(t.PartitionSpec, &spec) == nil && spec.ColumnTypes != nil {
			return spec.ColumnTypes
		}
	}
	return fallback
}

func collectParquetKeys(tasks []db.Task) []string {
	seen := make(map[string]struct{})
	var out []string
	forEachTaskParquetObject(tasks, func(o map[string]any) {
		k, _ := o["key"].(string)
		if strings.TrimSpace(k) == "" {
			k, _ = o["object_key"].(string)
		}
		k = strings.TrimSpace(k)
		if k == "" {
			return
		}
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		out = append(out, k)
	})
	return out
}

// forEachTaskParquetObject decodes each task's Parquet object metadata and invokes fn for each object.
// Malformed task metadata is skipped best-effort.
func forEachTaskParquetObject(tasks []db.Task, fn func(map[string]any)) {
	for _, t := range tasks {
		if len(t.ParquetObjects) == 0 {
			continue
		}
		var arr []map[string]any
		if err := json.Unmarshal(t.ParquetObjects, &arr); err != nil {
			continue
		}
		for _, o := range arr {
			fn(o)
		}
	}
}

func parseExistingState(b []byte) (maxPart int, maxHWM string) {
	if len(b) == 0 {
		return 0, ""
	}
	var s struct {
		MaxHWMValue string `json:"max_hwm_value"`
		MaxPart     int    `json:"max_part"`
		NextPart    int    `json:"next_part"`
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return 0, ""
	}
	mp := s.MaxPart
	if s.NextPart > 0 {
		mp = maxInt(mp, s.NextPart-1)
	}
	return mp, strings.TrimSpace(s.MaxHWMValue)
}

func maxPartNumber(keys []string) int {
	max := 0
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		base := k
		if i := strings.LastIndex(base, "/"); i >= 0 {
			base = base[i+1:]
		}
		if !strings.HasPrefix(base, "part-") || !strings.HasSuffix(base, ".parquet") {
			continue
		}
		ns := strings.TrimSuffix(strings.TrimPrefix(base, "part-"), ".parquet")
		if dash := strings.IndexByte(ns, '-'); dash >= 0 {
			suffix := ns[dash+1:]
			if suffix == "" {
				continue
			}
			valid := true
			for _, r := range suffix {
				if r < '0' || r > '9' {
					valid = false
					break
				}
			}
			if !valid {
				continue
			}
			ns = ns[:dash]
			if ns == "" {
				continue
			}
		}
		iv, err := strconv.Atoi(ns)
		if err != nil {
			continue
		}
		if iv > max {
			max = iv
		}
	}
	return max
}

func resolveCursorDomain(opts jobopts.Options, tasks []db.Task) connectors.CursorDomain {
	if d := connectors.NormalizeCursorDomain(opts.CursorDomain); d != connectors.CursorDomainUnknown {
		return d
	}
	for _, t := range tasks {
		var part struct {
			Type         string `json:"type"`
			CursorDomain string `json:"cursor_domain"`
		}
		if err := json.Unmarshal(t.PartitionSpec, &part); err != nil {
			continue
		}
		if d := connectors.NormalizeCursorDomain(part.CursorDomain); d != connectors.CursorDomainUnknown {
			return d
		}
		switch part.Type {
		case "sql_int_range", "mssql_int_range":
			return connectors.CursorDomainInt64
		}
	}
	return connectors.CursorDomainUnknown
}

// deriveMaxCursor scans the task metadata for the maximum high-water mark (HWM) value observed across all tasks of the run.
func deriveMaxCursor(tasks []db.Task, domain connectors.CursorDomain) string {
	max := ""
	forEachTaskParquetObject(tasks, func(o map[string]any) {
		mv, _ := o["max_hwm"].(string)
		if mv == "" {
			return
		}
		if max == "" || connectors.CompareCursorValues(domain, max, mv) < 0 {
			max = mv
		}
	})
	return strings.TrimSpace(max)
}

// datasetPrefixForJob derives the dataset prefix for the job based on the source engine, target metadata, and job options.
func datasetPrefixForJob(job db.Job, srcEngine string, opts jobopts.Options, tgtMeta map[string]any) string {
	targetPrefix := ""
	if tgtMeta != nil {
		if p, ok := tgtMeta["prefix"].(string); ok {
			targetPrefix = p
		}
	}

	sourceName := strings.TrimSpace(opts.SourceName)
	if sourceName == "" {
		if opts.NormalizedSourceMode() == "query" {
			if hash := strings.TrimSpace(opts.QueryHash); hash != "" {
				sourceName = "query_" + hash
			} else {
				sourceName = "query"
			}
		} else {
			sourceName = strings.TrimSpace(opts.Table)
		}
	}
	if sourceName == "" {
		sourceName = strings.TrimSpace(job.TargetTable)
	}
	return dataset.Prefix(targetPrefix, srcEngine, sourceName)
}

// defaultFullRunRetainCount is the default number of successful full-refresh runs
// to keep in S3. Override via ORABBIT_FULL_RUN_RETAIN_COUNT env var.
const defaultFullRunRetainCount = 1 //nolint:unused // see purgeStaleFullRunsForRun

// purgeStaleFullRunsForRun deletes the S3 objects for obsolete full-refresh run
// directories after a new full-refresh run has been successfully committed and
// published to the Iceberg catalog.
//
// Safety guarantees:
//   - Only called after ICEBERG_REGISTRATION_SUCCEEDED (new dataset fully published).
//   - Only operates on non-incremental (full-refresh) jobs.
//   - Keeps the latest retainCount runs intact.
//   - Failures are best-effort: logged but never propagated to the caller.
//   - Idempotent: re-running on already-purged directories is a no-op.
//
// It is not called anywhere yet, so ORABBIT_FULL_RUN_RETAIN_COUNT has no
// effect; enabling it deletes published data and needs a deliberate decision.
//
//nolint:unused
func (s *Server) purgeStaleFullRunsForRun(ctx context.Context, runID string) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return
	}

	run, err := s.st.GetRun(ctx, runID)
	if err != nil {
		s.log.Warn("full-run purge: failed to load run",
			slog.String("run_id", runID),
			slog.String("err", err.Error()),
		)
		return
	}

	resolved, err := s.resolveRun(ctx, run)
	if err != nil {
		s.log.Warn("full-run purge: failed to resolve run configuration",
			slog.String("run_id", runID),
			slog.String("job_id", run.JobID),
			slog.String("err", err.Error()),
		)
		return
	}
	job := resolved.config.Job

	// Only apply retention to full (non-incremental) jobs.
	if job.Incremental {
		return
	}

	// Determine how many successful full runs to retain.
	retainCount := defaultFullRunRetainCount
	if raw := strings.TrimSpace(os.Getenv("ORABBIT_FULL_RUN_RETAIN_COUNT")); raw != "" {
		if n, err2 := strconv.Atoi(raw); err2 == nil && n >= 1 {
			retainCount = n
		}
	}

	// Collect all succeeded runs for this job, ordered oldest-first.
	allRuns, err := s.st.ListSucceededRunsForJob(ctx, job.ID)
	if err != nil {
		s.log.Warn("full-run purge: failed to list succeeded runs",
			slog.String("run_id", runID),
			slog.String("job_id", job.ID),
			slog.String("err", err.Error()),
		)
		return
	}

	if len(allRuns) <= retainCount {
		// Nothing to purge yet.
		return
	}

	// to_purge = all except the latest retainCount runs.
	toPurge := allRuns[:len(allRuns)-retainCount]

	cfg, err := s.targetS3Config(ctx, resolved)
	if err != nil {
		s.log.Warn("full-run purge: failed to resolve target",
			slog.String("job_id", job.ID),
			slog.String("err", err.Error()),
		)
		return
	}
	u, err := s3io.New(ctx, cfg)
	if err != nil {
		s.log.Warn("full-run purge: failed to create S3 client",
			slog.String("job_id", job.ID),
			slog.String("err", err.Error()),
		)
		return
	}
	basePrefix := strings.TrimSuffix(strings.TrimSpace(resolved.prefix), "/")
	if basePrefix == "" {
		s.log.Warn("full-run purge: could not resolve dataset prefix", slog.String("job_id", job.ID))
		return
	}

	totalPurged := 0
	for _, staleRun := range toPurge {
		// Guard: never delete the current (just-published) run.
		if staleRun.ID == runID {
			continue
		}

		runPrefix := basePrefix + "/_runs/run-" + staleRun.ID + "/"
		keys, listErr := u.ListKeys(ctx, runPrefix)
		if listErr != nil {
			s.log.Warn("full-run purge: list objects failed",
				slog.String("run_id", staleRun.ID),
				slog.String("prefix", runPrefix),
				slog.String("err", listErr.Error()),
			)
			continue
		}
		if len(keys) == 0 {
			// Already purged or never uploaded.
			continue
		}

		deleted, delErr := u.DeleteObjects(ctx, keys)
		if delErr != nil {
			s.log.Warn("full-run purge: delete objects failed",
				slog.String("run_id", staleRun.ID),
				slog.String("prefix", runPrefix),
				slog.String("err", delErr.Error()),
			)
			continue
		}

		totalPurged += deleted
		s.log.Info("full-run purge: deleted stale run objects",
			slog.String("current_run_id", runID),
			slog.String("purged_run_id", staleRun.ID),
			slog.String("prefix", runPrefix),
			slog.Int("objects_deleted", deleted),
		)
	}

	if totalPurged > 0 {
		s.log.Info("full-run purge: complete",
			slog.String("run_id", runID),
			slog.String("job_id", job.ID),
			slog.Int("runs_purged", len(toPurge)),
			slog.Int("total_objects_deleted", totalPurged),
		)
	}
}
