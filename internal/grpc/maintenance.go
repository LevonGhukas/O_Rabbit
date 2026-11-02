package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// maintenanceLease bounds how long a crashed master blocks an operation; a
// live holder renews it before every destructive step.
const maintenanceLease = 2 * time.Minute

// Orphan cleanup defaults and bounds.
const (
	DefaultOrphanMinAge = 7 * 24 * time.Hour
	MinOrphanMinAge     = time.Hour
)

// Compaction defaults.
const (
	defaultCompactionTargetBytes = 256 << 20
	defaultCompactionMinInputs   = 3
	defaultCompactionMaxInputs   = 1000
)

// OrphanCleanupParams are the request parameters of an orphan cleanup.
type OrphanCleanupParams struct {
	MinAgeHours float64 `json:"min_age_hours,omitempty"`
}

// CompactionParams are the request parameters of a compaction.
type CompactionParams struct {
	TargetFileBytes int64 `json:"target_file_bytes,omitempty"`
	SmallFileBytes  int64 `json:"small_file_bytes,omitempty"`
	MinInputFiles   int   `json:"min_input_files,omitempty"`
	MaxInputFiles   int   `json:"max_input_files,omitempty"`
}

// compactionTaskSpec is the partition_spec_json of a compaction task.
type compactionTaskSpec struct {
	Type                 string            `json:"type"`
	OutputPart           int64             `json:"output_part"`
	TargetFileBytes      int64             `json:"target_file_bytes"`
	IcebergPartitionSpec json.RawMessage   `json:"iceberg_partition_spec,omitempty"`
	Inputs               []compactionInput `json:"inputs"`
}

type compactionInput struct {
	Key     string `json:"key"`
	Size    int64  `json:"size"`
	Records int64  `json:"records"`
}

// CompactionTaskType is the partition spec type of compaction tasks.
const CompactionTaskType = "iceberg_compact"

// maintenanceTarget is the table an operation works on, resolved from the
// job's newest registered run.
type maintenanceTarget struct {
	sourceRun db.Run
	resolved  resolvedRun
	reg       icebergreg.RunConfig
	s3        s3io.Config
	prefix    string // dataset prefix, without slashes at either end
}

// ProcessMaintenanceOnce runs at most one claimed maintenance operation and
// reports whether it found one. Only the leader runs operations, and every
// step that changes state presents the claim's fencing token.
func (s *Server) ProcessMaintenanceOnce(ctx context.Context) (bool, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return false, err
	}
	if _, err := s.st.FailSubmittedCompactions(ctx, s.nowFn()); err != nil {
		return false, err
	}
	op, ok, err := s.st.ClaimMaintenanceOperation(ctx, s.nowFn(), maintenanceLease)
	if err != nil || !ok {
		return ok, err
	}
	log := s.log.With(slog.String("maintenance_id", op.ID), slog.String("kind", op.Kind), slog.String("job_id", op.JobID), slog.Bool("dry_run", op.DryRun))
	log.Info("maintenance operation started")
	switch op.Kind {
	case db.MaintenanceOrphanCleanup:
		err = s.runOrphanCleanup(ctx, op)
	case db.MaintenanceCompaction:
		err = s.planCompaction(ctx, op)
	default:
		err = fmt.Errorf("unknown maintenance kind %q", op.Kind)
	}
	if err != nil {
		if errors.Is(err, db.ErrMaintenanceFenced) || ctx.Err() != nil {
			// Another claim owns the operation now, or the master is
			// stopping; the lease expiry hands it to the next holder.
			log.Warn("maintenance operation interrupted", slog.String("err", err.Error()))
			return true, nil
		}
		class := string(failure.ClassOf(err))
		log.Error("maintenance operation failed", slog.String("failure_class", class), slog.String("err", err.Error()))
		if finishErr := s.st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, db.MaintenanceFailed, "", nil, class, db.RedactCredentials(err.Error()), s.nowFn()); finishErr != nil && !errors.Is(finishErr, db.ErrMaintenanceFenced) {
			return true, finishErr
		}
	}
	return true, nil
}

func (s *Server) resolveMaintenanceTarget(ctx context.Context, jobID string) (maintenanceTarget, error) {
	run, ok, err := s.st.LatestRegisteredRun(ctx, jobID)
	if err != nil {
		return maintenanceTarget{}, err
	}
	if !ok {
		return maintenanceTarget{}, failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf("job %s has no run with a registered Iceberg table", jobID))
	}
	resolved, err := s.resolveRun(ctx, run)
	if err != nil {
		return maintenanceTarget{}, err
	}
	reg, err := icebergreg.ParseRunConfig(run.RegistrationConfigJSON)
	if err != nil {
		return maintenanceTarget{}, err
	}
	s3cfg, err := s.targetS3Config(ctx, resolved)
	if err != nil {
		return maintenanceTarget{}, err
	}
	return maintenanceTarget{sourceRun: run, resolved: resolved, reg: reg, s3: s3cfg, prefix: normalizePrefix(resolved.prefix)}, nil
}

// loadMaintenanceTable loads the table and refuses one whose location is not
// the dataset prefix: listing that prefix would then not be the table's files.
func (s *Server) loadMaintenanceTable(ctx context.Context, t maintenanceTarget) (*icetable.Table, error) {
	tbl, err := icebergreg.LoadMaintenanceTable(ctx, t.reg, t.s3)
	if err != nil {
		return nil, err
	}
	want := "s3://" + t.s3.Bucket + "/" + t.prefix
	if strings.TrimSuffix(tbl.Location(), "/") != want {
		return nil, failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf("table %s is located at %s, not at the dataset prefix %s", t.reg.Table, tbl.Location(), want))
	}
	return tbl, nil
}

func tableEvidence(t maintenanceTarget, tbl *icetable.Table) db.MaintenanceTable {
	return db.MaintenanceTable{
		SourceRunID:      t.sourceRun.ID,
		Identifier:       t.reg.Table,
		UUID:             tbl.Metadata().TableUUID().String(),
		Location:         tbl.Location(),
		SnapshotID:       icebergreg.SnapshotIDString(tbl),
		MetadataLocation: tbl.MetadataLocation(),
	}
}

// runOrphanCleanup lists the table location, subtracts every file a retained
// snapshot references, and reports what is left with a reason per object.
// When not a dry run, it deletes only proven orphans, and only while the
// table is unchanged since the listing and the dataset has no active run or
// unfinished registration.
func (s *Server) runOrphanCleanup(ctx context.Context, op db.MaintenanceOperation) error {
	var params OrphanCleanupParams
	if err := json.Unmarshal(op.Params, &params); err != nil {
		return failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf("orphan cleanup parameters: %w", err))
	}
	minAge := DefaultOrphanMinAge
	if params.MinAgeHours > 0 {
		minAge = time.Duration(params.MinAgeHours * float64(time.Hour))
	}
	if minAge < MinOrphanMinAge {
		return failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf("min_age_hours must be at least %s", MinOrphanMinAge))
	}
	t, err := s.resolveMaintenanceTarget(ctx, op.JobID)
	if err != nil {
		return err
	}
	tbl, err := s.loadMaintenanceTable(ctx, t)
	if err != nil {
		return err
	}
	refs, snapshots, err := icebergreg.TableReferences(ctx, tbl, icebergreg.BucketKeys(t.s3.Bucket))
	if err != nil {
		return err
	}
	store, err := s3io.New(ctx, t.s3)
	if err != nil {
		return err
	}
	objects, err := store.ListObjects(ctx, t.prefix+"/")
	if err != nil {
		return fmt.Errorf("list table location: %w", err)
	}
	now := s.nowFn()
	decisions, referenced, skipped := icebergreg.ClassifyOrphans(objects, icebergreg.OrphanPolicy{
		TableKeyPrefix: t.prefix,
		MinAge:         minAge,
		Now:            now,
		References:     refs,
		Snapshots:      snapshots,
		RunState:       func(runID string) icebergreg.RunState { return s.maintenanceRunState(ctx, runID) },
	})
	evidence := tableEvidence(t, tbl)
	candidates := make([]db.MaintenanceCandidate, 0, len(decisions))
	orphans := 0
	var orphanBytes int64
	for _, d := range decisions {
		d.Evidence["metadata_location"] = evidence.MetadataLocation
		d.Evidence["snapshot_id"] = evidence.SnapshotID
		raw, _ := json.Marshal(d.Evidence)
		c := db.MaintenanceCandidate{
			ObjectKey:    d.Object.Key,
			SizeBytes:    d.Object.Size,
			LastModified: db.FormatTimestamp(d.Object.LastModified),
			AgeSeconds:   int64(d.Age / time.Second),
			Decision:     "RETAINED",
			Reason:       d.Reason,
			Evidence:     raw,
			Status:       "RETAINED",
		}
		if d.Orphan {
			c.Decision, c.Status = "ORPHAN", "WOULD_DELETE"
			orphans++
			orphanBytes += d.Object.Size
		}
		candidates = append(candidates, c)
	}
	report := map[string]any{
		"objects_listed":     len(objects),
		"referenced_files":   referenced,
		"non_data_objects":   skipped,
		"retained_files":     len(candidates) - orphans,
		"orphan_files":       orphans,
		"orphan_bytes":       orphanBytes,
		"snapshots_examined": snapshots,
		"min_age_hours":      minAge.Hours(),
		"deleted_files":      0,
	}
	if err := s.st.RecordMaintenancePlan(ctx, op.ID, op.LeaseToken, evidence, candidates, report, now); err != nil {
		return err
	}
	if op.DryRun || orphans == 0 {
		return s.st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, db.MaintenanceSucceeded, "", report, "", "", s.nowFn())
	}

	abort := func(class, message string) error {
		report["abort_reason"] = class
		return s.st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, db.MaintenanceAborted, "", report, class, message, s.nowFn())
	}
	if busy, reason, err := s.datasetBusy(ctx, t.sourceRun.DatasetKey); err != nil {
		return err
	} else if busy {
		return abort("DATASET_BUSY", reason)
	}
	unchanged := func() (bool, error) {
		fresh, err := s.loadMaintenanceTable(ctx, t)
		if err != nil {
			return false, err
		}
		return fresh.MetadataLocation() == evidence.MetadataLocation, nil
	}
	if same, err := unchanged(); err != nil {
		return err
	} else if !same {
		return abort("CONCURRENT_COMMIT", "the table changed after the listing; nothing was deleted, run the cleanup again")
	}

	deleted, failed := 0, 0
	for i, c := range candidates {
		if c.Decision != "ORPHAN" {
			continue
		}
		// The lease proves this master still owns the operation; the table
		// check that no commit landed since the references were read.
		if err := s.st.RenewMaintenanceLease(ctx, op.ID, op.LeaseToken, s.nowFn(), maintenanceLease); err != nil {
			return err
		}
		if i > 0 && i%100 == 0 {
			if same, err := unchanged(); err != nil {
				return err
			} else if !same {
				report["deleted_files"], report["delete_failed_files"] = deleted, failed
				return abort("CONCURRENT_COMMIT", fmt.Sprintf("the table changed during cleanup; stopped after deleting %d files", deleted))
			}
		}
		status := s.deleteOrphan(ctx, store, c)
		switch status {
		case "DELETED":
			deleted++
		case "DELETE_FAILED":
			failed++
		}
		if err := s.st.SetMaintenanceCandidateStatus(ctx, op.ID, op.LeaseToken, c.ObjectKey, status, s.nowFn()); err != nil {
			return err
		}
	}
	report["deleted_files"], report["delete_failed_files"] = deleted, failed
	status, class := db.MaintenanceSucceeded, ""
	if failed > 0 {
		status, class = db.MaintenanceFailed, "ORPHAN_DELETE_FAILED"
	}
	return s.st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, status, "", report, class, "", s.nowFn())
}

// deleteOrphan deletes one orphan if it is still exactly the listed object,
// and reports what happened.
func (s *Server) deleteOrphan(ctx context.Context, store *s3io.Uploader, c db.MaintenanceCandidate) string {
	cur, found, err := store.StatObject(ctx, c.ObjectKey)
	if err != nil {
		return "DELETE_FAILED"
	}
	if !found {
		return "DELETED"
	}
	if cur.Size != c.SizeBytes || db.FormatTimestamp(cur.LastModified) != c.LastModified {
		// Rewritten since the listing: no longer the object that was proven
		// an orphan.
		return "CHANGED"
	}
	if err := store.DeleteExactObject(ctx, c.ObjectKey, ""); err != nil {
		s.log.Warn("orphan delete failed", slog.String("key", c.ObjectKey), slog.String("err", err.Error()))
		return "DELETE_FAILED"
	}
	if _, still, err := store.StatObject(ctx, c.ObjectKey); err != nil || still {
		return "DELETE_FAILED"
	}
	return "DELETED"
}

// datasetBusy reports whether the dataset has an active run or an unfinished
// registration, whose files may be on their way into the table.
func (s *Server) datasetBusy(ctx context.Context, datasetKey string) (bool, string, error) {
	if active, ok, err := s.st.FindActiveRunByDatasetKey(ctx, datasetKey); err != nil {
		return false, "", err
	} else if ok {
		return true, fmt.Sprintf("run %s is %s", active.ID, active.Status), nil
	}
	n, err := s.st.DatasetRegistrationsInFlight(ctx, datasetKey)
	if err != nil {
		return false, "", err
	}
	if n > 0 {
		return true, fmt.Sprintf("%d registration(s) have not finished", n), nil
	}
	return false, "", nil
}

// maintenanceRunState classifies the run that wrote an object. Errors are
// reported as UNKNOWN, which keeps the object.
func (s *Server) maintenanceRunState(ctx context.Context, runID string) icebergreg.RunState {
	run, err := s.st.GetRun(ctx, runID)
	if err != nil {
		return icebergreg.RunStateUnknown
	}
	switch run.Status {
	case "PLANNING", "RUNNING", "COMMITTING":
		return icebergreg.RunStateActive
	case "CANCELED":
		return icebergreg.RunStateCanceled
	case "FAILED":
		return icebergreg.RunStateFailed
	case "SUCCEEDED":
	default:
		return icebergreg.RunStateUnknown
	}
	if _, ok, err := s.st.GetMaintenanceOperationForRun(ctx, runID); err != nil {
		return icebergreg.RunStateUnknown
	} else if ok {
		// A compaction run's files reach the table through its own commit.
		return icebergreg.RunStateRegistered
	}
	status, err := s.st.RunRegistrationStatus(ctx, runID)
	switch {
	case err != nil:
		return icebergreg.RunStateUnknown
	case status == "REGISTERED":
		return icebergreg.RunStateRegistered
	case status == "":
		return icebergreg.RunStateNotRegistered
	default:
		return icebergreg.RunStateRegistrationPending
	}
}

// planCompaction selects small files and, unless this is a dry run, starts
// a compaction run whose workers rewrite each group into new files. The
// run's commit replaces the inputs (see commitCompaction).
func (s *Server) planCompaction(ctx context.Context, op db.MaintenanceOperation) error {
	var params CompactionParams
	if err := json.Unmarshal(op.Params, &params); err != nil {
		return failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf("compaction parameters: %w", err))
	}
	// A previous holder crashed after starting the run: follow that run
	// instead of planning (and blocking on) a second one.
	if op.RunID != "" {
		if run, err := s.st.GetRun(ctx, op.RunID); err == nil {
			switch run.Status {
			case "PLANNING", "RUNNING", "COMMITTING":
				return s.st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, db.MaintenanceSubmitted, run.ID, nil, "", "", s.nowFn())
			case "SUCCEEDED":
				return s.st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, db.MaintenanceSucceeded, run.ID, nil, "", "", s.nowFn())
			}
		}
	}
	t, err := s.resolveMaintenanceTarget(ctx, op.JobID)
	if err != nil {
		return err
	}
	opts := compactionOptions(params, t)
	if opts.SmallFileBytes > opts.TargetFileBytes || opts.SmallFileBytes <= 0 {
		return failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf("small_file_bytes must be positive and at most target_file_bytes"))
	}
	tbl, err := s.loadMaintenanceTable(ctx, t)
	if err != nil {
		return err
	}
	files, err := icebergreg.CompactionFiles(ctx, tbl, icebergreg.BucketKeys(t.s3.Bucket))
	if err != nil {
		return err
	}
	keys := make([]string, len(files))
	for i, f := range files {
		keys[i] = f.Key
	}
	fingerprints, err := s.st.ArtifactFingerprints(ctx, keys)
	if err != nil {
		return err
	}
	groups, skips := icebergreg.PlanCompaction(files, fingerprints, opts)

	var candidates []db.MaintenanceCandidate
	inputFiles := 0
	for gi, g := range groups {
		for _, f := range g.Inputs {
			raw, _ := json.Marshal(map[string]any{"path": f.Path, "group": gi + 1, "partition": g.Partition, "records": f.Records, "group_files": len(g.Inputs), "group_bytes": g.Bytes})
			candidates = append(candidates, db.MaintenanceCandidate{ObjectKey: f.Key, SizeBytes: f.Size, Decision: "COMPACT", Reason: "SMALL_FILE", Evidence: raw, Status: "PLANNED"})
			inputFiles++
		}
	}
	for _, sk := range skips {
		if sk.Reason == "NOT_SMALL" {
			continue // most files; counted in the report
		}
		raw, _ := json.Marshal(map[string]any{"path": sk.File.Path, "partition": sk.File.Partition, "records": sk.File.Records})
		candidates = append(candidates, db.MaintenanceCandidate{ObjectKey: sk.File.Key, SizeBytes: sk.File.Size, Decision: "RETAINED", Reason: sk.Reason, Evidence: raw, Status: "RETAINED"})
	}
	report := map[string]any{
		"data_files":        len(files),
		"groups":            len(groups),
		"input_files":       inputFiles,
		"skipped_files":     len(skips),
		"target_file_bytes": opts.TargetFileBytes,
		"small_file_bytes":  opts.SmallFileBytes,
		"min_input_files":   opts.MinInputFiles,
		"max_input_files":   opts.MaxInputFiles,
	}
	evidence := tableEvidence(t, tbl)
	if err := s.st.RecordMaintenancePlan(ctx, op.ID, op.LeaseToken, evidence, candidates, report, s.nowFn()); err != nil {
		return err
	}
	if op.DryRun || len(groups) == 0 {
		return s.st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, db.MaintenanceSucceeded, "", report, "", "", s.nowFn())
	}
	specJSON, err := t.reg.WorkerPartitionSpecJSON()
	if err != nil {
		return err
	}
	runID, err := s.startCompactionRun(ctx, op, t, groups, opts.TargetFileBytes, specJSON)
	if err != nil {
		if errors.Is(err, db.ErrActiveDatasetRun) {
			report["abort_reason"] = "DATASET_BUSY"
			return s.st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, db.MaintenanceAborted, "", report, "DATASET_BUSY", "the dataset has an active run; try again when it finishes", s.nowFn())
		}
		return err
	}
	report["run_id"] = runID
	return s.st.FinishMaintenanceOperation(ctx, op.ID, op.LeaseToken, db.MaintenanceSubmitted, runID, report, "", "", s.nowFn())
}

func compactionOptions(p CompactionParams, t maintenanceTarget) icebergreg.CompactionOptions {
	opts := icebergreg.CompactionOptions{TargetFileBytes: p.TargetFileBytes, SmallFileBytes: p.SmallFileBytes, MinInputFiles: p.MinInputFiles, MaxInputFiles: p.MaxInputFiles}
	if opts.TargetFileBytes <= 0 {
		opts.TargetFileBytes = t.reg.TargetFileSize
	}
	if opts.TargetFileBytes <= 0 {
		opts.TargetFileBytes = t.resolved.opts.TargetFileBytes
	}
	if opts.TargetFileBytes <= 0 {
		opts.TargetFileBytes = defaultCompactionTargetBytes
	}
	if opts.SmallFileBytes <= 0 {
		opts.SmallFileBytes = opts.TargetFileBytes * 3 / 4
	}
	if opts.MinInputFiles <= 0 {
		opts.MinInputFiles = defaultCompactionMinInputs
	}
	if opts.MaxInputFiles <= 0 {
		opts.MaxInputFiles = defaultCompactionMaxInputs
	}
	return opts
}

// startCompactionRun creates the run and its tasks. The run ID is recorded on
// the operation before any task exists, so the run's commit can never be
// mistaken for an ordinary export. The run uses the dataset key, so it
// excludes exports into the same dataset while it is active.
func (s *Server) startCompactionRun(ctx context.Context, op db.MaintenanceOperation, t maintenanceTarget, groups []icebergreg.CompactionGroup, targetBytes int64, specJSON string) (string, error) {
	run := db.Run{
		ID:            newID(),
		JobID:         op.JobID,
		DatasetKey:    t.sourceRun.DatasetKey,
		Status:        "PLANNING",
		CorrelationID: newID(),
		StartedAt:     db.FormatTimestamp(s.nowFn()),
	}
	if err := s.st.CreateRun(ctx, run); err != nil {
		return "", err
	}
	fail := func(cause error) (string, error) {
		msg := db.RedactCredentials(cause.Error())
		_ = s.st.UpdateRunStatus(context.WithoutCancel(ctx), run.ID, "FAILED", true, &msg)
		return "", cause
	}
	if err := s.st.SetMaintenanceRunID(ctx, op.ID, op.LeaseToken, run.ID, s.nowFn()); err != nil {
		return fail(err)
	}
	tasks := make([]db.TaskInsert, len(groups))
	for i, g := range groups {
		spec := compactionTaskSpec{Type: CompactionTaskType, OutputPart: int64(i + 1), TargetFileBytes: targetBytes}
		if specJSON != "" {
			spec.IcebergPartitionSpec = json.RawMessage(specJSON)
		}
		for _, f := range g.Inputs {
			spec.Inputs = append(spec.Inputs, compactionInput{Key: f.Key, Size: f.Size, Records: f.Records})
		}
		raw, err := json.Marshal(spec)
		if err != nil {
			return fail(err)
		}
		tasks[i] = db.TaskInsert{ID: newID(), RunID: run.ID, TaskIndex: i + 1, PartitionSpec: raw, Status: "PENDING"}
	}
	if _, err := s.st.StartRunWithTasks(ctx, run, tasks); err != nil {
		return fail(err)
	}
	return run.ID, nil
}

// commitCompaction publishes a compaction run: it verifies the workers'
// artifacts like any commit, then replaces the inputs with them in one
// Iceberg snapshot. It does not touch the dataset's export state (_state.json
// or the high-water mark): compaction changes files, not data.
func (s *Server) commitCompaction(ctx context.Context, run db.Run, op db.MaintenanceOperation) error {
	terminal := func(class string, err error) error {
		return &classifiedCommitError{class: class, component: "compaction", err: err}
	}
	tasks, err := s.st.ListTasksForRun(ctx, run.ID)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if task.Status != "SUCCEEDED" {
			return fmt.Errorf("commit verification: task %s is %s", task.ID, task.Status)
		}
	}
	records, err := s.st.ListArtifactsForRun(ctx, run.ID)
	if err != nil {
		return err
	}
	resolved, err := s.resolveRun(ctx, run)
	if err != nil {
		return err
	}
	s3cfg, err := s.targetS3Config(ctx, resolved)
	if err != nil {
		return err
	}
	store, err := s.newCommitObjectStoreFn(ctx, s3cfg)
	if err != nil {
		return err
	}
	if err := verifyArtifacts(ctx, store, records); err != nil {
		return err
	}
	outputs := make([]string, 0, len(records))
	var outputRows int64
	for _, r := range records {
		if err := r.Validate(); err != nil {
			return terminal(commitFailureIntegrity, fmt.Errorf("commit artifact %s: %w", r.ObjectKey, err))
		}
		outputs = append(outputs, "s3://"+s3cfg.Bucket+"/"+r.ObjectKey)
		outputRows += r.RowCount
	}
	sort.Strings(outputs)

	candidates, err := s.st.ListMaintenanceCandidates(ctx, op.ID)
	if err != nil {
		return err
	}
	var inputs []string
	for _, c := range candidates {
		if c.Decision != "COMPACT" {
			continue
		}
		var ev struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal(c.Evidence, &ev); err != nil || ev.Path == "" {
			return terminal(commitFailureValidation, fmt.Errorf("compaction input %s has no table path", c.ObjectKey))
		}
		inputs = append(inputs, ev.Path)
	}
	sourceRun, err := s.st.GetRun(ctx, op.SourceRunID)
	if err != nil {
		return err
	}
	reg, err := icebergreg.ParseRunConfig(sourceRun.RegistrationConfigJSON)
	if err != nil {
		return err
	}
	load := func(ctx context.Context) (*icetable.Table, error) {
		tbl, err := icebergreg.LoadMaintenanceTable(ctx, reg, s3cfg)
		if err != nil {
			return nil, err
		}
		if got := tbl.Metadata().TableUUID().String(); got != op.TableUUID {
			return nil, failure.NewFailure(failure.FailureCatalogConflict, false, true, fmt.Errorf("table %s was replaced (uuid %s, planned %s)", reg.Table, got, op.TableUUID))
		}
		return tbl, nil
	}
	if s.loadCompactionTableFn != nil {
		load = s.loadCompactionTableFn
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("compaction\x00%s\x00%s\x00%s", run.ID, strings.Join(inputs, "\x00"), strings.Join(outputs, "\x00"))))
	commitID := hex.EncodeToString(digest[:])
	props := map[string]string{"orabbit.commit_id": commitID, "orabbit.maintenance_id": op.ID}
	if _, err := icebergreg.CommitCompaction(ctx, load, reg, run.ID, inputs, outputs, outputRows, props); err != nil {
		var f *failure.Failure
		if errors.As(err, &f) && !f.Retryable {
			_ = s.st.CompleteCompactionOperation(context.WithoutCancel(ctx), run.ID, db.MaintenanceAborted, string(f.Class), db.RedactCredentials(err.Error()), s.nowFn())
			return terminal(commitFailureValidation, err)
		}
		return err
	}
	intent, _ := json.Marshal(map[string]any{"kind": "compaction", "maintenance_id": op.ID, "commit_id": commitID, "inputs": inputs, "outputs": outputs})
	if err := s.st.SaveCommitIntent(ctx, run.ID, commitID, intent); err != nil {
		return err
	}
	if err := s.st.SetCommitPhase(ctx, run.ID, "VERIFIED"); err != nil {
		return err
	}
	return s.st.CompleteCompactionOperation(ctx, run.ID, db.MaintenanceSucceeded, "", "", s.nowFn())
}
