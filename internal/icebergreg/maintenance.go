package icebergreg

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	iceberg "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// CompactionOperation marks the snapshots a compaction commits, together with
// the run ID property, so a retried commit can recognize its own snapshot.
const CompactionOperation = "compaction"

const operationProperty = "orabbit.operation"

// LoadMaintenanceTable loads a rest-go table for maintenance. Unlike
// registration it never creates namespaces or tables.
func LoadMaintenanceTable(ctx context.Context, reg RunConfig, datasetS3 s3io.Config) (*icetable.Table, error) {
	reg = reg.Normalize()
	if !reg.Enabled || reg.Engine != "rest-go" {
		return nil, failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf("maintenance supports Iceberg tables registered with engine rest-go"))
	}
	if err := validateTableIdentifier(reg.Table); err != nil {
		return nil, err
	}
	cat, ident, err := newRESTCatalog(ctx, datasetS3.Bucket, reg, registrationS3(datasetS3, reg), reg.Table)
	if err != nil {
		return nil, err
	}
	return cat.LoadTable(ctx, ident)
}

// ObjectKey returns the object key of an Iceberg file path in bucket, or
// false when the path is in another bucket or not an S3 URI.
func ObjectKey(path, bucket string) (string, bool) {
	for _, scheme := range []string{"s3://", "s3a://", "s3n://"} {
		if rest, ok := strings.CutPrefix(path, scheme); ok {
			b, key, found := strings.Cut(rest, "/")
			if !found || b != bucket || key == "" {
				return "", false
			}
			return key, true
		}
	}
	return "", false
}

// KeyOf maps an Iceberg file path to an object key; false means the path is
// not in the store being maintained.
type KeyOf func(path string) (string, bool)

// BucketKeys is the KeyOf for s3:// paths in bucket.
func BucketKeys(bucket string) KeyOf {
	return func(path string) (string, bool) { return ObjectKey(path, bucket) }
}

// TableReferences returns, for every file a retained snapshot references
// (data and delete files, live or removed in that snapshot), the ID of one
// snapshot that references it, keyed by object key. Counting files a
// snapshot removed is conservative: its parent still needs them.
func TableReferences(ctx context.Context, tbl *icetable.Table, keyOf KeyOf) (map[string]int64, int, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, 0, err
	}
	refs := map[string]int64{}
	seenManifest := map[string]bool{}
	snapshots := tbl.Metadata().Snapshots()
	for _, snap := range snapshots {
		manifests, err := snap.Manifests(fs)
		if err != nil {
			return nil, 0, fmt.Errorf("snapshot %d manifests: %w", snap.SnapshotID, err)
		}
		for _, mf := range manifests {
			// Snapshots share manifests; each is read once.
			if seenManifest[mf.FilePath()] {
				continue
			}
			seenManifest[mf.FilePath()] = true
			entries, err := mf.FetchEntries(fs, false)
			if err != nil {
				return nil, 0, fmt.Errorf("manifest %s: %w", mf.FilePath(), err)
			}
			for _, e := range entries {
				if key, ok := keyOf(e.DataFile().FilePath()); ok {
					if _, dup := refs[key]; !dup {
						refs[key] = snap.SnapshotID
					}
				}
			}
		}
	}
	return refs, len(snapshots), nil
}

// RunState is what O_Rabbit's own records say about the run that wrote an
// object under <prefix>/_runs/run-<id>/.
type RunState string

const (
	RunStateRegistered          RunState = "REGISTERED"           // published and registered
	RunStateFailed              RunState = "FAILED"               // never published
	RunStateActive              RunState = "ACTIVE"               // planning, running or committing
	RunStateRegistrationPending RunState = "REGISTRATION_PENDING" // published, registration not REGISTERED
	RunStateNotRegistered       RunState = "NOT_REGISTERED"       // published without Iceberg
	RunStateCanceled            RunState = "CANCELED"             // canceled-object cleanup owns it
	RunStateUnknown             RunState = "UNKNOWN"              // no such run
)

// OrphanDecision is the verdict on one listed object.
type OrphanDecision struct {
	Object   s3io.ObjectInfo
	Age      time.Duration
	Orphan   bool
	Reason   string
	Evidence map[string]any
}

// OrphanPolicy is the input to ClassifyOrphans.
type OrphanPolicy struct {
	TableKeyPrefix string // object key prefix of the table location, without a trailing slash
	MinAge         time.Duration
	Now            time.Time
	References     map[string]int64
	Snapshots      int
	RunState       func(runID string) RunState
}

// ClassifyOrphans decides, for each object under the table location, whether
// it is a proven orphan. Only Parquet files outside metadata/ can be orphans,
// and only when no retained snapshot references them, they are older than
// MinAge, and no run that may still need them owns them. Referenced objects
// and non-data files are counted but not returned.
func ClassifyOrphans(objects []s3io.ObjectInfo, p OrphanPolicy) (decisions []OrphanDecision, referenced, skipped int) {
	prefix := strings.TrimSuffix(p.TableKeyPrefix, "/") + "/"
	for _, obj := range objects {
		rel, inTable := strings.CutPrefix(obj.Key, prefix)
		if !inTable || strings.HasPrefix(rel, "metadata/") || !strings.HasSuffix(obj.Key, ".parquet") {
			skipped++
			continue
		}
		if _, ok := p.References[obj.Key]; ok {
			referenced++
			continue
		}
		d := OrphanDecision{Object: obj, Age: p.Now.Sub(obj.LastModified), Evidence: map[string]any{
			"referenced":         false,
			"snapshots_examined": p.Snapshots,
		}}
		runID := runIDFromKey(rel)
		if runID != "" {
			d.Evidence["run_id"] = runID
		}
		switch {
		case strings.Contains(rel, "/_staging/") || strings.HasPrefix(rel, "_staging/"):
			d.Reason = "STAGING_OBJECT"
		case d.Age < p.MinAge:
			d.Reason = "YOUNGER_THAN_MIN_AGE"
		case runID != "":
			state := p.RunState(runID)
			d.Evidence["run_state"] = string(state)
			switch state {
			case RunStateRegistered, RunStateFailed:
				d.Orphan, d.Reason = true, "UNREFERENCED_"+string(state)+"_RUN"
			case RunStateActive:
				d.Reason = "RUN_ACTIVE"
			case RunStateRegistrationPending:
				d.Reason = "REGISTRATION_NOT_FINISHED"
			case RunStateNotRegistered:
				// The run published plain Parquet without Iceberg; the files
				// are the dataset, not garbage.
				d.Reason = "RUN_PUBLISHED_WITHOUT_ICEBERG"
			case RunStateCanceled:
				d.Reason = "OWNED_BY_CANCELED_OBJECT_CLEANUP"
			default:
				d.Reason = "RUN_UNKNOWN"
			}
		default:
			// A data file outside any O_Rabbit run directory, such as one a
			// catalog client wrote under data/.
			d.Orphan, d.Reason = true, "UNREFERENCED_TABLE_FILE"
		}
		decisions = append(decisions, d)
	}
	return decisions, referenced, skipped
}

// runIDFromKey returns <id> for keys under _runs/run-<id>/.
func runIDFromKey(rel string) string {
	rest, ok := strings.CutPrefix(rel, "_runs/run-")
	if !ok {
		return ""
	}
	id, _, found := strings.Cut(rest, "/")
	if !found {
		return ""
	}
	return id
}

// CompactionFile is one live data file that compaction may rewrite.
type CompactionFile struct {
	Path      string
	Key       string
	Size      int64
	Records   int64
	Partition string // canonical partition tuple
}

// CompactionFiles returns the live data files of the current snapshot written
// with the current partition spec. Tables with delete files are refused:
// workers rewrite raw files and would bring deleted rows back.
func CompactionFiles(ctx context.Context, tbl *icetable.Table, keyOf KeyOf) ([]CompactionFile, error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}
	spec := tbl.Spec()
	specID := int32(spec.ID())
	var out []CompactionFile
	for _, mf := range manifests {
		entries, err := mf.FetchEntries(fs, true)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			df := e.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				return nil, failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf(
					"table has delete files (for example %s); compacting would bring deleted rows back, so run a full refresh first", df.FilePath()))
			}
			if df.SpecID() != specID {
				continue
			}
			key, ok := keyOf(df.FilePath())
			if !ok {
				continue
			}
			out = append(out, CompactionFile{Path: df.FilePath(), Key: key, Size: df.FileSizeBytes(), Records: df.Count(), Partition: canonicalPartition(df.Partition())})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func canonicalPartition(p map[int]any) string {
	ids := make([]int, 0, len(p))
	for id := range p {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d=%v", id, p[id])
	}
	return strings.Join(parts, ",")
}

// CompactionOptions bounds one compaction.
type CompactionOptions struct {
	TargetFileBytes int64 // output size; inputs are packed up to it
	SmallFileBytes  int64 // only files below this are rewritten
	MinInputFiles   int   // a group needs at least this many files
	MaxInputFiles   int   // the most files one operation rewrites
}

// CompactionGroup is the inputs one task rewrites into new files. All inputs
// share a partition tuple and a schema.
type CompactionGroup struct {
	Partition   string
	Fingerprint string
	Inputs      []CompactionFile
	Bytes       int64
	Records     int64
}

// CompactionSkip is a file compaction leaves alone, and why.
type CompactionSkip struct {
	File   CompactionFile
	Reason string
}

// PlanCompaction groups small files by partition and schema fingerprint and
// packs each group into tasks of about TargetFileBytes. Files without a
// fingerprint were not written by O_Rabbit and are skipped.
func PlanCompaction(files []CompactionFile, fingerprints map[string]string, opts CompactionOptions) ([]CompactionGroup, []CompactionSkip) {
	type groupKey struct{ partition, fingerprint string }
	var (
		skips  []CompactionSkip
		order  []groupKey
		byKey  = map[groupKey][]CompactionFile{}
		groups []CompactionGroup
	)
	for _, f := range files {
		fp, known := fingerprints[f.Key]
		switch {
		case f.Size >= opts.SmallFileBytes:
			skips = append(skips, CompactionSkip{f, "NOT_SMALL"})
		case !known:
			skips = append(skips, CompactionSkip{f, "NOT_WRITTEN_BY_ORABBIT"})
		default:
			k := groupKey{f.Partition, fp}
			if _, seen := byKey[k]; !seen {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], f)
		}
	}
	total := 0
	for _, k := range order {
		var cur CompactionGroup
		flush := func() {
			if len(cur.Inputs) >= max(opts.MinInputFiles, 2) {
				groups = append(groups, cur)
				total += len(cur.Inputs)
			} else {
				for _, f := range cur.Inputs {
					skips = append(skips, CompactionSkip{f, "TOO_FEW_FILES_IN_PARTITION"})
				}
			}
			cur = CompactionGroup{Partition: k.partition, Fingerprint: k.fingerprint}
		}
		cur = CompactionGroup{Partition: k.partition, Fingerprint: k.fingerprint}
		for _, f := range byKey[k] {
			if opts.MaxInputFiles > 0 && total+len(cur.Inputs) >= opts.MaxInputFiles {
				skips = append(skips, CompactionSkip{f, "MAX_INPUT_FILES_REACHED"})
				continue
			}
			if len(cur.Inputs) > 0 && cur.Bytes+f.Size > opts.TargetFileBytes {
				flush()
			}
			cur.Inputs = append(cur.Inputs, f)
			cur.Bytes += f.Size
			cur.Records += f.Records
		}
		flush()
	}
	return groups, skips
}

// CommitCompaction atomically replaces the input data files with the
// verified output files in one snapshot. It is idempotent: a snapshot
// already carrying runID's compaction stamp means an earlier attempt
// committed. A concurrent commit is retried by reloading the table; if any
// input is no longer live, nothing is committed and a non-retryable
// CATALOG_CONFLICT is returned, leaving the outputs for orphan cleanup.
func CommitCompaction(ctx context.Context, load func(context.Context) (*icetable.Table, error), reg RunConfig, runID string, inputs, outputs []string, outputRows int64, props map[string]string) (alreadyCommitted bool, err error) {
	reg = reg.Normalize()
	snapshotProps := iceberg.Properties{operationProperty: CompactionOperation, "orabbit.run_id": runID}
	for k, v := range props {
		snapshotProps[k] = v
	}
	const attempts = 3
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		tbl, err := load(ctx)
		if err != nil {
			return false, err
		}
		if compactionCommitted(tbl, runID) {
			return true, nil
		}
		live, err := currentDataFiles(ctx, tbl)
		if err != nil {
			return false, err
		}
		byPath := make(map[string]iceberg.DataFile, len(live))
		for _, df := range live {
			byPath[df.FilePath()] = df
		}
		existing := make([]iceberg.DataFile, 0, len(inputs))
		var inputRows int64
		var missing []string
		for _, path := range inputs {
			df, ok := byPath[path]
			if !ok {
				missing = append(missing, path)
				continue
			}
			existing = append(existing, df)
			inputRows += df.Count()
		}
		if len(missing) > 0 {
			return false, failure.NewFailure(failure.FailureCatalogConflict, false, true, fmt.Errorf(
				"%d of %d compaction inputs are no longer in the table (for example %s); another commit replaced them, so the compaction was not applied",
				len(missing), len(inputs), missing[0]))
		}
		if inputRows != outputRows {
			return false, failure.NewFailure(failure.FailureDataIntegrity, false, true, fmt.Errorf(
				"compaction outputs hold %d rows but the inputs hold %d", outputRows, inputRows))
		}
		tx := tbl.NewTransaction()
		if spec := tbl.Spec(); !spec.IsUnpartitioned() {
			err = registerPartitionedFiles(ctx, tx, reg, reg.Table, existing, true, outputs, snapshotProps)
		} else {
			err = applyRESTGoFileMutation(ctx, tx, true, inputs, outputs, snapshotProps)
		}
		if err != nil {
			return false, err
		}
		if _, err := tx.Commit(ctx); err != nil {
			// A conflicting commit (or an unknown outcome) is resolved by
			// reloading: the stamp shows whether this commit landed, and the
			// input check whether it can still apply.
			lastErr = err
			continue
		}
		return false, nil
	}
	return false, fmt.Errorf("compaction commit did not succeed after %d attempts: %w", attempts, lastErr)
}

func compactionCommitted(tbl *icetable.Table, runID string) bool {
	for _, snap := range tbl.Metadata().Snapshots() {
		if snap.Summary == nil {
			continue
		}
		if snap.Summary.Properties[operationProperty] == CompactionOperation && snap.Summary.Properties["orabbit.run_id"] == runID {
			return true
		}
	}
	return false
}

// SnapshotIDString renders a table's current snapshot ID, or "" when it has
// none.
func SnapshotIDString(tbl *icetable.Table) string {
	if snap := tbl.CurrentSnapshot(); snap != nil {
		return strconv.FormatInt(snap.SnapshotID, 10)
	}
	return ""
}
