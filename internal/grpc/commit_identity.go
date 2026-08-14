package grpcapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

const (
	commitFailureTransient  = "TRANSIENT_STORAGE_CATALOG_FAILURE"
	commitFailureValidation = "TERMINAL_VALIDATION_FAILURE"
	commitFailureIntegrity  = "DURABLE_INTEGRITY_CONFLICT"
	commitFailureOperator   = "OPERATOR_ACTION_REQUIRED"
)

type classifiedCommitError struct {
	class     string
	retryable bool
	operator  bool
	component string
	err       error
}

func (e *classifiedCommitError) Error() string { return e.err.Error() }
func (e *classifiedCommitError) Unwrap() error { return e.err }

func commitIntegrityError(component, format string, args ...any) error {
	return &classifiedCommitError{
		class:     commitFailureIntegrity,
		component: component,
		err:       fmt.Errorf("commit integrity conflict: %s: %s", component, fmt.Sprintf(format, args...)),
	}
}

func commitOperatorError(component, format string, args ...any) error {
	return &classifiedCommitError{
		class:     commitFailureOperator,
		operator:  true,
		component: component,
		err:       fmt.Errorf("commit operator action required: %s: %s", component, fmt.Sprintf(format, args...)),
	}
}

func classifyCommitError(err error) (class string, retryable, operator bool, component string) {
	var classified *classifiedCommitError
	if errors.As(err, &classified) {
		return classified.class, classified.retryable, classified.operator, classified.component
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "fencing conflict"):
		return commitFailureOperator, false, true, "checkpoint_fencing"
	case strings.Contains(message, "integrity conflict"), strings.Contains(message, "artifact size mismatch"), strings.Contains(message, "artifact sha256 mismatch"):
		return commitFailureIntegrity, false, false, "durable_integrity"
	case strings.Contains(message, "malformed durable intent"), strings.Contains(message, "missing proposed state"):
		return commitFailureValidation, false, false, "durable_validation"
	}
	return commitFailureTransient, true, false, "storage_or_catalog"
}

func normalizeEndpoint(raw string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(raw), "/"))
}

func normalizePrefix(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

type durableCommitManifest struct {
	SchemaVersion int               `json:"schema_version"`
	RunID         string            `json:"run_id"`
	Bucket        string            `json:"bucket"`
	Prefix        string            `json:"prefix"`
	Objects       []string          `json:"objects"`
	Artifacts     []artifact.Record `json:"artifacts"`
}

type durableCommitState struct {
	Bucket      string `json:"bucket"`
	Prefix      string `json:"prefix"`
	RunID       string `json:"last_committed_run_id"`
	CommitID    string `json:"commit_id"`
	ManifestKey string `json:"manifest_key"`
}

func validatePersistedCommitIntent(run db.Run, raw json.RawMessage, current durableCommitDestination, committedKeys []string, accepted []artifact.Record) (durableCommitIntent, durableCommitDestination, error) {
	var intent durableCommitIntent
	if err := json.Unmarshal(raw, &intent); err != nil {
		return intent, durableCommitDestination{}, commitIntegrityError("intent_json", "malformed durable intent")
	}
	if strings.TrimSpace(intent.CommitID) == "" || strings.TrimSpace(intent.ManifestKey) == "" || strings.TrimSpace(intent.StateKey) == "" || len(intent.Manifest) == 0 || len(intent.ProposedState) == 0 {
		return intent, durableCommitDestination{}, commitIntegrityError("intent_fields", "required persisted identity fields are missing")
	}
	digest := sha256.Sum256(intent.Manifest)
	actualCommitID := hex.EncodeToString(digest[:])
	if intent.CommitID != actualCommitID {
		return intent, durableCommitDestination{}, commitIntegrityError("manifest_hash", "persisted commit_id does not authenticate exact manifest bytes")
	}
	if run.CommitID != intent.CommitID {
		return intent, durableCommitDestination{}, commitIntegrityError("commit_id", "run commit_id does not match persisted intent")
	}
	if intent.DatasetID != "" && intent.DatasetID != run.DatasetKey {
		return intent, durableCommitDestination{}, commitIntegrityError("dataset_id", "persisted dataset identity does not match run")
	}

	var manifest durableCommitManifest
	if err := json.Unmarshal(intent.Manifest, &manifest); err != nil {
		return intent, durableCommitDestination{}, commitIntegrityError("manifest_json", "malformed persisted manifest")
	}
	if manifest.SchemaVersion != 1 && manifest.SchemaVersion != 2 {
		return intent, durableCommitDestination{}, commitIntegrityError("schema_version", "unsupported manifest schema version %d", manifest.SchemaVersion)
	}
	if manifest.RunID != run.ID {
		return intent, durableCommitDestination{}, commitIntegrityError("run_id", "manifest run_id %q does not match run %q", manifest.RunID, run.ID)
	}

	destination := intent.Destination
	if strings.TrimSpace(destination.Endpoint) == "" {
		// Legacy intents predate the durable endpoint snapshot. Their exact
		// bucket/prefix remain authenticated by the manifest and state, while
		// the current endpoint supplies the only available transport identity.
		destination.Endpoint = current.Endpoint
		destination.Region = current.Region
		destination.ForcePathStyle = current.ForcePathStyle
		destination.Bucket = manifest.Bucket
		destination.Prefix = manifest.Prefix
	}
	destination.Endpoint = normalizeEndpoint(destination.Endpoint)
	destination.Bucket = strings.TrimSpace(destination.Bucket)
	destination.Prefix = normalizePrefix(destination.Prefix)
	if destination.Endpoint == "" || destination.Bucket == "" || destination.Prefix == "" {
		return intent, durableCommitDestination{}, commitIntegrityError("destination", "durable endpoint, bucket, and prefix are required")
	}
	if normalizeEndpoint(current.Endpoint) != destination.Endpoint {
		return intent, durableCommitDestination{}, commitOperatorError("endpoint", "current credentials belong to endpoint %q, durable intent requires %q", normalizeEndpoint(current.Endpoint), destination.Endpoint)
	}
	if strings.TrimSpace(current.Bucket) != destination.Bucket {
		return intent, durableCommitDestination{}, commitOperatorError("bucket", "current credentials belong to bucket %q, durable intent requires %q", strings.TrimSpace(current.Bucket), destination.Bucket)
	}
	if strings.TrimSpace(manifest.Bucket) != destination.Bucket {
		return intent, durableCommitDestination{}, commitIntegrityError("manifest_bucket", "manifest bucket differs from durable destination")
	}
	if normalizePrefix(manifest.Prefix) != destination.Prefix {
		return intent, durableCommitDestination{}, commitIntegrityError("manifest_prefix", "manifest prefix differs from durable destination")
	}
	if !strings.HasPrefix(intent.ManifestKey, destination.Prefix+"/") {
		return intent, durableCommitDestination{}, commitIntegrityError("manifest_key", "persisted manifest key is outside durable prefix")
	}
	if intent.StateKey != destination.Prefix+"/_state.json" {
		return intent, durableCommitDestination{}, commitIntegrityError("state_key", "persisted state key is outside durable prefix")
	}

	var state durableCommitState
	if err := json.Unmarshal(intent.ProposedState, &state); err != nil {
		return intent, durableCommitDestination{}, commitIntegrityError("state_json", "malformed proposed state")
	}
	if state.RunID != run.ID || state.CommitID != intent.CommitID || state.ManifestKey != intent.ManifestKey {
		return intent, durableCommitDestination{}, commitIntegrityError("state_identity", "proposed state does not bind run, commit, and manifest")
	}
	if strings.TrimSpace(state.Bucket) != destination.Bucket || normalizePrefix(state.Prefix) != destination.Prefix {
		return intent, durableCommitDestination{}, commitIntegrityError("state_destination", "proposed state destination differs from durable intent")
	}

	wantKeys := append([]string(nil), committedKeys...)
	sort.Strings(wantKeys)
	storedKeys := append([]string(nil), manifest.Objects...)
	if manifest.SchemaVersion == 2 {
		storedKeys = storedKeys[:0]
		for _, record := range manifest.Artifacts {
			if err := record.Validate(); err != nil || record.RunID != run.ID {
				return intent, durableCommitDestination{}, commitIntegrityError("artifacts", "persisted verified artifact is invalid")
			}
			storedKeys = append(storedKeys, record.ObjectKey)
		}
		if len(manifest.Artifacts) != len(accepted) {
			return intent, durableCommitDestination{}, commitIntegrityError("artifact_set", "persisted artifact count differs from durable run records")
		}
		for i := range manifest.Artifacts {
			persistedBytes, _ := json.Marshal(manifest.Artifacts[i])
			acceptedBytes, _ := json.Marshal(accepted[i])
			if !bytes.Equal(persistedBytes, acceptedBytes) {
				return intent, durableCommitDestination{}, commitIntegrityError("artifact_set", "persisted artifact records differ from durable run records")
			}
		}
	}
	sort.Strings(storedKeys)
	if strings.Join(storedKeys, "\x00") != strings.Join(wantKeys, "\x00") {
		return intent, durableCommitDestination{}, commitIntegrityError("task_outputs", "persisted manifest objects differ from durable task outputs")
	}
	intent.Destination = destination
	return intent, destination, nil
}
