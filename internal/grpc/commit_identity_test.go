package grpcapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func persistedEmptyIntentFixture(t *testing.T) (db.Run, durableCommitIntent, json.RawMessage) {
	t.Helper()
	run := db.Run{ID: "run-empty", DatasetKey: "durable-dataset"}
	manifest := json.RawMessage(`{"schema_version":2,"bucket":"bucket-a","prefix":"prefix-a","run_id":"run-empty","objects":[],"artifacts":[]}`)
	digest := sha256.Sum256(manifest)
	commitID := hex.EncodeToString(digest[:])
	state := json.RawMessage(`{"bucket":"bucket-a","prefix":"prefix-a","last_committed_run_id":"run-empty","commit_id":"` + commitID + `","manifest_key":"prefix-a/_commits/run-run-empty.json"}`)
	intent := durableCommitIntent{
		CommitID:      commitID,
		DatasetID:     run.DatasetKey,
		ManifestKey:   "prefix-a/_commits/run-run-empty.json",
		StateKey:      "prefix-a/_state.json",
		Destination:   durableCommitDestination{Endpoint: "http://minio:9000", Region: "us-east-1", Bucket: "bucket-a", Prefix: "prefix-a", ForcePathStyle: true},
		Manifest:      manifest,
		ProposedState: state,
	}
	raw, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	run.CommitID = commitID
	run.CommitIntentJSON = raw
	return run, intent, raw
}

func TestPersistedCommitIntentUsesDurableKeysAcrossPrefixDrift(t *testing.T) {
	run, _, raw := persistedEmptyIntentFixture(t)
	current := durableCommitDestination{Endpoint: "http://minio:9000/", Region: "us-east-1", Bucket: "bucket-a", Prefix: "prefix-b", ForcePathStyle: true}
	got, destination, err := validatePersistedCommitIntent(run, raw, current, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if destination.Prefix != "prefix-a" || got.ManifestKey != "prefix-a/_commits/run-run-empty.json" || got.StateKey != "prefix-a/_state.json" {
		t.Fatalf("durable identity changed: destination=%+v intent=%+v", destination, got)
	}
}

func TestPersistedCommitIntentClassifiesIdentityComponents(t *testing.T) {
	run, intent, raw := persistedEmptyIntentFixture(t)
	tests := []struct {
		name      string
		raw       json.RawMessage
		current   durableCommitDestination
		component string
		operator  bool
	}{
		{name: "endpoint", raw: raw, current: durableCommitDestination{Endpoint: "http://other:9000", Bucket: "bucket-a"}, component: "endpoint", operator: true},
		{name: "bucket", raw: raw, current: durableCommitDestination{Endpoint: "http://minio:9000", Bucket: "bucket-b"}, component: "bucket", operator: true},
	}
	corrupt := intent
	corrupt.Manifest = json.RawMessage(strings.Replace(string(intent.Manifest), `"artifacts":[]`, `"artifacts":null`, 1))
	corruptRaw, _ := json.Marshal(corrupt)
	tests = append(tests, struct {
		name      string
		raw       json.RawMessage
		current   durableCommitDestination
		component string
		operator  bool
	}{name: "manifest hash", raw: corruptRaw, current: durableCommitDestination{Endpoint: "http://minio:9000", Bucket: "bucket-a"}, component: "manifest_hash"})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := validatePersistedCommitIntent(run, tc.raw, tc.current, nil, nil)
			if err == nil {
				t.Fatal("expected identity failure")
			}
			class, retryable, operator, component := classifyCommitError(err)
			if retryable || operator != tc.operator || component != tc.component {
				t.Fatalf("classification=(%s,%v,%v,%s) err=%v", class, retryable, operator, component, err)
			}
			if !strings.Contains(err.Error(), tc.component) {
				t.Fatalf("diagnostic does not identify component: %v", err)
			}
		})
	}
}

func TestPersistedCommitIntentRejectsRunAndDatasetMismatch(t *testing.T) {
	run, intent, _ := persistedEmptyIntentFixture(t)
	for _, mutate := range []func(*durableCommitIntent){
		func(i *durableCommitIntent) { i.DatasetID = "other-dataset" },
		func(i *durableCommitIntent) {
			i.Manifest = json.RawMessage(`{"schema_version":2,"bucket":"bucket-a","prefix":"prefix-a","run_id":"other-run","objects":[],"artifacts":[]}`)
			digest := sha256.Sum256(i.Manifest)
			i.CommitID = hex.EncodeToString(digest[:])
			run.CommitID = i.CommitID
		},
	} {
		candidate := intent
		mutate(&candidate)
		raw, _ := json.Marshal(candidate)
		_, _, err := validatePersistedCommitIntent(run, raw, durableCommitDestination{Endpoint: "http://minio:9000", Bucket: "bucket-a"}, nil, nil)
		if err == nil {
			t.Fatal("expected durable identity mismatch")
		}
	}
}
