package icebergreg

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
)

const (
	OutcomeExactlyCommitted       = "EXACTLY_COMMITTED"
	OutcomeDefinitelyNotCommitted = "DEFINITELY_NOT_COMMITTED"
	OutcomePartiallyCommitted     = "PARTIALLY_COMMITTED"
	OutcomeConflictingCommit      = "CONFLICTING_COMMIT"
	OutcomeTableNotFound          = "TABLE_NOT_FOUND"
	OutcomeCatalogUnavailable     = "CATALOG_UNAVAILABLE"
	OutcomeInsufficientEvidence   = "INSUFFICIENT_EVIDENCE"
)

type OperationIdentity struct {
	RegistrationID    string `json:"registration_id"`
	RunID             string `json:"run_id"`
	CommitID          string `json:"commit_id"`
	ArtifactSetDigest string `json:"artifact_set_digest"`
	ManifestKey       string `json:"manifest_key"`
}

func (o OperationIdentity) Properties() map[string]string {
	return map[string]string{"orabbit.registration_id": o.RegistrationID, "orabbit.run_id": o.RunID, "orabbit.commit_id": o.CommitID, "orabbit.artifact_set_digest": o.ArtifactSetDigest, "orabbit.manifest_key": o.ManifestKey}
}

type ExpectedFile struct {
	Path              string `json:"path"`
	Size              int64  `json:"size"`
	Records           int64  `json:"records"`
	SHA256            string `json:"sha256"`
	SchemaFingerprint string `json:"schema_fingerprint"`
}
type ObservedFile struct {
	Path       string `json:"path"`
	Size       *int64 `json:"size,omitempty"`
	Records    *int64 `json:"records,omitempty"`
	SnapshotID string `json:"snapshot_id,omitempty"`
	Status     string `json:"status,omitempty"`
}
type SnapshotEvidence struct {
	ID      string            `json:"id"`
	Summary map[string]string `json:"summary"`
	Files   []ObservedFile    `json:"files"`
}
type CatalogObservation struct {
	Backend            string             `json:"backend"`
	TableExists        bool               `json:"table_exists"`
	TableIdentifier    string             `json:"table_identifier"`
	MetadataStart      string             `json:"metadata_start"`
	MetadataEnd        string             `json:"metadata_end"`
	CurrentSnapshotID  string             `json:"current_snapshot_id"`
	CompleteHistory    bool               `json:"complete_history"`
	SchemaCompatible   bool               `json:"schema_compatible"`
	LocationCompatible bool               `json:"location_compatible"`
	Snapshots          []SnapshotEvidence `json:"snapshots"`
}
type ReconciliationDecision struct {
	Outcome                string `json:"outcome"`
	EvidenceDigest         string `json:"evidence_digest"`
	MatchedFiles           int    `json:"matched_files"`
	ExpectedFiles          int    `json:"expected_files"`
	SnapshotID             string `json:"snapshot_id,omitempty"`
	MetadataIdentity       string `json:"metadata_identity,omitempty"`
	MarkerMatched          bool   `json:"marker_matched"`
	OperatorActionRequired bool   `json:"operator_action_required"`
	Reason                 string `json:"reason"`
}

func NormalizeObjectURI(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid object uri %q", raw)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	decoded, err := url.PathUnescape(u.EscapedPath())
	if err != nil {
		return "", err
	}
	clean := path.Clean("/" + decoded)
	u.Path = clean
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
func evidenceDigest(obs CatalogObservation, expected []ExpectedFile) (string, error) {
	b, err := json.Marshal(struct {
		O CatalogObservation `json:"observation"`
		E []ExpectedFile     `json:"expected"`
	}{obs, expected})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func DecideReconciliation(op OperationIdentity, expected []ExpectedFile, obs CatalogObservation) (ReconciliationDecision, error) {
	sort.Slice(expected, func(i, j int) bool { return expected[i].Path < expected[j].Path })
	d := ReconciliationDecision{ExpectedFiles: len(expected), MetadataIdentity: obs.MetadataEnd}
	digest, err := evidenceDigest(obs, expected)
	if err != nil {
		return d, err
	}
	d.EvidenceDigest = digest
	if obs.MetadataStart == "" || obs.MetadataEnd == "" || obs.MetadataStart != obs.MetadataEnd {
		return insufficient(d, "metadata changed during observation"), nil
	}
	if !obs.TableExists {
		if obs.CompleteHistory {
			d.Outcome = OutcomeTableNotFound
			d.Reason = "stable observation proves table absent"
			return d, nil
		}
		return insufficient(d, "table absence lacks complete history"), nil
	}
	if !obs.SchemaCompatible || !obs.LocationCompatible {
		d.Outcome = OutcomeConflictingCommit
		d.OperatorActionRequired = true
		d.Reason = "table identity, location, or schema conflicts"
		return d, nil
	}
	expectedMap := map[string]ExpectedFile{}
	for _, f := range expected {
		n, e := NormalizeObjectURI(f.Path)
		if e != nil {
			return d, e
		}
		f.Path = n
		expectedMap[n] = f
	}
	matched := map[string]bool{}
	markerMatch := false
	markerConflict := false
	markerSnapshot := ""
	for _, s := range obs.Snapshots {
		props := s.Summary
		if props["orabbit.registration_id"] == op.RegistrationID {
			if props["orabbit.commit_id"] == op.CommitID && props["orabbit.artifact_set_digest"] == op.ArtifactSetDigest && props["orabbit.manifest_key"] == op.ManifestKey {
				markerMatch = true
				markerSnapshot = s.ID
			} else {
				markerConflict = true
			}
		}
		for _, f := range s.Files {
			n, e := NormalizeObjectURI(f.Path)
			if e != nil {
				continue
			}
			if exp, ok := expectedMap[n]; ok {
				if (f.Size != nil && *f.Size != exp.Size) || (f.Records != nil && *f.Records != exp.Records) {
					markerConflict = true
				} else {
					matched[n] = true
				}
			}
		}
	}
	d.MatchedFiles = len(matched)
	d.MarkerMatched = markerMatch
	d.SnapshotID = markerSnapshot
	if markerConflict {
		d.Outcome = OutcomeConflictingCommit
		d.OperatorActionRequired = true
		d.Reason = "operation marker or file metadata conflicts"
		return d, nil
	}
	if len(matched) > 0 && len(matched) < len(expectedMap) {
		d.Outcome = OutcomePartiallyCommitted
		d.OperatorActionRequired = true
		d.Reason = "only part of expected file set observed"
		return d, nil
	}
	if markerMatch && len(matched) == len(expectedMap) {
		d.Outcome = OutcomeExactlyCommitted
		d.Reason = "exact operation marker and files observed"
		return d, nil
	}
	if len(matched) == len(expectedMap) && len(expectedMap) > 0 {
		return insufficient(d, "exact paths lack a durable operation marker"), nil
	}
	if len(matched) == 0 && obs.CompleteHistory {
		d.Outcome = OutcomeDefinitelyNotCommitted
		d.Reason = "stable complete history proves marker and files absent"
		return d, nil
	}
	return insufficient(d, "absence is not proven by available history"), nil
}
func insufficient(d ReconciliationDecision, reason string) ReconciliationDecision {
	d.Outcome = OutcomeInsufficientEvidence
	d.OperatorActionRequired = true
	d.Reason = reason
	return d
}
