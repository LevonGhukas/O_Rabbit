package icebergreg

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const ReceiptVersion = 1

type CatalogReceipt struct {
	Version            int    `json:"version"`
	Backend            string `json:"backend"`
	Namespace          string `json:"namespace"`
	Table              string `json:"table"`
	RegistrationID     string `json:"registration_id"`
	CommitID           string `json:"commit_id"`
	ArtifactSetDigest  string `json:"artifact_set_digest"`
	DefiniteAt         string `json:"definite_success_at"`
	MetadataLocation   string `json:"metadata_location,omitempty"`
	SnapshotID         string `json:"snapshot_id,omitempty"`
	SequenceNumber     *int64 `json:"sequence_number,omitempty"`
	MetadataVersion    string `json:"metadata_version,omitempty"`
	ExternalIdentity   string `json:"external_identity,omitempty"`
	IdentityAvailable  bool   `json:"external_identity_available"`
	NoOp               bool   `json:"no_op,omitempty"`
	NoOpReason         string `json:"no_op_reason,omitempty"`
	NoOpEvidenceDigest string `json:"no_op_evidence_digest,omitempty"`
}

func (r CatalogReceipt) MarshalDeterministic() ([]byte, error) {
	if r.Version == 0 {
		r.Version = ReceiptVersion
	}
	if r.Version != ReceiptVersion || strings.TrimSpace(r.Backend) == "" ||
		strings.TrimSpace(r.Table) == "" || strings.TrimSpace(r.RegistrationID) == "" ||
		len(r.CommitID) != 64 || len(r.ArtifactSetDigest) != 64 || strings.TrimSpace(r.DefiniteAt) == "" {
		return nil, fmt.Errorf("incomplete catalog receipt")
	}
	if r.NoOp && (strings.TrimSpace(r.NoOpReason) == "" || len(r.NoOpEvidenceDigest) != 64) {
		return nil, fmt.Errorf("incomplete no-op catalog receipt")
	}
	if r.NoOp {
		if _, err := hex.DecodeString(r.NoOpEvidenceDigest); err != nil {
			return nil, fmt.Errorf("invalid no-op catalog receipt evidence")
		}
	}
	return json.Marshal(r)
}

func ParseCatalogReceipt(raw string) (CatalogReceipt, error) {
	var r CatalogReceipt
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return r, err
	}
	_, err := r.MarshalDeterministic()
	return r, err
}
