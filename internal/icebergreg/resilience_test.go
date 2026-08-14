package icebergreg

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
)

func TestCatalogReceiptDeterministicAndSecretFree(t *testing.T) {
	seq := int64(7)
	r := CatalogReceipt{Version: 1, Backend: "rest-go", Namespace: "ns", Table: "ns.tbl", RegistrationID: "reg-1", CommitID: strings.Repeat("a", 64), ArtifactSetDigest: strings.Repeat("b", 64), DefiniteAt: "2026-07-23T00:00:00Z", MetadataLocation: "s3://bucket/metadata/v2.json", SnapshotID: "42", SequenceNumber: &seq, MetadataVersion: "2", ExternalIdentity: "operation-1", IdentityAvailable: true}
	a, err := r.MarshalDeterministic()
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.MarshalDeterministic()
	if err != nil || string(a) != string(b) {
		t.Fatalf("nondeterministic receipt: %q %q err=%v", a, b, err)
	}
	if strings.Contains(strings.ToLower(string(a)), "token") || strings.Contains(strings.ToLower(string(a)), "secret") {
		t.Fatalf("receipt leaked secret field: %s", a)
	}
	got, err := ParseCatalogReceipt(string(a))
	if err != nil || got.SnapshotID != "42" || !got.IdentityAvailable {
		t.Fatalf("parse=%+v err=%v", got, err)
	}
}

func TestCatalogReceiptAllowsExplicitPartialIdentity(t *testing.T) {
	r := CatalogReceipt{Backend: "ice", Namespace: "ns", Table: "ns.tbl", RegistrationID: "reg-1", CommitID: strings.Repeat("a", 64), ArtifactSetDigest: strings.Repeat("b", 64), DefiniteAt: "2026-07-23T00:00:00Z", IdentityAvailable: false}
	body, err := r.MarshalDeterministic()
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	_ = json.Unmarshal(body, &decoded)
	if decoded["external_identity_available"] != false {
		t.Fatalf("partial identity not explicit: %s", body)
	}
	if _, ok := decoded["snapshot_id"]; ok {
		t.Fatalf("invented snapshot identity: %s", body)
	}
}

func TestCompleteNoOpRegistrationPersistsExplicitEvidenceBeforeVerification(t *testing.T) {
	base := CatalogReceipt{
		Version:           1,
		Backend:           "rest-go",
		Namespace:         "ns",
		Table:             "ns.tbl",
		RegistrationID:    "reg-1",
		CommitID:          strings.Repeat("a", 64),
		ArtifactSetDigest: strings.Repeat("b", 64),
		DefiniteAt:        "2026-07-23T00:00:00Z",
		IdentityAvailable: false,
	}
	body, err := base.MarshalDeterministic()
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	req := RunRequest{
		CatalogReceiptFactory: func() (string, error) {
			order = append(order, "receipt")
			return string(body), nil
		},
		CatalogNoOp: func(raw string) error {
			order = append(order, "noop")
			got, err := ParseCatalogReceipt(raw)
			if err != nil {
				return err
			}
			if !got.NoOp || got.NoOpReason != "ALL_ARTIFACTS_ALREADY_APPLIED" {
				t.Fatalf("receipt does not describe verified no-op: %+v", got)
			}
			if len(got.NoOpEvidenceDigest) != 64 || got.IdentityAvailable {
				t.Fatalf("receipt evidence/identity = %+v", got)
			}
			return nil
		},
		IceStateWriting: func() error {
			order = append(order, "verify")
			return nil
		},
		BeforeExternalCommit: func() error {
			t.Fatal("no-op registration crossed the external catalog boundary")
			return nil
		},
	}
	result, err := completeNoOpRegistration(req, "ALL_ARTIFACTS_ALREADY_APPLIED", []byte(`{"catalog_receipt":{"commit_id":"stable"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Objects != 0 {
		t.Fatalf("objects = %d, want 0", result.Objects)
	}
	if got := strings.Join(order, ","); got != "receipt,noop,verify" {
		t.Fatalf("callback order = %q", got)
	}
}

func TestCompleteNoOpRegistrationResumeUsesDurableReceiptWithoutReplay(t *testing.T) {
	noOpCalls := 0
	verifyCalls := 0
	result, err := completeNoOpRegistration(RunRequest{
		CatalogAlreadyCommitted: true,
		CatalogNoOp: func(string) error {
			noOpCalls++
			return nil
		},
		IceStateWriting: func() error {
			verifyCalls++
			return nil
		},
	}, "ALL_ARTIFACTS_ALREADY_APPLIED", []byte("durable evidence"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Objects != 0 || noOpCalls != 0 || verifyCalls != 1 {
		t.Fatalf("result=%+v no-op calls=%d verify calls=%d", result, noOpCalls, verifyCalls)
	}
}

func TestUnverifiedNoOpCrossesDurableBoundaryAndRequiresReconciliation(t *testing.T) {
	boundaryCalls := 0
	_, err := requireNoOpReconciliation(RunRequest{
		BeforeExternalCommit: func() error {
			boundaryCalls++
			return nil
		},
	})
	if boundaryCalls != 1 {
		t.Fatalf("boundary calls = %d, want 1", boundaryCalls)
	}
	failInfo := ClassifyFailure(err, true)
	if failInfo.Class != failure.FailureExternalAmbiguous || failInfo.Retryable || failInfo.DefiniteRejection {
		t.Fatalf("failure=%+v", failInfo)
	}
}

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

var _ net.Error = timeoutNetError{}

func TestFailureClassificationForBothBackends(t *testing.T) {
	exitErr := &exec.ExitError{Stderr: []byte("permission denied")}
	cases := []struct {
		name     string
		err      error
		external bool
		class    failure.FailureClass
		retry    bool
	}{
		{"configuration", failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, errors.New("missing")), false, failure.FailureConfigurationUnavailable, false},
		{"pre-timeout", context.DeadlineExceeded, false, failure.FailureCatalogTimeout, true},
		{"post-timeout", context.DeadlineExceeded, true, failure.FailureExternalAmbiguous, false},
		{"pre-network", timeoutNetError{}, false, failure.FailureCatalogUnavailable, true},
		{"post-network", timeoutNetError{}, true, failure.FailureExternalAmbiguous, false},
		{"ice-authorization", exitErr, false, failure.FailureAuthorization, false},
		{"unknown-rest", errors.New("unknown"), false, failure.FailureUnknownPermanent, false},
		{"unknown-post", errors.New("unknown"), true, failure.FailureUnknownAmbiguous, false},
		{"cancel-pre", context.Canceled, false, failure.FailureCanceled, false},
		{"cancel-post", context.Canceled, true, failure.FailureExternalAmbiguous, false},
	}
	for _, backend := range []string{"rest-go", "ice"} {
		for _, tc := range cases {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				got := ClassifyFailure(tc.err, tc.external)
				if got.Class != tc.class || got.Retryable != tc.retry {
					t.Fatalf("got=%+v", got)
				}
			})
		}
	}
}

func TestReceiptTimestampIsStableInput(t *testing.T) {
	now := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	r := CatalogReceipt{Backend: "rest-go", Namespace: "ns", Table: "ns.tbl", RegistrationID: "r", CommitID: strings.Repeat("a", 64), ArtifactSetDigest: strings.Repeat("b", 64), DefiniteAt: now}
	if _, err := r.MarshalDeterministic(); err != nil {
		t.Fatal(err)
	}
}
