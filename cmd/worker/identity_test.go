package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/workeridentity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func issueTestIdentity(t *testing.T, ca *workeridentity.CA, id string) (string, string) {
	t.Helper()
	keyPEM, csrPEM, err := workeridentity.NewKeyAndCSR()
	if err != nil {
		t.Fatal(err)
	}
	issued, err := ca.Issue(csrPEM, id, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return keyPEM, issued.CertificatePEM
}

func TestWorkerIdentityPersistsAndRenewsAtomically(t *testing.T) {
	caPEM, caKey, _ := workeridentity.GenerateCA(time.Now())
	ca, _ := workeridentity.ParseCA(caPEM, caKey)
	id, _ := workeridentity.NewWorkerID()
	dir := t.TempDir()

	if w, err := loadWorkerIdentity(dir); err != nil || w != nil {
		t.Fatalf("unenrolled worker must load no identity: %v %v", w, err)
	}
	keyPEM, certPEM := issueTestIdentity(t, ca, id)
	w := &workerIdentity{path: filepath.Join(dir, workerIdentityFile)}
	if err := w.install(keyPEM, certPEM); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(w.path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("identity file must be private, mode=%v err=%v", info.Mode().Perm(), err)
	}
	loaded, err := loadWorkerIdentity(dir)
	if err != nil || loaded.workerID() != id {
		t.Fatalf("reloaded identity=%v err=%v", loaded, err)
	}

	cp := fakeControlPlaneClient{renewCertificate: func(_ context.Context, in *grpcpb.RenewWorkerCertificateRequest, _ ...grpc.CallOption) (*grpcpb.RenewWorkerCertificateResponse, error) {
		issued, err := ca.Issue(in.CsrPem, id, time.Hour, time.Now())
		if err != nil {
			return nil, err
		}
		return &grpcpb.RenewWorkerCertificateResponse{CertificatePem: issued.CertificatePEM}, nil
	}}
	before := loaded.leaf().SerialNumber
	if err := loaded.renew(context.Background(), cp); err != nil {
		t.Fatal(err)
	}
	if loaded.leaf().SerialNumber.Cmp(before) == 0 || loaded.workerID() != id {
		t.Fatal("renewal must install a new certificate for the same identity")
	}

	otherID, _ := workeridentity.NewWorkerID()
	otherKey, otherCert := issueTestIdentity(t, ca, otherID)
	if err := loaded.install(otherKey, otherCert); err == nil {
		t.Fatal("a certificate for another worker must be refused")
	}
}

func TestTaskS3CredentialsRefreshFromLeaseholderRPC(t *testing.T) {
	calls := 0
	cp := fakeControlPlaneClient{getTaskCredentials: func(_ context.Context, in *grpcpb.GetTaskCredentialsRequest, _ ...grpc.CallOption) (*grpcpb.GetTaskCredentialsResponse, error) {
		calls++
		if in.AttemptId != "attempt" || in.FencingToken != "fence" {
			t.Fatalf("request must carry the attempt credentials: %+v", in)
		}
		return &grpcpb.GetTaskCredentialsResponse{SourceDsn: "dsn", S3AccessKeyId: "k", S3SecretAccessKey: "s", S3SessionToken: "tok", S3ExpiresAtUnixMs: time.Now().Add(time.Hour).UnixMilli()}, nil
	}}
	task := &grpcpb.TaskAssignment{TaskId: "task", AttemptId: "attempt", FencingToken: "fence"}
	creds, err := fetchTaskCredentials(context.Background(), cp, "worker", task)
	if err != nil || creds.SourceDSN != "dsn" {
		t.Fatalf("creds=%+v err=%v", creds, err)
	}
	first, err := creds.S3.Retrieve(context.Background())
	if err != nil || calls != 1 || !first.CanExpire || first.SessionToken != "tok" {
		t.Fatalf("first retrieval must reuse fetched credentials: calls=%d creds=%+v err=%v", calls, first, err)
	}
	if _, err := creds.S3.Retrieve(context.Background()); err != nil || calls != 2 {
		t.Fatalf("refresh must fetch from the master: calls=%d err=%v", calls, err)
	}

	fenced := fakeControlPlaneClient{getTaskCredentials: func(context.Context, *grpcpb.GetTaskCredentialsRequest, ...grpc.CallOption) (*grpcpb.GetTaskCredentialsResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "task ownership lost")
	}}
	if _, err := fetchTaskCredentials(context.Background(), fenced, "worker", task); err == nil {
		t.Fatal("fenced credential request must fail")
	} else if _, ok := err.(*taskOwnershipLostError); !ok {
		t.Fatalf("fenced request must be reported as ownership loss, got %T", err)
	}
}
