package grpcapi

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/workeridentity"
	"github.com/aws/aws-sdk-go-v2/aws"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpcstatus "google.golang.org/grpc/status"
)

// identityHarness runs the real control-plane gRPC server over TLS with
// master-issued worker identities.
type identityHarness struct {
	t        *testing.T
	st       *db.Store
	srv      *Server
	addr     string
	serverCA string
}

func newIdentityHarness(t *testing.T) *identityHarness {
	t.Helper()
	dir := t.TempDir()
	serverCA, serverCert, serverKey := writeServerTLS(t, dir)
	st := openGRPCTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, nil)
	ca, err := LoadWorkerCA(context.Background(), st, testCryptoKey, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	srv.SetWorkerIdentity(ca, time.Hour)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Serve(ctx, lis, Config{TLSCertFile: serverCert, TLSKeyFile: serverKey}, srv)
	}()
	t.Cleanup(func() { cancel(); <-done })
	return &identityHarness{t: t, st: st, srv: srv, addr: lis.Addr().String(), serverCA: serverCA}
}

func (h *identityHarness) enrollmentToken(pool string, maxUses int) string {
	h.t.Helper()
	token := "orw_test_" + pool + "_" + time.Now().Format("150405.000000000")
	digest := sha256.Sum256([]byte(token))
	if _, err := h.st.CreateEnrollmentTokenAudited(context.Background(), "tok-"+token, hex.EncodeToString(digest[:]), pool, maxUses, time.Now().Add(time.Hour), db.AuditRecord{Action: "test", ResourceType: "test", ResourceID: "test"}); err != nil {
		h.t.Fatal(err)
	}
	return token
}

// client dials the master, presenting cert when it is non-nil.
func (h *identityHarness) client(cert *tls.Certificate) grpcpb.ControlPlaneClient {
	h.t.Helper()
	var source func() (*tls.Certificate, error)
	if cert != nil {
		source = func() (*tls.Certificate, error) { return cert, nil }
	}
	tlsCfg, err := ClientTLSConfig(h.serverCA, "localhost", source)
	if err != nil {
		h.t.Fatal(err)
	}
	conn, err := grpc.NewClient(h.addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = conn.Close() })
	return grpcpb.NewControlPlaneClient(conn)
}

// enroll returns the new worker's ID and a client authenticated as it.
func (h *identityHarness) enroll(token string) (string, *tls.Certificate) {
	h.t.Helper()
	keyPEM, csrPEM, err := workeridentity.NewKeyAndCSR()
	if err != nil {
		h.t.Fatal(err)
	}
	resp, err := h.client(nil).EnrollWorker(context.Background(), &grpcpb.EnrollWorkerRequest{EnrollmentToken: token, CsrPem: csrPEM, Name: "w"})
	if err != nil {
		h.t.Fatalf("enroll: %v", err)
	}
	cert, err := tls.X509KeyPair([]byte(resp.CertificatePem), []byte(keyPEM))
	if err != nil {
		h.t.Fatal(err)
	}
	return resp.WorkerId, &cert
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := grpcstatus.Code(err); got != want {
		t.Fatalf("status=%v want %v (err=%v)", got, want, err)
	}
}

func TestWorkerEnrollmentIssuesCertificateBoundIdentity(t *testing.T) {
	h := newIdentityHarness(t)
	ctx := context.Background()
	token := h.enrollmentToken("default", 1)

	workerID, cert := h.enroll(token)
	if !workeridentity.ValidWorkerID(workerID) {
		t.Fatalf("worker ID %q is not a server-issued UUID", workerID)
	}
	leaf, _ := x509.ParseCertificate(cert.Certificate[0])
	if got, err := workeridentity.IdentityFromCertificate(leaf); err != nil || got != workerID {
		t.Fatalf("certificate identity=%q err=%v want %q", got, err, workerID)
	}

	// Single-use token cannot enroll a second worker.
	_, csr, _ := workeridentity.NewKeyAndCSR()
	_, err := h.client(nil).EnrollWorker(ctx, &grpcpb.EnrollWorkerRequest{EnrollmentToken: token, CsrPem: csr})
	requireCode(t, err, codes.Unauthenticated)

	worker := h.client(cert)
	resp, err := worker.RegisterWorker(ctx, &grpcpb.RegisterWorkerRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.WorkerId != workerID {
		t.Fatalf("registered worker_id=%q want authenticated %q", resp.WorkerId, workerID)
	}
}

func TestWorkerRPCsRequireCertificateAndRejectImpersonation(t *testing.T) {
	h := newIdentityHarness(t)
	ctx := context.Background()
	_, certA := h.enroll(h.enrollmentToken("default", 2))
	workerB, _ := h.enroll(h.enrollmentToken("default", 1))

	_, err := h.client(nil).Heartbeat(ctx, &grpcpb.HeartbeatRequest{WorkerId: workerB})
	requireCode(t, err, codes.Unauthenticated)

	_, err = h.client(certA).Heartbeat(ctx, &grpcpb.HeartbeatRequest{WorkerId: workerB})
	requireCode(t, err, codes.PermissionDenied)

	// A certificate from another CA is refused during the handshake.
	_, err = h.client(foreignWorkerCertificate(t)).Heartbeat(ctx, &grpcpb.HeartbeatRequest{})
	if err == nil {
		t.Fatal("certificate from a foreign CA must be rejected")
	}
}

func TestRevokedWorkerIsRejectedImmediately(t *testing.T) {
	h := newIdentityHarness(t)
	ctx := context.Background()
	workerID, cert := h.enroll(h.enrollmentToken("default", 1))
	worker := h.client(cert)
	if _, err := worker.Heartbeat(ctx, &grpcpb.HeartbeatRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.st.RevokeWorkerIdentityAudited(ctx, workerID, time.Now(), db.AuditRecord{Action: "test", ResourceType: "test", ResourceID: "test"}); err != nil {
		t.Fatal(err)
	}
	_, err := worker.Heartbeat(ctx, &grpcpb.HeartbeatRequest{})
	requireCode(t, err, codes.PermissionDenied)
}

func TestWorkerCertificateRenewalKeepsIdentity(t *testing.T) {
	h := newIdentityHarness(t)
	workerID, cert := h.enroll(h.enrollmentToken("default", 1))
	_, csr, _ := workeridentity.NewKeyAndCSR()
	resp, err := h.client(cert).RenewWorkerCertificate(context.Background(), &grpcpb.RenewWorkerCertificateRequest{CsrPem: csr})
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(resp.CertificatePem))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := workeridentity.IdentityFromCertificate(leaf); got != workerID {
		t.Fatalf("renewed certificate identity=%q want %q", got, workerID)
	}
}

func TestTasksAndCredentialsAreScopedToPoolAndLeaseholder(t *testing.T) {
	h := newIdentityHarness(t)
	ctx := context.Background()
	createPoolTestFixture(t, h.st)

	_, defaultCert := h.enroll(h.enrollmentToken("default", 2))
	_, otherDefaultCert := h.enroll(h.enrollmentToken("default", 1))
	_, restrictedCert := h.enroll(h.enrollmentToken("restricted", 1))
	defaultWorker, otherWorker, restrictedWorker := h.client(defaultCert), h.client(otherDefaultCert), h.client(restrictedCert)

	request := func(c grpcpb.ControlPlaneClient) *grpcpb.TaskAssignment {
		t.Helper()
		resp, err := c.RequestTask(ctx, &grpcpb.RequestTaskRequest{ProtocolVersion: WorkerProtocolVersion})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Task
	}

	restrictedTask := request(restrictedWorker)
	if restrictedTask.TaskId != "task-restricted" {
		t.Fatalf("restricted worker got task %q", restrictedTask.TaskId)
	}
	if request(restrictedWorker).TaskId != "" {
		t.Fatal("restricted worker must not receive default-pool tasks")
	}
	defaultTask := request(defaultWorker)
	if defaultTask.TaskId != "task-default" {
		t.Fatalf("default worker got task %q", defaultTask.TaskId)
	}

	credsReq := &grpcpb.GetTaskCredentialsRequest{TaskId: defaultTask.TaskId, AttemptId: defaultTask.AttemptId, FencingToken: defaultTask.FencingToken}
	creds, err := defaultWorker.GetTaskCredentials(ctx, credsReq)
	if err != nil {
		t.Fatal(err)
	}
	if creds.SourceDsn != "postgres://src" || creds.S3SecretAccessKey != "secret" {
		t.Fatalf("leaseholder credentials=%+v", creds)
	}
	// Another worker with the stolen attempt credentials gets nothing.
	_, err = otherWorker.GetTaskCredentials(ctx, credsReq)
	requireCode(t, err, codes.FailedPrecondition)
}

func TestSTSCredentialsAreScopedToRunPrefix(t *testing.T) {
	h := newIdentityHarness(t)
	ctx := context.Background()
	createPoolTestFixture(t, h.st, "credential_mode", "sts", "sts_role_arn", "arn:aws:iam::123456789012:role/orabbit-writer")
	var got stsRequest
	h.srv.assumeRoleFn = func(_ context.Context, req stsRequest) (aws.Credentials, error) {
		got = req
		return aws.Credentials{AccessKeyID: "tmp", SecretAccessKey: "tmp-secret", SessionToken: "session", CanExpire: true, Expires: time.Now().Add(time.Hour)}, nil
	}
	_, cert := h.enroll(h.enrollmentToken("default", 1))
	worker := h.client(cert)
	task, err := worker.RequestTask(ctx, &grpcpb.RequestTaskRequest{ProtocolVersion: WorkerProtocolVersion})
	if err != nil || task.Task.TaskId == "" {
		t.Fatalf("request task: %+v %v", task, err)
	}
	creds, err := worker.GetTaskCredentials(ctx, &grpcpb.GetTaskCredentialsRequest{TaskId: task.Task.TaskId, AttemptId: task.Task.AttemptId, FencingToken: task.Task.FencingToken})
	if err != nil {
		t.Fatal(err)
	}
	if creds.S3AccessKeyId != "tmp" || creds.S3SessionToken != "session" || creds.S3ExpiresAtUnixMs == 0 {
		t.Fatalf("worker must receive only temporary credentials: %+v", creds)
	}
	if got.AccessKeyID != "key" || got.RoleARN != "arn:aws:iam::123456789012:role/orabbit-writer" {
		t.Fatalf("master must assume the role with the stored keys: %+v", got)
	}
	wantResource := "arn:aws:s3:::bucket1/" + task.Task.S3Prefix + "/_runs/run-run-default/*"
	var policy struct {
		Statement []struct {
			Resource []string
		}
	}
	if err := json.Unmarshal([]byte(got.Policy), &policy); err != nil {
		t.Fatal(err)
	}
	if len(policy.Statement) != 1 || len(policy.Statement[0].Resource) != 1 || policy.Statement[0].Resource[0] != wantResource {
		t.Fatalf("session policy=%s want resource %s", got.Policy, wantResource)
	}
}

// createPoolTestFixture creates one pending task in the default pool and one
// in the "restricted" pool. targetMeta adds key/value pairs to the target
// connection metadata.
func createPoolTestFixture(t *testing.T, st *db.Store, targetMeta ...string) {
	t.Helper()
	ctx := context.Background()
	seal := func(id, v string) []byte {
		b, err := crypto.Encrypt(testCryptoKey, []byte(v), []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	meta := map[string]any{"endpoint": "http://minio:9000", "region": "us-east-1", "bucket": "bucket1", "prefix": "exports"}
	for i := 0; i+1 < len(targetMeta); i += 2 {
		meta[targetMeta[i]] = targetMeta[i+1]
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: "src", Name: "src", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: seal("src", `{"dsn":"postgres://src"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: "tgt", Name: "tgt", Kind: "target", Engine: "s3", MetadataJSON: mustJSONRaw(t, meta), SecretEncBlob: seal("tgt", `{"access_key_id":"key","secret_access_key":"secret"}`)}); err != nil {
		t.Fatal(err)
	}
	for _, pool := range []string{"default", "restricted"} {
		options := map[string]any{"table": "orders_" + pool}
		if pool != "default" {
			options["worker_pool"] = pool
		}
		if err := st.CreateJob(ctx, db.Job{ID: "job-" + pool, Name: "job-" + pool, SourceConnectionID: "src", TargetConnectionID: "tgt", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: mustJSONRaw(t, options)}); err != nil {
			t.Fatal(err)
		}
		if err := st.CreateRun(ctx, db.Run{ID: "run-" + pool, JobID: "job-" + pool, Status: "RUNNING", CorrelationID: "c-" + pool, StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
		if err := st.InsertTasks(ctx, []db.TaskInsert{{ID: "task-" + pool, RunID: "run-" + pool, TaskIndex: 1, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"}}); err != nil {
			t.Fatal(err)
		}
	}
}

// writeServerTLS writes a CA and a localhost server certificate signed by it.
func writeServerTLS(t *testing.T, dir string) (caFile, certFile, keyFile string) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test server CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	caFile, certFile, keyFile = filepath.Join(dir, "ca.crt"), filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	for path, block := range map[string]*pem.Block{caFile: {Type: "CERTIFICATE", Bytes: caDER}, certFile: {Type: "CERTIFICATE", Bytes: der}, keyFile: {Type: "PRIVATE KEY", Bytes: keyDER}} {
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return caFile, certFile, keyFile
}

// foreignWorkerCertificate is a well-formed worker certificate signed by a
// CA the master does not trust.
func foreignWorkerCertificate(t *testing.T) *tls.Certificate {
	t.Helper()
	caPEM, caKey, err := workeridentity.GenerateCA(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ca, err := workeridentity.ParseCA(caPEM, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, csrPEM, _ := workeridentity.NewKeyAndCSR()
	id, _ := workeridentity.NewWorkerID()
	issued, err := ca.Issue(csrPEM, id, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair([]byte(issued.CertificatePEM), []byte(keyPEM))
	if err != nil {
		t.Fatal(err)
	}
	return &cert
}

func TestIssueIgnoresCSRIdentityClaims(t *testing.T) {
	caPEM, caKey, _ := workeridentity.GenerateCA(time.Now())
	ca, _ := workeridentity.ParseCA(caPEM, caKey)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	csrDER, _ := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "admin"}, DNSNames: []string{"master"}}, key)
	csrPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}))
	id, _ := workeridentity.NewWorkerID()
	issued, err := ca.Issue(csrPEM, id, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(issued.CertificatePEM))
	leaf, _ := x509.ParseCertificate(block.Bytes)
	if leaf.Subject.CommonName != id || len(leaf.DNSNames) != 0 || len(leaf.URIs) != 1 || !strings.HasSuffix(leaf.URIs[0].String(), id) {
		t.Fatalf("certificate must carry only the master-chosen identity: cn=%q dns=%v uris=%v", leaf.Subject.CommonName, leaf.DNSNames, leaf.URIs)
	}
	if leaf.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth || leaf.IsCA {
		t.Fatal("worker certificate must be a client-auth leaf")
	}
}
