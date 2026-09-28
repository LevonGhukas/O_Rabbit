package grpcapi

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string
}

func newTestCA(t *testing.T, dir, name string) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	file := filepath.Join(dir, name+".crt")
	writePEM(t, file, "CERTIFICATE", der)
	return testCA{cert: cert, key: key, file: file}
}

// issue writes a leaf certificate signed by ca and returns its cert/key paths.
func (ca testCA) issue(t *testing.T, dir, name string, usage x509.ExtKeyUsage) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

func writePEM(t *testing.T, path, kind string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func startMTLSHealthServer(t *testing.T, certFile, keyFile, clientCAFile string) string {
	t.Helper()
	tlsCfg, err := ServerTLSConfig(certFile, keyFile, clientCAFile)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func checkHealth(t *testing.T, addr, caFile, certFile, keyFile string) error {
	t.Helper()
	tlsCfg, err := ClientTLSConfig(caFile, "localhost", certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	return err
}

func TestMutualTLSRequiresWorkerCertificateFromClientCA(t *testing.T) {
	dir := t.TempDir()
	serverCA := newTestCA(t, dir, "server-ca")
	workerCA := newTestCA(t, dir, "worker-ca")
	rogueCA := newTestCA(t, dir, "rogue-ca")
	serverCert, serverKey := serverCA.issue(t, dir, "master", x509.ExtKeyUsageServerAuth)
	workerCert, workerKey := workerCA.issue(t, dir, "worker", x509.ExtKeyUsageClientAuth)
	rogueCert, rogueKey := rogueCA.issue(t, dir, "rogue", x509.ExtKeyUsageClientAuth)

	addr := startMTLSHealthServer(t, serverCert, serverKey, workerCA.file)

	if err := checkHealth(t, addr, serverCA.file, workerCert, workerKey); err != nil {
		t.Fatalf("worker with a client certificate from the client CA must connect: %v", err)
	}
	if err := checkHealth(t, addr, serverCA.file, "", ""); err == nil {
		t.Fatal("worker without a client certificate must be rejected")
	}
	if err := checkHealth(t, addr, serverCA.file, rogueCert, rogueKey); err == nil {
		t.Fatal("worker certificate from another CA must be rejected")
	}
}

func TestServerTLSWithoutClientCAAcceptsCertificatelessWorkers(t *testing.T) {
	dir := t.TempDir()
	serverCA := newTestCA(t, dir, "server-ca")
	serverCert, serverKey := serverCA.issue(t, dir, "master", x509.ExtKeyUsageServerAuth)

	addr := startMTLSHealthServer(t, serverCert, serverKey, "")
	if err := checkHealth(t, addr, serverCA.file, "", ""); err != nil {
		t.Fatalf("server-only TLS must accept workers without client certificates: %v", err)
	}
}

func TestTLSConfigValidation(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir, "ca")
	cert, key := ca.issue(t, dir, "leaf", x509.ExtKeyUsageClientAuth)
	notPEM := filepath.Join(dir, "not.pem")
	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := ServerTLSConfig("", key, ""); err == nil {
		t.Fatal("server TLS without a certificate must fail")
	}
	if _, err := ServerTLSConfig(cert, key, notPEM); err == nil || !strings.Contains(err.Error(), "no PEM certificates") {
		t.Fatalf("invalid client CA must fail, got %v", err)
	}
	if _, err := ClientTLSConfig(ca.file, "", cert, ""); err == nil {
		t.Fatal("client certificate without a key must fail")
	}
	if _, err := ClientTLSConfig(notPEM, "", "", ""); err == nil {
		t.Fatal("invalid server CA must fail")
	}
}
