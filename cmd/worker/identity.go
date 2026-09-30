package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	grpcapi "github.com/LevonGhukas/O_Rabbit/internal/grpc"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/workeridentity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const workerIdentityFile = "worker-identity.pem"

// workerIdentity holds the worker's master-issued certificate and private
// key. Both are persisted together in one file so a renewal can never leave a
// mismatched key and certificate on disk.
type workerIdentity struct {
	path string

	mu   sync.RWMutex
	cert *tls.Certificate
	id   string
}

// loadWorkerIdentity reads a persisted identity. It returns (nil, nil) when
// the worker has not enrolled yet.
func loadWorkerIdentity(dir string) (*workerIdentity, error) {
	w := &workerIdentity{path: filepath.Join(dir, workerIdentityFile)}
	b, err := os.ReadFile(w.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read worker identity: %w", err)
	}
	if err := w.set(b); err != nil {
		return nil, fmt.Errorf("worker identity %s: %w", w.path, err)
	}
	return w, nil
}

func (w *workerIdentity) set(bundle []byte) error {
	var certPEM, keyPEM []byte
	for rest := bundle; ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		encoded := pem.EncodeToMemory(block)
		switch block.Type {
		case "CERTIFICATE":
			certPEM = append(certPEM, encoded...)
		case "PRIVATE KEY":
			keyPEM = encoded
		}
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return err
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	id, err := workeridentity.IdentityFromCertificate(leaf)
	if err != nil {
		return err
	}
	cert.Leaf = leaf
	w.mu.Lock()
	w.cert, w.id = &cert, id
	w.mu.Unlock()
	return nil
}

// install validates and atomically persists a new key and certificate, then
// makes it the certificate presented on new connections.
func (w *workerIdentity) install(keyPEM, certPEM string) error {
	bundle := []byte(certPEM + keyPEM)
	probe := &workerIdentity{}
	if err := probe.set(bundle); err != nil {
		return fmt.Errorf("master returned an unusable certificate: %w", err)
	}
	if w.id != "" && probe.id != w.id {
		return fmt.Errorf("master returned a certificate for another worker")
	}
	if err := os.MkdirAll(filepath.Dir(w.path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(w.path), ".worker-identity-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(bundle); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), w.path); err != nil {
		return err
	}
	return w.set(bundle)
}

func (w *workerIdentity) workerID() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.id
}

func (w *workerIdentity) leaf() *x509.Certificate {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cert.Leaf
}

// clientCertificate is the TLS GetClientCertificate source.
func (w *workerIdentity) clientCertificate() (*tls.Certificate, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cert, nil
}

// enrollWorker exchanges a one-time enrollment token for a master-issued
// identity. The private key is generated here and never sent.
func enrollWorker(ctx context.Context, log *slog.Logger, cfg workerConfig, dir string) (*workerIdentity, error) {
	token := strings.TrimSpace(cfg.EnrollmentToken)
	if token == "" {
		return nil, errors.New("worker is not enrolled: set ORABBIT_WORKER_ENROLLMENT_TOKEN (created with POST /workers/enrollment-tokens) for the first start")
	}
	tlsCfg, err := grpcapi.ClientTLSConfig(cfg.TLSCAFile, cfg.TLSServerName, nil)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(cfg.MasterAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithUnaryInterceptor(grpcapi.WorkerAuthUnaryClientInterceptor(cfg.WorkerAuthToken)),
		grpc.WithKeepaliveParams(grpcapi.WorkerKeepaliveParams),
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	keyPEM, csrPEM, err := workeridentity.NewKeyAndCSR()
	if err != nil {
		return nil, err
	}
	hostname, _ := os.Hostname()
	name := strings.TrimSpace(cfg.WorkerID)
	if name == "" {
		name = hostname
	}
	req := &grpcpb.EnrollWorkerRequest{EnrollmentToken: token, CsrPem: csrPEM, Name: name, Hostname: hostname}
	cp := grpcpb.NewControlPlaneClient(conn)
	backoff := time.Second
	for {
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, err := cp.EnrollWorker(callCtx, req)
		cancel()
		if err == nil {
			w := &workerIdentity{path: filepath.Join(dir, workerIdentityFile)}
			if err := w.install(keyPEM, resp.CertificatePem); err != nil {
				return nil, err
			}
			log.Info("worker enrolled", slog.String("worker_id", resp.WorkerId), slog.String("pool", resp.Pool), slog.Time("cert_not_after", time.UnixMilli(resp.NotAfterUnixMs)))
			return w, nil
		}
		if code := status.Code(err); code != codes.Unavailable && code != codes.DeadlineExceeded {
			return nil, fmt.Errorf("enroll worker: %w", err)
		}
		log.Warn("enrollment failed, retrying", slog.String("err", err.Error()), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

// renewLoop renews the identity certificate once two thirds of its lifetime
// has passed, retrying until it expires. The master bounds connection age, so
// the renewed certificate is used after the next reconnect.
func (w *workerIdentity) renewLoop(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient) {
	for {
		leaf := w.leaf()
		lifetime := leaf.NotAfter.Sub(leaf.NotBefore)
		wait := time.Until(leaf.NotBefore.Add(lifetime * 2 / 3))
		select {
		case <-ctx.Done():
			return
		case <-time.After(max(wait, 0)):
		}
		err := w.renew(ctx, cp)
		if err == nil {
			log.Info("worker certificate renewed", slog.String("worker_id", w.workerID()), slog.Time("not_after", w.leaf().NotAfter))
			continue
		}
		if status.Code(err) == codes.PermissionDenied {
			log.Error("worker identity revoked; certificate will not be renewed", slog.String("worker_id", w.workerID()), slog.String("err", err.Error()))
			return
		}
		retry := min(time.Minute, max(time.Until(leaf.NotAfter)/10, time.Second))
		log.Warn("worker certificate renewal failed, retrying", slog.String("err", err.Error()), slog.Duration("retry_in", retry), slog.Time("not_after", leaf.NotAfter))
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
}

func (w *workerIdentity) renew(ctx context.Context, cp grpcpb.ControlPlaneClient) error {
	keyPEM, csrPEM, err := workeridentity.NewKeyAndCSR()
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := cp.RenewWorkerCertificate(callCtx, &grpcpb.RenewWorkerCertificateRequest{WorkerId: w.workerID(), CsrPem: csrPEM})
	if err != nil {
		return err
	}
	return w.install(keyPEM, resp.CertificatePem)
}
