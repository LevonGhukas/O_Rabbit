package grpcapi

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/workeridentity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const enrollWorkerMethod = controlPlaneMethodPrefix + "EnrollWorker"

var workerCAKeyAAD = db.WorkerCAKeyAAD

// authenticatedWorker is the identity the master derived from a verified
// worker certificate.
type authenticatedWorker struct {
	ID   string
	Pool string
}

type authenticatedWorkerKey struct{}

func authenticatedWorkerFrom(ctx context.Context) (authenticatedWorker, bool) {
	w, ok := ctx.Value(authenticatedWorkerKey{}).(authenticatedWorker)
	return w, ok
}

// LoadWorkerCA returns the master's worker CA, creating and persisting one on
// first use. The CA private key is stored sealed with the master key.
func LoadWorkerCA(ctx context.Context, st *db.Store, k crypto.Key, now time.Time) (*workeridentity.CA, error) {
	certPEM, keyEnc, err := st.LoadOrCreateWorkerCA(ctx, "", nil)
	if errors.Is(err, sql.ErrNoRows) {
		newCert, keyDER, genErr := workeridentity.GenerateCA(now)
		if genErr != nil {
			return nil, genErr
		}
		sealed, encErr := crypto.Encrypt(k, keyDER, workerCAKeyAAD)
		if encErr != nil {
			return nil, encErr
		}
		certPEM, keyEnc, err = st.LoadOrCreateWorkerCA(ctx, newCert, sealed)
	}
	if err != nil {
		return nil, err
	}
	keyDER, err := crypto.Decrypt(k, keyEnc, workerCAKeyAAD)
	if err != nil {
		return nil, err
	}
	return workeridentity.ParseCA(certPEM, keyDER)
}

// SetWorkerIdentity enables master-issued worker identities: enrollment,
// certificate renewal, and certificate-bound authorization of worker RPCs.
func (s *Server) SetWorkerIdentity(ca *workeridentity.CA, certTTL time.Duration) {
	s.workerCA = ca
	if certTTL > 0 {
		s.workerCertTTL = certTTL
	}
}

// workerIdentityUnaryInterceptor authenticates worker RPCs from the verified
// TLS client certificate. The certificate's worker ID is authoritative: a
// request worker_id that names another worker is rejected, and an empty one
// is filled in, so every handler, lease, and audit record uses the
// authenticated identity. With TLS disabled (loopback development) there is
// no certificate and the request worker_id is trusted as before.
func (s *Server) workerIdentityUnaryInterceptor(tlsEnabled bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !tlsEnabled || !strings.HasPrefix(info.FullMethod, controlPlaneMethodPrefix) || info.FullMethod == enrollWorkerMethod {
			return handler(ctx, req)
		}
		worker, err := s.authenticateWorker(ctx)
		if err != nil {
			s.log.Warn("worker RPC rejected", slog.String("method", info.FullMethod), slog.String("reason", err.Error()))
			return nil, err
		}
		if err := bindRequestWorkerID(req, worker.ID); err != nil {
			s.log.Warn("worker RPC rejected", slog.String("method", info.FullMethod), slog.String("worker_id", worker.ID), slog.String("reason", err.Error()))
			return nil, err
		}
		return handler(context.WithValue(ctx, authenticatedWorkerKey{}, worker), req)
	}
}

func (s *Server) authenticateWorker(ctx context.Context) (authenticatedWorker, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return authenticatedWorker{}, grpcstatus.Error(codes.Unauthenticated, "worker certificate required")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return authenticatedWorker{}, grpcstatus.Error(codes.Unauthenticated, "worker certificate required; enroll the worker first")
	}
	cert := tlsInfo.State.VerifiedChains[0][0]
	if s.nowFn().After(cert.NotAfter) {
		return authenticatedWorker{}, grpcstatus.Error(codes.Unauthenticated, "worker certificate expired")
	}
	id, err := workeridentity.IdentityFromCertificate(cert)
	if err != nil {
		return authenticatedWorker{}, grpcstatus.Error(codes.Unauthenticated, err.Error())
	}
	ident, err := s.st.GetWorkerIdentity(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return authenticatedWorker{}, grpcstatus.Error(codes.PermissionDenied, "worker identity is unknown or revoked")
	}
	if err != nil {
		return authenticatedWorker{}, err
	}
	if ident.Status != db.WorkerIdentityActive {
		return authenticatedWorker{}, grpcstatus.Error(codes.PermissionDenied, "worker identity is unknown or revoked")
	}
	return authenticatedWorker{ID: ident.ID, Pool: ident.Pool}, nil
}

// bindRequestWorkerID makes the request's worker_id field equal to the
// authenticated identity, rejecting a conflicting value.
func bindRequestWorkerID(req any, workerID string) error {
	m, ok := req.(proto.Message)
	if !ok {
		return nil
	}
	msg := m.ProtoReflect()
	fd := msg.Descriptor().Fields().ByName("worker_id")
	if fd == nil || fd.Kind() != protoreflect.StringKind {
		return nil
	}
	if claimed := strings.TrimSpace(msg.Get(fd).String()); claimed != "" && claimed != workerID {
		return grpcstatus.Error(codes.PermissionDenied, "request worker_id does not match the authenticated worker certificate")
	}
	msg.Set(fd, protoreflect.ValueOfString(workerID))
	return nil
}

func (s *Server) EnrollWorker(ctx context.Context, req *grpcpb.EnrollWorkerRequest) (*grpcpb.EnrollWorkerResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	if s.workerCA == nil {
		return nil, grpcstatus.Error(codes.FailedPrecondition, "worker enrollment requires gRPC TLS on the master")
	}
	token := strings.TrimSpace(req.EnrollmentToken)
	if token == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "enrollment_token is required")
	}
	workerID, err := workeridentity.NewWorkerID()
	if err != nil {
		return nil, err
	}
	now := s.nowFn()
	issued, err := s.workerCA.Issue(req.CsrPem, workerID, s.workerCertTTL, now)
	if err != nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, err.Error())
	}
	digest := sha256.Sum256([]byte(token))
	ident, err := s.st.EnrollWorkerIdentity(ctx, hex.EncodeToString(digest[:]), workerID, truncate(req.Name, 128), truncate(req.Hostname, 255), issued.Serial, issued.NotAfter, now)
	if errors.Is(err, db.ErrEnrollmentTokenInvalid) {
		s.log.Warn("worker enrollment rejected", slog.String("reason", "invalid or expired enrollment token"), slog.String("hostname", truncate(req.Hostname, 255)))
		return nil, grpcstatus.Error(codes.Unauthenticated, "enrollment token is invalid or expired")
	}
	if err != nil {
		return nil, err
	}
	s.log.Info("worker enrolled", slog.String("worker_id", ident.ID), slog.String("name", ident.Name), slog.String("pool", ident.Pool), slog.String("enrollment_token_id", ident.EnrollmentTokenID), slog.String("cert_serial", issued.Serial))
	return &grpcpb.EnrollWorkerResponse{
		WorkerId:         ident.ID,
		CertificatePem:   issued.CertificatePEM,
		CaCertificatePem: s.workerCA.CertificatePEM(),
		NotAfterUnixMs:   issued.NotAfter.UnixMilli(),
		Pool:             ident.Pool,
	}, nil
}

func (s *Server) RenewWorkerCertificate(ctx context.Context, req *grpcpb.RenewWorkerCertificateRequest) (*grpcpb.RenewWorkerCertificateResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	worker, ok := authenticatedWorkerFrom(ctx)
	if !ok || s.workerCA == nil {
		return nil, grpcstatus.Error(codes.FailedPrecondition, "certificate renewal requires an authenticated worker over gRPC TLS")
	}
	now := s.nowFn()
	issued, err := s.workerCA.Issue(req.CsrPem, worker.ID, s.workerCertTTL, now)
	if err != nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.st.RecordWorkerCertificate(ctx, worker.ID, issued.Serial, issued.NotAfter, now); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, grpcstatus.Error(codes.PermissionDenied, "worker identity is unknown or revoked")
		}
		return nil, err
	}
	s.log.Info("worker certificate renewed", slog.String("worker_id", worker.ID), slog.String("cert_serial", issued.Serial), slog.Time("not_after", issued.NotAfter))
	return &grpcpb.RenewWorkerCertificateResponse{CertificatePem: issued.CertificatePEM, NotAfterUnixMs: issued.NotAfter.UnixMilli()}, nil
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return s[:n]
	}
	return s
}
