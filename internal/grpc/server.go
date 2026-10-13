// internal/grpc/server.go
// The gRPC control plane server: configuration, policies, leadership and
// serving. Worker RPCs are in worker_rpcs.go, run commits in commit_run.go,
// and background reconciliation/cleanup loops in cleanup_loops.go.

package grpcapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	httpapi "github.com/LevonGhukas/O_Rabbit/internal/http"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	"github.com/LevonGhukas/O_Rabbit/internal/telemetry"
	"github.com/LevonGhukas/O_Rabbit/internal/workeridentity"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	grpcstatus "google.golang.org/grpc/status"
)

// Config holds the gRPC server configuration.
type Config struct {
	Addr              string
	Insecure          bool
	TLSCertFile       string
	TLSKeyFile        string
	WorkerAuthToken   string
	HeartbeatInterval time.Duration
}

type icebergRegistrationRunner interface {
	RegisterRun(context.Context, icebergreg.RunRequest) (icebergreg.RunResult, error)
}

// Server implements the gRPC control plane server.
type Server struct {
	commitCatalog CommitCatalogPolicy
	grpcpb.UnimplementedControlPlaneServer

	log *slog.Logger
	st  *db.Store
	bc  *httpapi.Broadcaster
	k   crypto.Key

	hbInterval time.Duration

	icebergRegistrar           icebergRegistrationRunner
	commitRunFn                func(context.Context, string) error
	completeRunCommitFn        func(context.Context, string) error
	runIcebergRegistrationFn   func(context.Context, string) (bool, icebergreg.RunResult, error)
	newCommitObjectStoreFn     func(context.Context, s3io.Config) (commitObjectStore, error)
	upsertHWMFn                func(context.Context, string, string) error
	leasePolicy                db.LeasePolicy
	nowFn                      func() time.Time
	attemptIDFn                func() (string, error)
	fencingTokenFn             func() (string, error)
	leadership                 interface{ Assert(context.Context) error }
	multipartCleanupGrace      time.Duration
	multipartCleanupRetry      time.Duration
	multipartCleanupMax        int
	newMultipartCleanerFn      func(context.Context, s3io.Config) (multipartCleaner, error)
	canceledObjectRetry        time.Duration
	canceledObjectMax          int
	canceledObjectDryRun       bool
	newCanceledObjectCleanerFn func(context.Context, s3io.Config) (canceledObjectCleaner, error)
	catalogWorkSlots           chan struct{}
	uploadCapacityLimit        int
	uploadCapacityLeaseTTL     time.Duration
	workerCA                   *workeridentity.CA
	workerCertTTL              time.Duration
	assumeRoleFn               func(context.Context, stsRequest) (aws.Credentials, error)
}

type multipartCleaner interface {
	VerifyTrackedFinalObject(context.Context, string, int64, string, map[string]string) (bool, error)
	ListManagedMultipartUploads(context.Context, string, int) ([]s3io.MultipartUploadInfo, error)
	AbortTrackedMultipart(context.Context, string, string) error
	MultipartUploadExists(context.Context, string, string, string) (bool, error)
}

type canceledObjectCleaner interface {
	ObserveExactObject(context.Context, string, int64, string, map[string]string) (s3io.ExactObjectObservation, error)
	DeleteExactObject(context.Context, string, string) error
}

// WorkerProtocolVersion is the worker/master protocol revision. Version 6
// moved task secrets from TaskAssignment to GetTaskCredentials; version 7
// moved multipart lifecycle reports to ReportMultipartLifecycle.
const WorkerProtocolVersion = 7

const workerProtocolVersion = WorkerProtocolVersion

// ListenAndServe starts the gRPC server and listens for incoming connections.
const (
	// maxRecvMsgBytes bounds each worker request. Results and progress
	// payloads are far smaller; the limit caps worker-supplied data.
	maxRecvMsgBytes = 4 << 20
	maxSendMsgBytes = 16 << 20

	// gracefulStopTimeout bounds shutdown: a long commit running inside
	// ReportTaskResult must not keep a stopping master alive indefinitely.
	// Its run stays COMMITTING and is resumed by the next leader.
	gracefulStopTimeout = 30 * time.Second
)

const (
	maxWorkerProgressMessageBytes = 4 << 10
	maxWorkerProgressFieldsBytes  = 64 << 10
	maxMultipartErrorMessageBytes = 4 << 10
	maxMultipartObjectKeyBytes    = 4 << 10
	maxMultipartUploadIDBytes     = 2 << 10
	maxMultipartErrorClassBytes   = 256
)

// NewServer creates a new gRPC server instance.
func NewServer(log *slog.Logger, st *db.Store, bc *httpapi.Broadcaster, k crypto.Key, hbInterval time.Duration, registrar icebergRegistrationRunner) *Server {
	if log == nil {
		log = slog.Default()
	}
	if hbInterval <= 0 {
		hbInterval = 5 * time.Second
	}
	s := &Server{
		log:              log,
		st:               st,
		bc:               bc,
		k:                k,
		hbInterval:       hbInterval,
		icebergRegistrar: registrar,
	}
	s.commitRunFn = s.commitRun
	s.completeRunCommitFn = st.CompleteRunCommit
	s.runIcebergRegistrationFn = s.runIcebergRegistration
	s.newCommitObjectStoreFn = func(ctx context.Context, cfg s3io.Config) (commitObjectStore, error) {
		u, err := s3io.New(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return s3CommitObjectStore{uploader: u}, nil
	}
	s.upsertHWMFn = st.UpsertHWM
	s.leasePolicy = db.LeasePolicy{Duration: 30 * time.Second, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: 30 * time.Second}
	s.nowFn = time.Now
	s.multipartCleanupGrace, s.multipartCleanupRetry, s.multipartCleanupMax = 15*time.Minute, time.Minute, 5
	s.newMultipartCleanerFn = func(ctx context.Context, cfg s3io.Config) (multipartCleaner, error) {
		return s3io.New(ctx, cfg)
	}
	s.canceledObjectRetry, s.canceledObjectMax, s.canceledObjectDryRun = 5*time.Minute, 5, true
	s.catalogWorkSlots = make(chan struct{}, 2)
	s.uploadCapacityLimit, s.uploadCapacityLeaseTTL = 8, 2*time.Minute
	s.newCanceledObjectCleanerFn = func(ctx context.Context, cfg s3io.Config) (canceledObjectCleaner, error) {
		return s3io.New(ctx, cfg)
	}
	s.workerCertTTL = 24 * time.Hour
	s.commitCatalog = DefaultCommitCatalogPolicy()
	s.assumeRoleFn = assumeRoleWithSTS
	return s
}

func (s *Server) SetLeasePolicy(policy db.LeasePolicy) { s.leasePolicy = policy }

// CommitCatalogPolicy holds the timeouts, leases and retry budgets of run
// publication and catalog registration/reconciliation.
type CommitCatalogPolicy struct {
	// CommitTimeout bounds one publication attempt of a run.
	CommitTimeout time.Duration
	// CommitMaxAttempts bounds retries of a failing commit.
	CommitMaxAttempts int
	// RegistrationTimeout bounds one catalog registration.
	RegistrationTimeout time.Duration
	Registration        db.RegistrationPolicy
	// ReconciliationLease and ReconciliationMaxAttempts govern catalog
	// observation after an ambiguous registration.
	ReconciliationLease       time.Duration
	ReconciliationMaxAttempts int
}

// DefaultCommitCatalogPolicy returns the built-in commit and catalog policy.
func DefaultCommitCatalogPolicy() CommitCatalogPolicy {
	return CommitCatalogPolicy{
		CommitTimeout:             30 * time.Minute,
		CommitMaxAttempts:         5,
		RegistrationTimeout:       30 * time.Minute,
		Registration:              db.RegistrationPolicy{LeaseDuration: 30 * time.Second, MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Minute},
		ReconciliationLease:       30 * time.Second,
		ReconciliationMaxAttempts: 5,
	}
}

// SetCommitCatalogPolicy replaces the commit and catalog policy. Zero fields
// keep their defaults.
func (s *Server) SetCommitCatalogPolicy(p CommitCatalogPolicy) {
	d := DefaultCommitCatalogPolicy()
	if p.CommitTimeout <= 0 {
		p.CommitTimeout = d.CommitTimeout
	}
	if p.CommitMaxAttempts <= 0 {
		p.CommitMaxAttempts = d.CommitMaxAttempts
	}
	if p.RegistrationTimeout <= 0 {
		p.RegistrationTimeout = d.RegistrationTimeout
	}
	if p.Registration.LeaseDuration <= 0 {
		p.Registration.LeaseDuration = d.Registration.LeaseDuration
	}
	if p.Registration.MaxAttempts <= 0 {
		p.Registration.MaxAttempts = d.Registration.MaxAttempts
	}
	if p.Registration.BackoffBase <= 0 {
		p.Registration.BackoffBase = d.Registration.BackoffBase
	}
	if p.Registration.BackoffMax <= 0 {
		p.Registration.BackoffMax = d.Registration.BackoffMax
	}
	if p.ReconciliationLease <= 0 {
		p.ReconciliationLease = d.ReconciliationLease
	}
	if p.ReconciliationMaxAttempts <= 0 {
		p.ReconciliationMaxAttempts = d.ReconciliationMaxAttempts
	}
	s.commitCatalog = p
}

func (s *Server) SetUploadCapacityPolicy(limit int, ttl time.Duration) {
	if limit > 0 {
		s.uploadCapacityLimit = limit
	}
	if ttl > 0 {
		s.uploadCapacityLeaseTTL = ttl
	}
}

func (s *Server) SetLeadershipGuard(guard interface{ Assert(context.Context) error }) {
	s.leadership = guard
}

func (s *Server) SetMultipartCleanupPolicy(grace, retry time.Duration, max int) {
	if grace > 0 {
		s.multipartCleanupGrace = grace
	}
	if retry > 0 {
		s.multipartCleanupRetry = retry
	}
	if max > 0 {
		s.multipartCleanupMax = max
	}
}

func (s *Server) SetCanceledObjectCleanupPolicy(retry time.Duration, max int, dryRun bool) {
	if retry > 0 {
		s.canceledObjectRetry = retry
	}
	if max > 0 {
		s.canceledObjectMax = max
	}
	s.canceledObjectDryRun = dryRun
}

func (s *Server) requireLeadership(ctx context.Context) error {
	if s.leadership == nil {
		return nil
	}
	if err := s.leadership.Assert(ctx); err != nil {
		return grpcstatus.Error(codes.Unavailable, "master is not the active leader")
	}
	return nil
}

func (s *Server) leadershipContext(ctx context.Context) (context.Context, context.CancelFunc) {
	leader, ok := s.leadership.(interface{ WorkContext() context.Context })
	if !ok || leader.WorkContext() == nil {
		return context.WithCancel(ctx)
	}
	combined, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(leader.WorkContext(), cancel)
	return combined, func() {
		stop()
		cancel()
	}
}

func stringMapValue(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

// maxPartNumber scans the list of S3 object keys to find the maximum logical part number based on
// the naming convention "part-xxxxxx.parquet" or "part-xxxxxx-yyy.parquet".
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// orJSON returns the input string if it is non-empty and non-whitespace; otherwise, it returns an empty JSON object "{}".
func orJSON(s string) string {
	if strings.TrimSpace(s) == "" {
		return "{}"
	}
	return s
}

// newID generates a new random ID as a hex string. It is used for worker IDs, event IDs, etc.
func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// serverOptions are the control-plane server options besides credentials.
func serverOptions(cfg Config, srv *Server) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			telemetry.UnaryServerInterceptor(),
			recoveryUnaryServerInterceptor(srv.log),
			workerAuthUnaryServerInterceptor(cfg.WorkerAuthToken),
			srv.workerIdentityUnaryInterceptor(!cfg.Insecure),
		),
		grpc.ChainStreamInterceptor(recoveryStreamServerInterceptor(srv.log)),
		grpc.MaxRecvMsgSize(maxRecvMsgBytes),
		grpc.MaxSendMsgSize(maxSendMsgBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			// Ping connections idle this long and drop them if the ping is
			// not acknowledged, so vanished workers are detected promptly.
			Time:    2 * time.Minute,
			Timeout: 20 * time.Second,
			// Bounded connection age makes workers re-handshake periodically,
			// so a renewed identity certificate replaces the one a connection
			// was opened with. The grace period covers long result RPCs.
			MaxConnectionAge:      time.Hour,
			MaxConnectionAgeGrace: 35 * time.Minute,
		}),
		// Accept the worker's keepalive pings (WorkerKeepaliveParams); pings
		// more frequent than MinTime are answered with GOAWAY.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             WorkerKeepaliveParams.Time / 2,
			PermitWithoutStream: false,
		}),
	}
}

// WorkerKeepaliveParams are the client keepalive settings workers use when
// dialing the master; they must stay within the server enforcement policy.
var WorkerKeepaliveParams = keepalive.ClientParameters{
	Time:                30 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: false,
}

func stopGracefully(g *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		g.Stop()
		<-done
	}
}

func ListenAndServe(ctx context.Context, cfg Config, srv *Server) error {
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	return Serve(ctx, lis, cfg, srv)
}

// Serve runs the control-plane gRPC server on lis until ctx is canceled.
func Serve(ctx context.Context, lis net.Listener, cfg Config, srv *Server) error {

	var creds credentials.TransportCredentials
	if cfg.Insecure {
		creds = insecure.NewCredentials()
	} else {
		if srv.workerCA == nil {
			_ = lis.Close()
			return errors.New("gRPC TLS requires the worker identity CA; call SetWorkerIdentity")
		}
		tlsCfg, err := ServerTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, srv.workerCA.Pool())
		if err != nil {
			_ = lis.Close()
			return err
		}
		creds = credentials.NewTLS(tlsCfg)
	}

	g := grpc.NewServer(append([]grpc.ServerOption{grpc.Creds(creds)}, serverOptions(cfg, srv)...)...)
	grpcpb.RegisterControlPlaneServer(g, srv)
	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(g, healthSrv)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	errCh := make(chan error, 1)
	go func() { errCh <- g.Serve(lis) }()

	select {
	case <-ctx.Done():
		healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		stopGracefully(g, gracefulStopTimeout)
		return nil
	case err := <-errCh:
		return err
	}
}
