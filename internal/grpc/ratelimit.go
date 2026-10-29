package grpcapi

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/ratelimit"
	"github.com/LevonGhukas/O_Rabbit/internal/telemetry"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Metric surface and reason names; fixed values keep label cardinality bounded.
const (
	surfaceGRPCEnroll = "grpc_enroll"
	surfaceGRPCWorker = "grpc_worker"

	reasonEnrollRate   = "enroll_rate"
	reasonAuthFailures = "auth_failures"
)

// RateLimits configures per-peer limits on the worker control plane. Peers
// are keyed by their connection address. A zero Policy disables that limit;
// the zero RateLimits limits nothing.
type RateLimits struct {
	// Enroll bounds EnrollWorker calls. It is checked before the CSR is
	// parsed or a certificate is issued.
	Enroll ratelimit.Policy
	// AuthFailures is the budget of rejected shared worker tokens and
	// enrollment tokens. Once it is spent the peer is refused before its
	// credentials are checked, unless it presents a verified worker
	// certificate. Rejected certificates do not count: a worker with an
	// expired certificate must not lock out its neighbours.
	AuthFailures ratelimit.Policy

	now func() time.Time
}

type grpcLimiters struct {
	enroll       *ratelimit.Limiter
	authFailures *ratelimit.Limiter
}

// SetRateLimits enables per-peer limits. Call it before serving.
func (s *Server) SetRateLimits(cfg RateLimits) {
	s.limits = grpcLimiters{
		enroll:       ratelimit.New(cfg.Enroll, ratelimit.DefaultMaxKeys, cfg.now),
		authFailures: ratelimit.New(cfg.AuthFailures, ratelimit.DefaultMaxKeys, cfg.now),
	}
}

// rateLimitedWorkerAuth wraps the shared worker-token interceptor auth. It
// refuses peers that spent their failure budget or enrollment rate, then
// spends the failure budget when auth rejects the token or EnrollWorker
// rejects the enrollment token.
func (s *Server) rateLimitedWorkerAuth(auth grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !strings.HasPrefix(info.FullMethod, controlPlaneMethodPrefix) {
			return auth(ctx, req, info, handler)
		}
		key := peerKey(ctx)
		isEnroll := info.FullMethod == enrollWorkerMethod
		surface := surfaceGRPCWorker
		if isEnroll {
			surface = surfaceGRPCEnroll
		}
		// A verified worker certificate cannot be guessed, so enrolled
		// workers are not blocked by failures of others on their address.
		if !hasVerifiedCertificate(ctx) {
			if exhausted, wait := s.limits.authFailures.Exhausted(key); exhausted {
				return nil, s.rateLimited(key, surface, reasonAuthFailures, wait)
			}
		}
		if isEnroll {
			if ok, wait := s.limits.enroll.Allow(key); !ok {
				return nil, s.rateLimited(key, surface, reasonEnrollRate, wait)
			}
		}
		tokenAccepted := false
		resp, err := auth(ctx, req, info, func(ctx context.Context, req any) (any, error) {
			tokenAccepted = true
			return handler(ctx, req)
		})
		// After the shared token is accepted, Unauthenticated means a bad
		// enrollment token for EnrollWorker, but a certificate problem for
		// every other RPC, which is not a guessing attempt.
		if grpcstatus.Code(err) == codes.Unauthenticated && (!tokenAccepted || isEnroll) {
			telemetry.ObserveAuthFailure(surface)
			s.limits.authFailures.Allow(key)
		}
		return resp, err
	}
}

func (s *Server) rateLimited(key, surface, reason string, wait time.Duration) error {
	telemetry.ObserveRateLimited(surface, reason)
	// Debug, not Warn: a flood must not turn into a flood of log lines. The
	// counter above is the operational signal.
	s.log.Debug("worker RPC rate limited", slog.String("peer", key), slog.String("surface", surface), slog.String("reason", reason))
	st := grpcstatus.New(codes.ResourceExhausted, fmt.Sprintf("rate limit exceeded; retry in %s", wait))
	if detailed, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(wait)}); err == nil {
		st = detailed
	}
	return st.Err()
}

func hasVerifiedCertificate(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	return ok && len(tlsInfo.State.VerifiedChains) > 0
}

func peerKey(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ratelimit.UnknownClient
	}
	return ratelimit.PeerKey(p.Addr.String())
}
