package grpcapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/ratelimit"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	grpcstatus "google.golang.org/grpc/status"
)

type grpcTestClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *grpcTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *grpcTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

const limitTestWorkerToken = "worker-token"

func peerContext(ip, token string) context.Context {
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 40000}})
	if token != "" {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(workerAuthorizationMetadataKey, "Bearer "+token))
	}
	return ctx
}

// limitedChain is the production order: rate limits wrap the shared-token
// check, and handler stands for everything after it (identity check, RPC).
func limitedChain(srv *Server, handler grpc.UnaryHandler) func(ctx context.Context, method string) error {
	interceptor := srv.rateLimitedWorkerAuth(workerAuthUnaryServerInterceptor(limitTestWorkerToken))
	return func(ctx context.Context, method string) error {
		_, err := interceptor(ctx, struct{}{}, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		return err
	}
}

func okHandler(context.Context, any) (any, error) { return struct{}{}, nil }

func newLimitedServer(limits RateLimits) *Server {
	srv := NewServer(nil, nil, nil, testCryptoKey, time.Second, nil)
	srv.SetRateLimits(limits)
	return srv
}

func TestRepeatedInvalidWorkerTokensAreThrottled(t *testing.T) {
	clock := &grpcTestClock{t: time.Unix(0, 0)}
	call := limitedChain(newLimitedServer(RateLimits{AuthFailures: ratelimit.Policy{Rate: 0.2, Burst: 2}, now: clock.Now}), okHandler)
	heartbeat := grpcpb.ControlPlane_Heartbeat_FullMethodName

	for range 2 {
		requireCode(t, call(peerContext("203.0.113.1", "wrong"), heartbeat), codes.Unauthenticated)
	}
	err := call(peerContext("203.0.113.1", "wrong"), heartbeat)
	requireCode(t, err, codes.ResourceExhausted)
	if got := retryDelay(t, err); got != 5*time.Second {
		t.Fatalf("RetryInfo delay=%v, want 5s", got)
	}
	// The block holds for the valid token too, and only for that peer.
	requireCode(t, call(peerContext("203.0.113.1", limitTestWorkerToken), heartbeat), codes.ResourceExhausted)
	requireCode(t, call(peerContext("203.0.113.2", limitTestWorkerToken), heartbeat), codes.OK)

	clock.Advance(5 * time.Second)
	requireCode(t, call(peerContext("203.0.113.1", limitTestWorkerToken), heartbeat), codes.OK)
}

func TestInvalidEnrollmentTokensAreThrottled(t *testing.T) {
	invalidToken := func(context.Context, any) (any, error) {
		return nil, grpcstatus.Error(codes.Unauthenticated, "enrollment token is invalid or expired")
	}
	call := limitedChain(newLimitedServer(RateLimits{AuthFailures: ratelimit.Policy{Rate: 0.1, Burst: 3}, now: (&grpcTestClock{}).Now}), invalidToken)

	for range 3 {
		requireCode(t, call(peerContext("203.0.113.1", limitTestWorkerToken), enrollWorkerMethod), codes.Unauthenticated)
	}
	requireCode(t, call(peerContext("203.0.113.1", limitTestWorkerToken), enrollWorkerMethod), codes.ResourceExhausted)
}

func TestRejectedWorkerCertificatesDoNotSpendTheFailureBudget(t *testing.T) {
	// A worker whose certificate expired retries every heartbeat; that must
	// not lock out healthy workers on the same host or block re-enrollment.
	expiredCert := func(context.Context, any) (any, error) {
		return nil, grpcstatus.Error(codes.Unauthenticated, "worker certificate expired")
	}
	srv := newLimitedServer(RateLimits{AuthFailures: ratelimit.Policy{Rate: 0.1, Burst: 1}, now: (&grpcTestClock{}).Now})
	stale := limitedChain(srv, expiredCert)
	for range 20 {
		requireCode(t, stale(peerContext("203.0.113.1", limitTestWorkerToken), grpcpb.ControlPlane_Heartbeat_FullMethodName), codes.Unauthenticated)
	}
	enroll := limitedChain(srv, okHandler)
	requireCode(t, enroll(peerContext("203.0.113.1", limitTestWorkerToken), enrollWorkerMethod), codes.OK)
}

func TestWorkersWithVerifiedCertificatesBypassTheFailureBlock(t *testing.T) {
	// A misconfigured client behind the same NAT spends the failure budget;
	// enrolled workers on that address must keep heartbeating and fetching
	// task credentials, which do not retry ResourceExhausted.
	call := limitedChain(newLimitedServer(RateLimits{AuthFailures: ratelimit.Policy{Rate: 0.1, Burst: 1}, now: (&grpcTestClock{}).Now}), okHandler)
	heartbeat := grpcpb.ControlPlane_Heartbeat_FullMethodName

	requireCode(t, call(peerContext("203.0.113.1", "wrong"), heartbeat), codes.Unauthenticated)
	requireCode(t, call(peerContext("203.0.113.1", limitTestWorkerToken), heartbeat), codes.ResourceExhausted)
	requireCode(t, call(withVerifiedCertificate(peerContext("203.0.113.1", limitTestWorkerToken)), heartbeat), codes.OK)
}

func withVerifiedCertificate(ctx context.Context) context.Context {
	p, _ := peer.FromContext(ctx)
	verified := *p
	verified.AuthInfo = credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}}
	return peer.NewContext(ctx, &verified)
}

func TestEnrollmentRateLimitsEachPeer(t *testing.T) {
	var issued atomic.Int64
	issue := func(context.Context, any) (any, error) {
		issued.Add(1)
		return struct{}{}, nil
	}
	call := limitedChain(newLimitedServer(RateLimits{Enroll: ratelimit.Policy{Rate: 1, Burst: 3}, now: (&grpcTestClock{}).Now}), issue)

	for range 3 {
		requireCode(t, call(peerContext("203.0.113.1", limitTestWorkerToken), enrollWorkerMethod), codes.OK)
	}
	requireCode(t, call(peerContext("203.0.113.1", limitTestWorkerToken), enrollWorkerMethod), codes.ResourceExhausted)
	if issued.Load() != 3 {
		t.Fatalf("handler ran %d times, want 3: a limited enrollment must not reach certificate issuance", issued.Load())
	}
	requireCode(t, call(peerContext("203.0.113.2", limitTestWorkerToken), enrollWorkerMethod), codes.OK)
	// Other worker RPCs are not counted against enrollment.
	requireCode(t, call(peerContext("203.0.113.1", limitTestWorkerToken), grpcpb.ControlPlane_Heartbeat_FullMethodName), codes.OK)
}

func TestConcurrentEnrollmentBurstFromOnePeer(t *testing.T) {
	const burst = 50
	call := limitedChain(newLimitedServer(RateLimits{Enroll: ratelimit.Policy{Rate: 1, Burst: burst}, now: (&grpcTestClock{}).Now}), okHandler)

	var ok, exhausted atomic.Int64
	var wg sync.WaitGroup
	for range burst + 10 {
		wg.Go(func() {
			switch grpcstatus.Code(call(peerContext("203.0.113.1", limitTestWorkerToken), enrollWorkerMethod)) {
			case codes.OK:
				ok.Add(1)
			case codes.ResourceExhausted:
				exhausted.Add(1)
			}
		})
	}
	wg.Wait()
	if ok.Load() != burst || exhausted.Load() != 10 {
		t.Fatalf("ok=%d exhausted=%d, want %d and 10", ok.Load(), exhausted.Load(), burst)
	}
}

func TestServerWithoutRateLimitsIsUnlimited(t *testing.T) {
	call := limitedChain(NewServer(nil, nil, nil, testCryptoKey, time.Second, nil), okHandler)

	for range 50 {
		requireCode(t, call(peerContext("203.0.113.1", "wrong"), enrollWorkerMethod), codes.Unauthenticated)
	}
	requireCode(t, call(peerContext("203.0.113.1", limitTestWorkerToken), enrollWorkerMethod), codes.OK)
}

func TestEnrollmentOverTLSIsRateLimited(t *testing.T) {
	h := newIdentityHarness(t, func(s *Server) {
		s.SetRateLimits(RateLimits{Enroll: ratelimit.Policy{Rate: 0.01, Burst: 2}})
	})
	token := h.enrollmentToken("default", 10)

	h.enroll(token)
	h.enroll(token)
	_, err := h.client(nil).EnrollWorker(context.Background(), &grpcpb.EnrollWorkerRequest{EnrollmentToken: token, CsrPem: "unused"})
	requireCode(t, err, codes.ResourceExhausted)
}

func retryDelay(t *testing.T, err error) time.Duration {
	t.Helper()
	for _, d := range grpcstatus.Convert(err).Details() {
		if info, ok := d.(*errdetails.RetryInfo); ok {
			return info.GetRetryDelay().AsDuration()
		}
	}
	t.Fatalf("no RetryInfo in %v", err)
	return 0
}
