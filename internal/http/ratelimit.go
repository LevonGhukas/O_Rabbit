package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
	"github.com/LevonGhukas/O_Rabbit/internal/ratelimit"
	"github.com/LevonGhukas/O_Rabbit/internal/telemetry"
)

// Metric surface and reason names; fixed values keep label cardinality bounded.
const (
	surfaceHTTPAPI       = "http_api"
	surfaceHTTPRemoteOps = "http_remote_ops"

	reasonRequestRate   = "request_rate"
	reasonSensitiveRate = "sensitive_rate"
	reasonAuthFailures  = "auth_failures"
)

// RateLimits configures the HTTP API's per-client limits. A zero Policy
// disables that limit; the zero RateLimits limits nothing.
type RateLimits struct {
	// Requests bounds every request except the /healthz and /ready probes.
	Requests ratelimit.Policy
	// Sensitive additionally bounds changes to remote operations and worker
	// identities (enrollment-token creation, revocation).
	Sensitive ratelimit.Policy
	// AuthFailures is the budget of rejected API or remote-ops tokens. Once
	// it is spent the client is refused before its token is checked.
	AuthFailures ratelimit.Policy
	// TrustedProxies are the reverse proxies whose X-Forwarded-For is used to
	// find the client address. Without any, the connection's peer is the client.
	TrustedProxies []netip.Prefix

	now func() time.Time
}

type httpLimiters struct {
	clients      ratelimit.ClientResolver
	requests     *ratelimit.Limiter
	sensitive    *ratelimit.Limiter
	authFailures *ratelimit.Limiter
}

func (l httpLimiters) enabled() bool {
	return l.requests != nil || l.sensitive != nil || l.authFailures != nil
}

// SetRateLimits enables per-client request limits on the HTTP API.
func (s *Server) SetRateLimits(cfg RateLimits) {
	s.limits = httpLimiters{
		clients:      ratelimit.NewClientResolver(cfg.TrustedProxies),
		requests:     ratelimit.New(cfg.Requests, ratelimit.DefaultMaxKeys, cfg.now),
		sensitive:    ratelimit.New(cfg.Sensitive, ratelimit.DefaultMaxKeys, cfg.now),
		authFailures: ratelimit.New(cfg.AuthFailures, ratelimit.DefaultMaxKeys, cfg.now),
	}
}

type clientKeyContextKey struct{}

// withRateLimit runs before authentication, so floods and credential
// guessing are refused before any token is compared.
func (s *Server) withRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.limits.enabled() || isProbePath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		key := s.clientKey(r)
		surface := requestSurface(r)
		if exhausted, wait := s.limits.authFailures.Exhausted(key); exhausted {
			s.rejectRateLimited(w, r, key, surface, reasonAuthFailures, wait)
			return
		}
		if ok, wait := s.limits.requests.Allow(key); !ok {
			s.rejectRateLimited(w, r, key, surface, reasonRequestRate, wait)
			return
		}
		if isSensitiveRequest(r) {
			if ok, wait := s.limits.sensitive.Allow(key); !ok {
				s.rejectRateLimited(w, r, key, surface, reasonSensitiveRate, wait)
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientKeyContextKey{}, key)))
	})
}

// recordAuthFailure spends one unit of the client's failed-authentication
// budget after a rejected token.
func (s *Server) recordAuthFailure(r *http.Request, surface string) {
	telemetry.ObserveAuthFailure(surface)
	if s.limits.authFailures == nil {
		return
	}
	key, ok := r.Context().Value(clientKeyContextKey{}).(string)
	if !ok {
		key = s.clientKey(r)
	}
	s.limits.authFailures.Allow(key)
}

func (s *Server) clientKey(r *http.Request) string {
	return ratelimit.Key(s.limits.clients.ClientIP(r.RemoteAddr, r.Header.Values("X-Forwarded-For")))
}

func (s *Server) rejectRateLimited(w http.ResponseWriter, r *http.Request, key, surface, reason string, wait time.Duration) {
	telemetry.ObserveRateLimited(surface, reason)
	// Debug, not Warn: a flood must not turn into a flood of log lines. The
	// counter above is the operational signal.
	s.log.Debug("http request rate limited",
		slog.String("client", key),
		slog.String("surface", surface),
		slog.String("reason", reason),
		slog.String("method", r.Method),
	)
	seconds := int(wait / time.Second)
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	retryable := true
	httperr.WriteAPIError(w, http.StatusTooManyRequests, httperr.APIError{
		Code:      httperr.CodeRateLimited,
		Message:   "rate limit exceeded; retry later",
		Details:   map[string]any{"retry_after_seconds": seconds},
		Retryable: &retryable,
	})
}

func isProbePath(path string) bool {
	return path == "/healthz" || path == "/ready"
}

func requestSurface(r *http.Request) string {
	if isRemoteOpsPath(r.URL.Path) {
		return surfaceHTTPRemoteOps
	}
	return surfaceHTTPAPI
}

// isSensitiveRequest reports whether r changes remote operations or worker
// identities: it runs commands on other hosts or mints worker credentials.
func isSensitiveRequest(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	return isRemoteOpsPath(r.URL.Path) || strings.HasPrefix(unversionedPath(r.URL.Path), "/workers/")
}
