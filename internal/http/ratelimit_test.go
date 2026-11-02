package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/ratelimit"
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestClock() *testClock {
	return &testClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

type limitedRequest struct {
	method, path, token, remoteAddr string
	forwardedFor                    string
}

func serveLimited(h http.Handler, lr limitedRequest) *httptest.ResponseRecorder {
	method := lr.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, lr.path, nil)
	if lr.token != "" {
		req.Header.Set("Authorization", "Bearer "+lr.token)
	}
	if lr.remoteAddr != "" {
		req.RemoteAddr = lr.remoteAddr
	}
	if lr.forwardedFor != "" {
		req.Header.Set("X-Forwarded-For", lr.forwardedFor)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func assertRateLimited(t *testing.T, rec *httptest.ResponseRecorder, wantRetryAfter string) {
	t.Helper()
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got != wantRetryAfter {
		t.Fatalf("Retry-After=%q, want %q", got, wantRetryAfter)
	}
	var body struct {
		Error struct {
			Code      string `json:"code"`
			Retryable *bool  `json:"retryable"`
			Details   struct {
				RetryAfterSeconds int `json:"retry_after_seconds"`
			} `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode 429 body: %v", err)
	}
	if body.Error.Code != "rate_limited" {
		t.Fatalf("error code=%q, want rate_limited", body.Error.Code)
	}
	if body.Error.Retryable == nil || !*body.Error.Retryable {
		t.Fatal("429 is not marked retryable")
	}
	if got := strconv.Itoa(body.Error.Details.RetryAfterSeconds); got != wantRetryAfter {
		t.Fatalf("retry_after_seconds=%s, want %s", got, wantRetryAfter)
	}
}

func TestRepeatedInvalidAPITokensAreThrottled(t *testing.T) {
	clock := newTestClock()
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRateLimits(RateLimits{AuthFailures: ratelimit.Policy{Rate: 0.1, Burst: 3}, now: clock.Now})
	h := srv.Handler()

	for i := range 3 {
		if rec := serveLimited(h, limitedRequest{path: "/status", token: "guess"}); rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d status=%d, want 401", i, rec.Code)
		}
	}
	assertRateLimited(t, serveLimited(h, limitedRequest{path: "/status", token: "guess"}), "10")
	// The block holds even for the valid token, so guesses reveal nothing.
	assertRateLimited(t, serveLimited(h, limitedRequest{path: "/status", token: "api-token"}), "10")

	clock.Advance(10 * time.Second)
	if rec := serveLimited(h, limitedRequest{path: "/status", token: "api-token"}); rec.Code != http.StatusOK {
		t.Fatalf("after expiry status=%d, want 200", rec.Code)
	}
}

func TestInvalidTokenBlockIsPerClient(t *testing.T) {
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRateLimits(RateLimits{AuthFailures: ratelimit.Policy{Rate: 0.1, Burst: 1}, now: newTestClock().Now})
	h := srv.Handler()

	serveLimited(h, limitedRequest{path: "/status", token: "guess", remoteAddr: "203.0.113.1:1"})
	assertRateLimited(t, serveLimited(h, limitedRequest{path: "/status", token: "guess", remoteAddr: "203.0.113.1:1"}), "10")
	if rec := serveLimited(h, limitedRequest{path: "/status", token: "api-token", remoteAddr: "203.0.113.2:1"}); rec.Code != http.StatusOK {
		t.Fatalf("other client status=%d, want 200", rec.Code)
	}
}

func TestRepeatedInvalidRemoteOpsTokensAreThrottled(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRemoteOpsToken("ops-token")
	srv.SetRateLimits(RateLimits{AuthFailures: ratelimit.Policy{Rate: 0.5, Burst: 2}, now: newTestClock().Now})
	h := srv.Handler()

	for i := range 2 {
		if rec := serveLimited(h, limitedRequest{path: "/servers", token: "api-token"}); rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d status=%d, want 401", i, rec.Code)
		}
	}
	assertRateLimited(t, serveLimited(h, limitedRequest{path: "/servers", token: "ops-token"}), "2")
}

func TestRequestRateLimitsEachClient(t *testing.T) {
	clock := newTestClock()
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRateLimits(RateLimits{Requests: ratelimit.Policy{Rate: 1, Burst: 5}, now: clock.Now})
	h := srv.Handler()

	for i := range 5 {
		if rec := serveLimited(h, limitedRequest{path: "/status", token: "api-token"}); rec.Code != http.StatusOK {
			t.Fatalf("request %d status=%d, want 200", i, rec.Code)
		}
	}
	assertRateLimited(t, serveLimited(h, limitedRequest{path: "/status", token: "api-token"}), "1")
	if rec := serveLimited(h, limitedRequest{path: "/status", token: "api-token", remoteAddr: "203.0.113.9:1"}); rec.Code != http.StatusOK {
		t.Fatalf("other client status=%d, want 200", rec.Code)
	}
	clock.Advance(time.Second)
	if rec := serveLimited(h, limitedRequest{path: "/status", token: "api-token"}); rec.Code != http.StatusOK {
		t.Fatalf("after refill status=%d, want 200", rec.Code)
	}
}

func TestProbesAreNotRateLimited(t *testing.T) {
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRateLimits(RateLimits{Requests: ratelimit.Policy{Rate: 1, Burst: 1}, now: newTestClock().Now})
	h := srv.Handler()

	for i := range 5 {
		if rec := serveLimited(h, limitedRequest{path: "/healthz"}); rec.Code != http.StatusOK {
			t.Fatalf("probe %d status=%d, want 200", i, rec.Code)
		}
	}
}

func TestConcurrentRequestsAllowExactlyTheBurst(t *testing.T) {
	const burst = 20
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRateLimits(RateLimits{Requests: ratelimit.Policy{Rate: 1, Burst: burst}, now: newTestClock().Now})
	h := srv.Handler()

	var ok, limited atomic.Int64
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			switch serveLimited(h, limitedRequest{path: "/status", token: "api-token"}).Code {
			case http.StatusOK:
				ok.Add(1)
			case http.StatusTooManyRequests:
				limited.Add(1)
			}
		})
	}
	wg.Wait()
	if ok.Load() != burst || limited.Load() != 100-burst {
		t.Fatalf("ok=%d limited=%d, want %d and %d", ok.Load(), limited.Load(), burst, 100-burst)
	}
}

func TestTrustedProxyForwardedForSeparatesClients(t *testing.T) {
	proxies, err := ratelimit.ParseTrustedProxies("10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRateLimits(RateLimits{Requests: ratelimit.Policy{Rate: 1, Burst: 1}, TrustedProxies: proxies, now: newTestClock().Now})
	h := srv.Handler()

	viaProxy := func(client string) int {
		return serveLimited(h, limitedRequest{path: "/status", token: "api-token", remoteAddr: "10.0.0.2:1", forwardedFor: client}).Code
	}
	if code := viaProxy("198.51.100.1"); code != http.StatusOK {
		t.Fatalf("client 1 status=%d, want 200", code)
	}
	if code := viaProxy("198.51.100.2"); code != http.StatusOK {
		t.Fatalf("client 2 behind the same proxy status=%d, want 200", code)
	}
	if code := viaProxy("198.51.100.1"); code != http.StatusTooManyRequests {
		t.Fatalf("client 1 again status=%d, want 429", code)
	}
}

func TestUntrustedPeerCannotSpoofForwardedFor(t *testing.T) {
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRateLimits(RateLimits{Requests: ratelimit.Policy{Rate: 1, Burst: 1}, now: newTestClock().Now})
	h := srv.Handler()

	spoof := func(claimed string) int {
		return serveLimited(h, limitedRequest{path: "/status", token: "api-token", remoteAddr: "203.0.113.5:1", forwardedFor: claimed}).Code
	}
	if code := spoof("198.51.100.1"); code != http.StatusOK {
		t.Fatalf("first request status=%d, want 200", code)
	}
	if code := spoof("198.51.100.2"); code != http.StatusTooManyRequests {
		t.Fatalf("spoofed X-Forwarded-For status=%d, want 429", code)
	}
}

func TestSensitiveRoutesHaveTheirOwnLimit(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRateLimits(RateLimits{Sensitive: ratelimit.Policy{Rate: 0.5, Burst: 1}, now: newTestClock().Now})
	h := srv.Handler()

	create := limitedRequest{method: http.MethodPost, path: "/api/v1/workers/enrollment-tokens", token: "api-token"}
	if rec := serveLimited(h, create); rec.Code != http.StatusCreated {
		t.Fatalf("first enrollment token status=%d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	assertRateLimited(t, serveLimited(h, create), "2")
	// Reads are not sensitive.
	if rec := serveLimited(h, limitedRequest{path: "/api/v1/workers/identities", token: "api-token"}); rec.Code != http.StatusOK {
		t.Fatalf("identity list status=%d, want 200", rec.Code)
	}
}

func TestIsSensitiveRequest(t *testing.T) {
	cases := []struct {
		method, path string
		want         bool
	}{
		{http.MethodPost, "/workers/enrollment-tokens", true},
		{http.MethodPost, "/api/v1/workers/identities/w1/revoke", true},
		{http.MethodPost, "/api/v1/servers/s1/ssh/test", true},
		{http.MethodDelete, "/deployments/d1", true},
		{http.MethodPost, "/executions", true},
		{http.MethodGet, "/servers", false},
		{http.MethodGet, "/workers/identities", false},
		{http.MethodPost, "/runs", false},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		if got := isSensitiveRequest(req); got != tc.want {
			t.Errorf("%s %s sensitive=%v, want %v", tc.method, tc.path, got, tc.want)
		}
	}
}

func TestServerWithoutRateLimitsIsUnlimited(t *testing.T) {
	h := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "api-token").Handler()

	for i := range 50 {
		if rec := serveLimited(h, limitedRequest{path: "/status", token: "guess"}); rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d status=%d, want 401 without limits", i, rec.Code)
		}
	}
}
