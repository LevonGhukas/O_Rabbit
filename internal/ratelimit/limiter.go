// Package ratelimit provides the master's in-memory, per-client token-bucket
// limits for the HTTP API and the worker gRPC control plane. The master runs
// as a single active leader, so the limits are local to the process.
package ratelimit

import (
	"errors"
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// DefaultMaxKeys bounds how many clients one limiter tracks, so memory stays
// bounded however many addresses send requests.
const DefaultMaxKeys = 10000

// minRetryAfter is the smallest retry hint returned to a limited client;
// Retry-After is expressed in whole seconds.
const minRetryAfter = time.Second

// Policy is a token bucket: Burst events at once, refilled at Rate events per
// second. A zero Rate disables the policy.
type Policy struct {
	Rate  float64
	Burst int
}

// Enabled reports whether the policy limits anything.
func (p Policy) Enabled() bool { return p.Rate > 0 && p.Burst > 0 }

// Validate rejects negative values and an enabled rate without a burst.
func (p Policy) Validate() error {
	if p.Rate < 0 || math.IsNaN(p.Rate) || math.IsInf(p.Rate, 0) {
		return errors.New("rate must be a finite number >= 0")
	}
	if p.Burst < 0 {
		return errors.New("burst must be >= 0")
	}
	if p.Rate > 0 && p.Burst < 1 {
		return errors.New("burst must be >= 1 when the rate is enabled")
	}
	return nil
}

// Limiter applies one Policy separately to each key. A nil *Limiter allows
// everything, so a disabled limit needs no special casing by callers.
type Limiter struct {
	policy  Policy
	maxKeys int
	now     func() time.Time

	mu      sync.Mutex
	buckets map[string]*rate.Limiter
}

// New returns a limiter for policy, or nil when the policy is disabled.
// maxKeys <= 0 uses DefaultMaxKeys; a nil now uses time.Now.
func New(policy Policy, maxKeys int, now func() time.Time) *Limiter {
	if !policy.Enabled() {
		return nil
	}
	if maxKeys <= 0 {
		maxKeys = DefaultMaxKeys
	}
	if now == nil {
		now = time.Now
	}
	return &Limiter{policy: policy, maxKeys: maxKeys, now: now, buckets: make(map[string]*rate.Limiter)}
}

// Allow consumes one event for key. When key has no budget left it returns
// false and how long the client should wait before retrying.
func (l *Limiter) Allow(key string) (bool, time.Duration) {
	if l == nil {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b := l.bucket(key, now)
	if tokens := b.TokensAt(now); tokens < 1 {
		return false, l.retryAfter(tokens)
	}
	b.AllowN(now, 1)
	return true, 0
}

// Exhausted reports, without consuming anything, whether key has no budget
// left and how long until it has.
func (l *Limiter) Exhausted(key string) (bool, time.Duration) {
	if l == nil {
		return false, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		return false, 0
	}
	if tokens := b.TokensAt(l.now()); tokens < 1 {
		return true, l.retryAfter(tokens)
	}
	return false, 0
}

// Len reports how many keys the limiter tracks.
func (l *Limiter) Len() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

func (l *Limiter) retryAfter(tokens float64) time.Duration {
	wait := time.Duration(math.Ceil((1-tokens)/l.policy.Rate)) * time.Second
	return max(wait, minRetryAfter)
}

// bucket returns key's bucket, creating it and making room when needed.
// l.mu must be held.
func (l *Limiter) bucket(key string, now time.Time) *rate.Limiter {
	if b, ok := l.buckets[key]; ok {
		return b
	}
	if len(l.buckets) >= l.maxKeys {
		l.evict(now)
	}
	b := rate.NewLimiter(rate.Limit(l.policy.Rate), l.policy.Burst)
	l.buckets[key] = b
	return b
}

// evict frees room for a new key. Full buckets go first: dropping one loses
// nothing because a new bucket starts full. Only when none is full does the
// fullest bucket go, so a drained client gains the least from eviction.
// l.mu must be held.
func (l *Limiter) evict(now time.Time) {
	full := float64(l.policy.Burst)
	fullestKey, fullestTokens := "", math.Inf(-1)
	for k, b := range l.buckets {
		tokens := b.TokensAt(now)
		if tokens >= full {
			delete(l.buckets, k)
			continue
		}
		if tokens > fullestTokens {
			fullestKey, fullestTokens = k, tokens
		}
	}
	if len(l.buckets) >= l.maxKeys {
		delete(l.buckets, fullestKey)
	}
}
