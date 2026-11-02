package ratelimit

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestLimiterAllowsBurstThenRejectsWithRetryAfter(t *testing.T) {
	clock := newFakeClock()
	l := New(Policy{Rate: 1, Burst: 3}, 100, clock.Now)

	for i := range 3 {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatalf("request %d rejected within burst", i)
		}
	}
	ok, wait := l.Allow("a")
	if ok {
		t.Fatal("request over burst allowed")
	}
	if wait != time.Second {
		t.Fatalf("retry after = %v, want 1s", wait)
	}
}

func TestLimiterBudgetRefillsAfterExpiry(t *testing.T) {
	clock := newFakeClock()
	l := New(Policy{Rate: 0.5, Burst: 1}, 100, clock.Now)

	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("first request rejected")
	}
	if ok, _ := l.Allow("a"); ok {
		t.Fatal("second request allowed before refill")
	}
	clock.Advance(1999 * time.Millisecond)
	if ok, _ := l.Allow("a"); ok {
		t.Fatal("request allowed before the refill interval elapsed")
	}
	clock.Advance(time.Millisecond)
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("request rejected after the refill interval elapsed")
	}
}

func TestLimiterKeysAreIndependent(t *testing.T) {
	l := New(Policy{Rate: 1, Burst: 1}, 100, newFakeClock().Now)

	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("a rejected")
	}
	if ok, _ := l.Allow("b"); !ok {
		t.Fatal("b rejected after a used its budget")
	}
}

func TestLimiterExhaustedDoesNotConsume(t *testing.T) {
	l := New(Policy{Rate: 1, Burst: 2}, 100, newFakeClock().Now)

	for range 5 {
		if exhausted, _ := l.Exhausted("a"); exhausted {
			t.Fatal("unused key reported exhausted")
		}
	}
	l.Allow("a")
	l.Allow("a")
	exhausted, wait := l.Exhausted("a")
	if !exhausted || wait != time.Second {
		t.Fatalf("Exhausted = %v, %v; want true, 1s", exhausted, wait)
	}
}

func TestLimiterRetryAfterIsAtLeastOneSecond(t *testing.T) {
	l := New(Policy{Rate: 100, Burst: 1}, 100, newFakeClock().Now)

	l.Allow("a")
	if _, wait := l.Allow("a"); wait != time.Second {
		t.Fatalf("retry after = %v, want the 1s minimum", wait)
	}
}

func TestLimiterConcurrentRequestsShareOneBudget(t *testing.T) {
	const burst = 25
	l := New(Policy{Rate: 1, Burst: burst}, 100, newFakeClock().Now)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Go(func() {
			if ok, _ := l.Allow("a"); ok {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()
	if got := allowed.Load(); got != burst {
		t.Fatalf("allowed %d concurrent requests, want exactly %d", got, burst)
	}
}

func TestNilLimiterAllowsEverything(t *testing.T) {
	var l *Limiter
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("nil limiter rejected a request")
	}
	if exhausted, _ := l.Exhausted("a"); exhausted {
		t.Fatal("nil limiter reported exhaustion")
	}
}

func TestNewReturnsNilForDisabledPolicy(t *testing.T) {
	if New(Policy{}, 100, nil) != nil {
		t.Fatal("zero policy produced a limiter")
	}
	if New(Policy{Rate: 1, Burst: 0}, 100, nil) != nil {
		t.Fatal("zero burst produced a limiter")
	}
}

func TestLimiterEvictsFullBucketsBeforeDrainedOnes(t *testing.T) {
	clock := newFakeClock()
	l := New(Policy{Rate: 1, Burst: 2}, 3, clock.Now)

	// The attacker's bucket is drained; the others are full again by the time
	// the cap is reached.
	l.Allow("attacker")
	l.Allow("attacker")
	l.Allow("idle-1")
	l.Allow("idle-2")
	clock.Advance(time.Second)
	l.Allow("attacker")

	l.Allow("new")
	if l.Len() > 3 {
		t.Fatalf("tracked %d keys, want at most 3", l.Len())
	}
	if ok, _ := l.Allow("attacker"); ok {
		t.Fatal("drained bucket was evicted and its budget reset")
	}
}

func TestLimiterStaysWithinKeyCap(t *testing.T) {
	l := New(Policy{Rate: 1, Burst: 1}, 10, newFakeClock().Now)

	for i := range 100 {
		l.Allow("k" + strconv.Itoa(i))
	}
	if l.Len() > 10 {
		t.Fatalf("tracked %d keys, want at most 10", l.Len())
	}
}

func TestPolicyValidate(t *testing.T) {
	cases := []struct {
		name    string
		policy  Policy
		wantErr bool
	}{
		{"disabled", Policy{}, false},
		{"valid", Policy{Rate: 0.5, Burst: 10}, false},
		{"negative rate", Policy{Rate: -1, Burst: 10}, true},
		{"rate without burst", Policy{Rate: 1, Burst: 0}, true},
		{"negative burst", Policy{Rate: 0, Burst: -1}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.policy.Validate(); (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
