package main

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func rateLimitedErr(t *testing.T, delay time.Duration) error {
	t.Helper()
	st, err := status.New(codes.ResourceExhausted, "rate limit exceeded").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

func TestEnrollRetryWait(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		backoff   time.Duration
		wantRetry bool
		wantWait  time.Duration
	}{
		{"unavailable retries with backoff", status.Error(codes.Unavailable, "down"), 2 * time.Second, true, 2 * time.Second},
		{"deadline retries with backoff", status.Error(codes.DeadlineExceeded, "slow"), time.Second, true, time.Second},
		{"rate limited waits the server hint", rateLimitedErr(t, 7*time.Second), time.Second, true, 7 * time.Second},
		{"rate limited keeps a longer backoff", rateLimitedErr(t, time.Second), 4 * time.Second, true, 4 * time.Second},
		{"rate limited without hint uses backoff", status.Error(codes.ResourceExhausted, "busy"), 2 * time.Second, true, 2 * time.Second},
		{"rate limit hint is capped", rateLimitedErr(t, time.Hour), time.Second, true, maxEnrollBackoff},
		{"invalid token is final", status.Error(codes.Unauthenticated, "bad token"), time.Second, false, 0},
		{"non-status error is final", errors.New("boom"), time.Second, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wait, retry := enrollRetryWait(tc.err, tc.backoff)
			if retry != tc.wantRetry || wait != tc.wantWait {
				t.Fatalf("enrollRetryWait = %v, %v; want %v, %v", wait, retry, tc.wantWait, tc.wantRetry)
			}
		})
	}
}
