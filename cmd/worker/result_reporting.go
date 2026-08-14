package main

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func isBusyRPC(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "SQLITE_BUSY") || strings.Contains(s, "database is locked")
}

func isTransientRPCError(err error) bool {
	if isBusyRPC(err) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal:
		return true
	default:
		return false
	}
}

type transientSuccessReportError struct{ err error }

func (e *transientSuccessReportError) Error() string {
	return "transient success-result reporting failure: " + e.err.Error()
}
func (e *transientSuccessReportError) Unwrap() error { return e.err }

func reportResultWithRetry(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, req *grpcpb.ReportTaskResultRequest) error {
	return reportResultWithRetryWindow(ctx, log, cp, req, 15*time.Second, 5*time.Second)
}

func reportResultWithRetryWindow(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, req *grpcpb.ReportTaskResultRequest, maxRetryWindow, maxAttemptDuration time.Duration) error {
	backoff := 200 * time.Millisecond
	deadline := time.Now().Add(maxRetryWindow)
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if lastErr != nil {
				if strings.EqualFold(req.Status, "SUCCEEDED") && isTransientRPCError(lastErr) {
					return &transientSuccessReportError{err: lastErr}
				}
				return lastErr
			}
			return context.DeadlineExceeded
		}
		attemptTimeout := maxAttemptDuration
		if remaining < attemptTimeout {
			attemptTimeout = remaining
		}

		callCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		resp, err := cp.ReportTaskResult(callCtx, req)
		cancel()
		if err == nil {
			_ = resp
			return nil
		}
		lastErr = err
		if !isTransientRPCError(err) {
			return err
		}
		log.Warn("report result failed, retrying", slog.String("err", err.Error()), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
		}
	}
}
