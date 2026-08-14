package icebergreg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os/exec"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
)

// ClassifyFailure is shared by both backends. Typed Go errors take precedence.
// CLI text is isolated here and deliberately recognizes only definite cases.
func ClassifyFailure(err error, externalStarted bool) failure.Failure {
	if err == nil {
		return failure.Failure{}
	}
	var f *failure.Failure
	if errors.As(err, &f) {
		out := *f
		if externalStarted && !out.DefiniteRejection && !out.Canceled {
			out.Class, out.Retryable = failure.FailureExternalAmbiguous, false
		}
		return out
	}
	if errors.Is(err, context.Canceled) {
		if externalStarted {
			return failure.Failure{Class: failure.FailureExternalAmbiguous, Err: err}
		}
		return failure.Failure{Class: failure.FailureCanceled, Canceled: true, DefiniteRejection: true, Err: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		if externalStarted {
			return failure.Failure{Class: failure.FailureExternalAmbiguous, Err: err}
		}
		return failure.Failure{Class: failure.FailureCatalogTimeout, Retryable: true, DefiniteRejection: true, Err: err}
	}
	var ne net.Error
	var ue *url.Error
	if errors.As(err, &ne) || errors.As(err, &ue) {
		if externalStarted {
			return failure.Failure{Class: failure.FailureExternalAmbiguous, Err: err}
		}
		return failure.Failure{Class: failure.FailureCatalogUnavailable, Retryable: true, DefiniteRejection: true, Err: err}
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		text := strings.ToLower(string(ee.Stderr))
		switch {
		case strings.Contains(text, "unauthenticated"), strings.Contains(text, "invalid token"):
			return failure.Failure{Class: failure.FailureAuthentication, DefiniteRejection: true, Err: err}
		case strings.Contains(text, "permission denied"), strings.Contains(text, "forbidden"):
			return failure.Failure{Class: failure.FailureAuthorization, DefiniteRejection: true, Err: err}
		case strings.Contains(text, "too many requests"), strings.Contains(text, "throttl"):
			return failure.Failure{Class: failure.FailureCatalogThrottled, Retryable: true, DefiniteRejection: true, Err: err}
		case strings.Contains(text, "conflict"):
			return failure.Failure{Class: failure.FailureCatalogConflict, DefiniteRejection: true, Err: err}
		}
	}
	if externalStarted {
		return failure.Failure{Class: failure.FailureUnknownAmbiguous, Err: err}
	}
	return failure.Failure{Class: failure.FailureUnknownPermanent, DefiniteRejection: true, Err: err}
}

func validateTableIdentifier(table string) error {
	parts := strings.Split(strings.TrimSpace(table), ".")
	if len(parts) < 2 {
		return failure.NewFailure(failure.FailureTableIdentifier, false, true, fmt.Errorf("invalid iceberg table %q (expected namespace.table)", table))
	}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return failure.NewFailure(failure.FailureTableIdentifier, false, true, fmt.Errorf("invalid iceberg table %q", table))
		}
	}
	return nil
}
