package grpcapi

import (
	"context"
	"errors"
	"time"
)

// Catalog operation names used for metrics and logs.
const (
	catalogOperationRegistration   = "registration"
	catalogOperationReconciliation = "reconciliation"
)

// reconciliationTimeoutClass marks a catalog observation stopped by its own
// deadline. The observation is retried; partial evidence is never used.
const reconciliationTimeoutClass = "CATALOG_OBSERVATION_TIMEOUT"

// errCatalogAttemptDeadline is the cause of a catalog attempt context that hit
// its own deadline. It tells that deadline apart from an outer deadline or a
// leadership cancel.
var errCatalogAttemptDeadline = errors.New("catalog attempt deadline exceeded")

// catalogAttemptContext bounds one registration or reconciliation attempt.
func catalogAttemptContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, timeout, errCatalogAttemptDeadline)
}

// catalogAttemptTimedOut reports whether attemptCtx ended because of its own
// deadline rather than its parent's.
func catalogAttemptTimedOut(attemptCtx context.Context) bool {
	return errors.Is(context.Cause(attemptCtx), errCatalogAttemptDeadline)
}
