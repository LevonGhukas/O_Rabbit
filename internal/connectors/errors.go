package connectors

import (
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
)

// ClassifyConnectorError maps driver-specific errors (like pq or pgx errors) to a typed failure.Failure
func ClassifyConnectorError(err error) error {
	if err == nil {
		return nil
	}

	// Check if it's already a failure
	if _, ok := err.(*failure.Failure); ok {
		return err
	}

	msg := strings.ToLower(err.Error())

	var class failure.FailureClass
	var retryable bool
	var definite bool

	switch {
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "deadline exceeded"):
		class = failure.FailureTimeout
		retryable = true
		definite = true
	case strings.Contains(msg, "connection refused"), strings.Contains(msg, "no route to host"), strings.Contains(msg, "network is unreachable"):
		class = failure.FailureNetworkConnection
		retryable = true
		definite = true
	case strings.Contains(msg, "authentication failed"), strings.Contains(msg, "password authentication failed"), strings.Contains(msg, "access denied"):
		class = failure.FailureAuthentication
		retryable = false
		definite = true
	case strings.Contains(msg, "permission denied"), strings.Contains(msg, "insufficient privileges"):
		class = failure.FailureAuthorization
		retryable = false
		definite = true
	case strings.Contains(msg, "syntax error"), strings.Contains(msg, "you have an error in your sql syntax"):
		class = failure.FailureQuerySyntax
		retryable = false
		definite = true
	case strings.Contains(msg, "relation"), strings.Contains(msg, "does not exist"), strings.Contains(msg, "table or view does not exist"):
		class = failure.FailureTableIdentifier
		retryable = false
		definite = true
	default:
		// We default to UNKNOWN_PERMANENT if we don't recognize the error
		// to be conservative with retries in data pipelines.
		class = failure.FailureUnknownPermanent
		retryable = false
		definite = true
	}

	return failure.NewFailure(class, retryable, definite, err)
}
