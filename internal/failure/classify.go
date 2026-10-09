package failure

import (
	"context"
	"errors"
	"strings"
)

// classificationRule maps lower-cased error-message fragments to a class.
// Rules are evaluated in order; the first matching rule wins.
type classificationRule struct {
	class     FailureClass
	retryable bool
	fragments []string
}

var classificationRules = []classificationRule{
	{FailureTimeout, true, []string{
		"timeout", "timed out", "deadline exceeded",
	}},
	{FailureAuthentication, false, []string{
		"password authentication failed", "authentication failed", "access denied for user",
		"login failed", "invalid username/password", "ora-01017", "invalid credentials",
		"invalidaccesskeyid", "signaturedoesnotmatch",
	}},
	{FailureAuthorization, false, []string{
		"permission denied", "insufficient privileges", "not authorized", "ora-01031",
		"accessdenied", "access denied",
	}},
	{FailureNetworkConnection, true, []string{
		"connection refused", "no route to host", "network is unreachable", "no such host",
		"connection reset", "broken pipe", "dial tcp", "dial error", "server selection error",
		"no hosts available", "could not connect", "unable to connect",
	}},
	{FailureQuerySyntax, false, []string{
		"syntax error", "error in your sql syntax", "incorrect syntax", "ora-00933", "ora-00936",
		"mismatched input", "no viable alternative",
	}},
	{FailureTableIdentifier, false, []string{
		"table or view does not exist", "ora-00942", "invalid object name", "unknown table",
		"no such table", "unconfigured table", "doesn't exist", "does not exist",
		"nosuchbucket", "nosuchkey",
	}},
	{FailureDataIntegrity, false, []string{
		"cannot convert", "conversion failed", "out of range", "overflow", "invalid input syntax",
		"division by zero", "divide by zero", "nullable cursor", "is nullable",
		"cannot be represented", "invalid time", "unsupported decimal",
	}},
}

// Classify returns err as a *Failure. Errors that already carry a Failure
// keep it; others are classified from context sentinels and well-known
// driver/storage message fragments. Unrecognized errors are
// FailureUnknownPermanent: retrying an unknown data-pipeline error is not
// assumed safe. Classify returns nil for a nil error.
func Classify(err error) *Failure {
	if err == nil {
		return nil
	}
	var f *Failure
	if errors.As(err, &f) {
		return f
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Failure{Class: FailureTimeout, Retryable: true, DefiniteRejection: true, Err: err}
	}
	msg := strings.ToLower(err.Error())
	for _, rule := range classificationRules {
		for _, fragment := range rule.fragments {
			if strings.Contains(msg, fragment) {
				return &Failure{Class: rule.class, Retryable: rule.retryable, DefiniteRejection: true, Err: err}
			}
		}
	}
	return &Failure{Class: FailureUnknownPermanent, DefiniteRejection: true, Err: err}
}

// ClassOf returns the failure class of err ("" for a nil error).
func ClassOf(err error) FailureClass {
	if f := Classify(err); f != nil {
		return f.Class
	}
	return ""
}
