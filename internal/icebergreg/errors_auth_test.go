package icebergreg

import (
	"fmt"
	"testing"

	restcatalog "github.com/apache/iceberg-go/catalog/rest"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
)

// A REST catalog that refuses the token (401/403/419) rejected the request,
// so the failure is definite even after the external commit started. It
// must not go to reconciliation, which would hit the same refusal forever.
func TestClassifyFailureRESTAuthIsDefiniteRejection(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want failure.FailureClass
	}{
		{restcatalog.ErrUnauthorized, failure.FailureAuthentication},
		{restcatalog.ErrAuthorizationExpired, failure.FailureAuthentication},
		{restcatalog.ErrForbidden, failure.FailureAuthorization},
	} {
		// The way restgo surfaces it: wrapped by our own context.
		err := fmt.Errorf("commit iceberg table: %w", fmt.Errorf("%w: NotAuthorizedException: Unauthorized", tc.err))
		for _, started := range []bool{false, true} {
			got := ClassifyFailure(err, started)
			if got.Class != tc.want || !got.DefiniteRejection || got.Retryable {
				t.Errorf("%v (external started=%t) = %+v, want definite %s", tc.err, started, got, tc.want)
			}
		}
	}
	// Other REST errors after the commit started stay ambiguous.
	if got := ClassifyFailure(fmt.Errorf("x: %w", restcatalog.ErrServerError), true); got.DefiniteRejection {
		t.Errorf("a 5xx after the commit started must stay ambiguous, got %+v", got)
	}
}
