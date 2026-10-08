package failure

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		err       error
		class     FailureClass
		retryable bool
	}{
		// Real error texts observed from O_Rabbit runs.
		{errors.New(`failed to connect to user=orabbit database=postgres: 169.58.40.227:5454: dial error: dial tcp: connect: connection refused`), FailureNetworkConnection, true},
		{errors.New(`pq: password authentication failed for user "etl"`), FailureAuthentication, false},
		{errors.New(`ORA-01017: invalid username/password; logon denied`), FailureAuthentication, false},
		{errors.New(`ORA-00942: table or view does not exist`), FailureTableIdentifier, false},
		{errors.New(`ERROR: relation "public.orders" does not exist (SQLSTATE 42P01)`), FailureTableIdentifier, false},
		{errors.New(`ERROR: syntax error at or near "FORM" (SQLSTATE 42601)`), FailureQuerySyntax, false},
		{errors.New(`read to arrow/parquet: ERROR: division by zero (SQLSTATE 22012)`), FailureDataIntegrity, false},
		{errors.New(`column "created_time": cannot convert int64 to time`), FailureDataIntegrity, false},
		{errors.New(`cursor column "account_balance" is nullable; nullable cursor columns can skip rows`), FailureDataIntegrity, false},
		{fmt.Errorf("query: %w", context.DeadlineExceeded), FailureTimeout, true},
		{errors.New(`gocql: not enough columns to scan into: have 0 want 26`), FailureUnknownPermanent, false},
	}
	for _, tc := range cases {
		f := Classify(tc.err)
		if f.Class != tc.class || f.Retryable != tc.retryable {
			t.Errorf("Classify(%q) = %s retryable=%v, want %s retryable=%v", tc.err, f.Class, f.Retryable, tc.class, tc.retryable)
		}
		if !errors.Is(f, tc.err) {
			t.Errorf("Classify(%q) does not wrap the original error", tc.err)
		}
	}
}

func TestClassifyKeepsExistingFailure(t *testing.T) {
	existing := NewFailure(FailureCatalogConflict, true, false, errors.New("commit conflict"))
	if got := Classify(fmt.Errorf("register: %w", existing)); got.Class != FailureCatalogConflict {
		t.Fatalf("class = %s, want %s", got.Class, FailureCatalogConflict)
	}
	if Classify(nil) != nil || ClassOf(nil) != "" {
		t.Fatal("nil error must classify to nil")
	}
}
