package failure

import (
	"errors"
	"fmt"
	"testing"
)

func TestNewFailurePreservesClassificationAndCauseThroughWrapping(t *testing.T) {
	cause := errors.New("catalog unavailable")
	err := fmt.Errorf("registration failed: %w", NewFailure(FailureCatalogUnavailable, true, false, cause))

	if !IsFailure(err, FailureCatalogUnavailable) {
		t.Fatalf("wrapped failure was not classified: %v", err)
	}
	if IsFailure(err, FailureAuthorization) {
		t.Fatalf("failure matched unrelated class: %v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("wrapped cause was lost: %v", err)
	}

	var got *Failure
	if !errors.As(err, &got) {
		t.Fatalf("failure details were not discoverable: %v", err)
	}
	if !got.Retryable || got.DefiniteRejection || got.Canceled {
		t.Fatalf("failure metadata=%+v", got)
	}
}

func TestNewFailureMarksOnlyCanceledClassAsCanceled(t *testing.T) {
	canceled := NewFailure(FailureCanceled, false, true, nil)
	var got *Failure
	if !errors.As(canceled, &got) {
		t.Fatalf("canceled error has no failure details: %v", canceled)
	}
	if got.Error() != string(FailureCanceled) || !got.Canceled || !got.DefiniteRejection {
		t.Fatalf("canceled failure metadata=%+v", got)
	}

	nonCanceled := NewFailure(FailureTimeout, true, false, nil)
	if errors.As(nonCanceled, &got) && got.Canceled {
		t.Fatalf("non-canceled failure was marked canceled: %+v", got)
	}
}

func TestIsFailureRejectsPlainAndNilErrors(t *testing.T) {
	if IsFailure(nil, FailureUnknownPermanent) {
		t.Fatal("nil error matched failure class")
	}
	if IsFailure(errors.New("plain error"), FailureUnknownPermanent) {
		t.Fatal("plain error matched failure class")
	}
}
