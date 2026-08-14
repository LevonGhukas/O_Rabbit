package artifact

import (
	"errors"
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
)

const (
	FailureLocalParquetInvalid        failure.FailureClass = "LOCAL_PARQUET_INVALID"
	FailureLocalSizeMismatch          failure.FailureClass = "LOCAL_SIZE_MISMATCH"
	FailureLocalRowCountMismatch      failure.FailureClass = "LOCAL_ROW_COUNT_MISMATCH"
	FailureLocalSchemaMismatch        failure.FailureClass = "LOCAL_SCHEMA_MISMATCH"
	FailureUploadFailed               failure.FailureClass = "UPLOAD_FAILED"
	FailureMultipartPartFailed        failure.FailureClass = "MULTIPART_PART_FAILED"
	FailureMultipartCompleteAmbiguous failure.FailureClass = "MULTIPART_COMPLETE_AMBIGUOUS"
	FailureMultipartIntentFailed      failure.FailureClass = "MULTIPART_INTENT_FAILED"
	FailureMultipartTrackingFailed    failure.FailureClass = "MULTIPART_TRACKING_FAILED"
	FailureRemoteSizeMismatch         failure.FailureClass = "REMOTE_SIZE_MISMATCH"
	FailureRemoteChecksumMismatch     failure.FailureClass = "REMOTE_CHECKSUM_MISMATCH"
	FailureRemoteMetadataMismatch     failure.FailureClass = "REMOTE_METADATA_MISMATCH"
	FailureExistingObjectConflict     failure.FailureClass = "EXISTING_OBJECT_CONFLICT"
	FailureVerificationUnavailable    failure.FailureClass = "VERIFICATION_UNAVAILABLE"
	FailureVerificationCanceled       failure.FailureClass = "VERIFICATION_CANCELED"
)

type Failure struct {
	Classification     failure.FailureClass
	Retryable          bool
	Ambiguous          bool
	ReconciliationOK   bool
	VerificationMethod string
	ObjectKey          string
	FileIndex          int
	Err                error
}

func (e *Failure) Error() string {
	if e.Err == nil {
		return string(e.Classification)
	}
	return fmt.Sprintf("%s: %v", string(e.Classification), e.Err)
}
func (e *Failure) Unwrap() error { return e.Err }
func AsFailure(err error) (*Failure, bool) {
	var f *Failure
	if !errors.As(err, &f) {
		return nil, false
	}
	return f, true
}

func ClassificationFromMessage(message string) failure.FailureClass {
	for _, classification := range []failure.FailureClass{FailureLocalParquetInvalid, FailureLocalSizeMismatch, FailureLocalRowCountMismatch, FailureLocalSchemaMismatch, FailureUploadFailed, FailureMultipartPartFailed, FailureMultipartCompleteAmbiguous, FailureMultipartIntentFailed, FailureMultipartTrackingFailed, FailureRemoteSizeMismatch, FailureRemoteChecksumMismatch, FailureRemoteMetadataMismatch, FailureExistingObjectConflict, FailureVerificationUnavailable, FailureVerificationCanceled} {
		strClass := string(classification)
		if message == strClass || strings.HasPrefix(message, strClass+":") || strings.Contains(message, ": "+strClass+":") {
			return classification
		}
	}
	return ""
}
