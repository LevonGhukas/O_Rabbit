package failure

import "errors"

type FailureClass string

const (
	// Core & Infrastructure
	FailureConfigurationUnavailable FailureClass = "CONFIGURATION_UNAVAILABLE"
	FailureAuthentication           FailureClass = "AUTHENTICATION_FAILED"
	FailureAuthorization            FailureClass = "AUTHORIZATION_FAILED"
	FailureNetworkConnection        FailureClass = "NETWORK_CONNECTION_FAILED"
	FailureRateLimited              FailureClass = "RATE_LIMITED"
	FailureTimeout                  FailureClass = "TIMEOUT"
	FailureCanceled                 FailureClass = "REGISTRATION_CANCELED"
	FailureRetryExhausted           FailureClass = "RETRY_LIMIT_EXHAUSTED"

	// Database & Connectors
	FailureDatabaseOffline FailureClass = "DATABASE_OFFLINE"
	FailureQuerySyntax     FailureClass = "QUERY_SYNTAX_ERROR"
	FailureDataIntegrity   FailureClass = "DATA_INTEGRITY_ERROR"
	FailureTableIdentifier FailureClass = "TABLE_IDENTIFIER_INVALID"

	// Iceberg Catalog
	FailureCatalogUnavailable FailureClass = "CATALOG_UNAVAILABLE"
	FailureCatalogThrottled   FailureClass = "CATALOG_THROTTLED"
	FailureCatalogTimeout     FailureClass = "CATALOG_TIMEOUT"
	FailureCatalogConflict    FailureClass = "CATALOG_CONFLICT"
	FailureSchemaIncompatible FailureClass = "SCHEMA_INCOMPATIBLE"
	FailureExternalAmbiguous  FailureClass = "EXTERNAL_COMMIT_AMBIGUOUS"
	FailureIceStateWrite      FailureClass = "ICE_STATE_WRITE_FAILED"
	FailureIceStateVerify     FailureClass = "ICE_STATE_VERIFY_FAILED"

	// Execution & Verification
	FailureArtifactVerification FailureClass = "ARTIFACT_VERIFICATION_FAILED"
	FailureSerialization        FailureClass = "SERIALIZATION_FAILED"
	FailureFileSystemError      FailureClass = "FILESYSTEM_ERROR"

	// Unknowns
	FailureUnknownPermanent FailureClass = "UNKNOWN_PERMANENT"
	FailureUnknownAmbiguous FailureClass = "UNKNOWN_AMBIGUOUS"
)

type Failure struct {
	Class             FailureClass
	Retryable         bool
	DefiniteRejection bool
	Canceled          bool
	Err               error
}

func (e *Failure) Error() string {
	if e.Err == nil {
		return string(e.Class)
	}
	return e.Err.Error()
}

func (e *Failure) Unwrap() error {
	return e.Err
}

func NewFailure(class FailureClass, retryable, definite bool, err error) error {
	return &Failure{
		Class:             class,
		Retryable:         retryable,
		DefiniteRejection: definite,
		Canceled:          class == FailureCanceled,
		Err:               err,
	}
}

// IsFailure checks if the error is a Failure and matches the given class.
func IsFailure(err error, class FailureClass) bool {
	var f *Failure
	if errors.As(err, &f) {
		return f.Class == class
	}
	return false
}
