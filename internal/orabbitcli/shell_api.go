package orabbitcli

import (
	"context"
	"net/http"
)

// This file is the API the interactive shell (package
// internal/orabbitcli/tui) uses. Gathering it here keeps the rest of the
// package unexported and shows in one place what the shell depends on.

// APIPrefix is the master's versioned API path prefix, e.g. "/api/v1".
const APIPrefix = apiV1

// DefaultMasterHTTP is the master URL used when none is configured.
func DefaultMasterHTTP() string { return localHTTPBase(defaultHTTPAddr) }

// NormalizeHTTPBase turns "host:port" or a URL into a base URL without a
// trailing slash.
func NormalizeHTTPBase(raw string) string { return normalizeHTTPBase(raw) }

// GetJSON sends a GET request and decodes the JSON response into out.
// Non-2xx responses return an error of the form "http <code>: <body>".
func GetJSON(ctx context.Context, url string, out any) error {
	return httpJSON(ctx, http.MethodGet, url, nil, out)
}

// CheckHealth reports whether the master at base answers its health check.
func CheckHealth(ctx context.Context, base string) bool { return checkHealth(ctx, base) }

// ActiveWorkerCount returns how many workers heartbeated recently.
func ActiveWorkerCount(ctx context.Context, base string) (int, error) {
	ws, err := listActiveWorkers(ctx, base)
	return len(ws), err
}

// GetRunDetails returns a run and its tasks.
func GetRunDetails(ctx context.Context, base, runID string) (RunDetails, error) {
	return getRunDetails(ctx, base, runID)
}

// CancelRun cancels a run.
func CancelRun(ctx context.Context, base, runID string) (CancelRunResponse, error) {
	return cancelRun(ctx, base, runID)
}

// DiagnoseRun returns the master's diagnosis of a run as raw JSON fields.
func DiagnoseRun(ctx context.Context, base, runID string) (map[string]any, error) {
	return diagnoseRun(ctx, base, runID)
}

// DefaultSourceDSN is an example DSN for a source engine, used as a hint.
func DefaultSourceDSN(engine string) string { return defaultSourceDSN(engine, "") }

// SubmitRun validates spec and submits it to the master at base, the same
// way `run submit` does.
func SubmitRun(ctx context.Context, base string, spec RunSubmitFile) (runID string, taskCount int, err error) {
	if err := spec.Validate(); err != nil {
		return "", 0, err
	}
	_, runID, taskCount, err = submitRunPlan(ctx, spec.toRanConfig(base))
	return runID, taskCount, err
}

// LoadRunSubmitFile reads a run spec with the strict `run submit` loader.
func LoadRunSubmitFile(path string) (RunSubmitFile, error) { return loadRunSubmitFile(path) }
