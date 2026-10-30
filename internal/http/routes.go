package httpapi

import (
	"net/http"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
)

// apiV1Prefix is the canonical, versioned API path. The unversioned paths
// (/runs, /jobs, /connections, /api/runs, ...) predate it and remain as
// deprecated aliases: they keep their behavior and point to their /api/v1
// successor with Deprecation and Link headers. Operational endpoints
// (/healthz, /ready, /metrics, /status) are not versioned.
const apiV1Prefix = "/api/v1"

// apiRoute is one API resource: its legacy path(s) and the handler that
// serves it. The handler always receives the legacy-shaped path, so
// existing path parsing keeps working unchanged.
type apiRoute struct {
	v1      string // path under /api/v1, e.g. "/runs/"
	legacy  []string
	target  string // legacy path prefix the handler expects
	handler http.HandlerFunc
	// policy names the scope each request needs. A nil policy denies every
	// request, so a new route must declare one.
	policy scopePolicy
}

func (s *Server) apiRoutes() []apiRoute {
	return []apiRoute{
		{v1: "/workers", legacy: []string{"/workers", "/api/workers"}, target: "/workers", handler: s.handleWorkers, policy: readWrite(auth.ScopeWorkersAdmin)},
		{v1: "/workers/", legacy: []string{"/workers/"}, target: "/workers/", handler: s.handleWorkerRoutes, policy: always(auth.ScopeWorkersAdmin)},
		{v1: "/servers", legacy: []string{"/servers"}, target: "/servers", handler: s.handleServers, policy: always(auth.ScopeRemoteOps)},
		{v1: "/servers/", legacy: []string{"/servers/"}, target: "/servers/", handler: s.handleServerByID, policy: always(auth.ScopeRemoteOps)},
		{v1: "/deployments", legacy: []string{"/deployments"}, target: "/deployments", handler: s.handleDeployments, policy: always(auth.ScopeRemoteOps)},
		{v1: "/deployments/", legacy: []string{"/deployments/"}, target: "/deployments/", handler: s.handleDeploymentByID, policy: always(auth.ScopeRemoteOps)},
		{v1: "/executions/", legacy: []string{"/executions/"}, target: "/executions/", handler: s.handleExecutionByID, policy: always(auth.ScopeRemoteOps)},
		{v1: "/connections", legacy: []string{"/connections"}, target: "/connections", handler: s.handleConnections, policy: readWrite(auth.ScopeSecretsAdmin)},
		{v1: "/connections/", legacy: []string{"/connections/"}, target: "/connections/", handler: s.handleConnectionByID, policy: readWrite(auth.ScopeSecretsAdmin)},
		{v1: "/jobs", legacy: []string{"/jobs"}, target: "/jobs", handler: s.handleJobs, policy: readWrite(auth.ScopeJobsWrite)},
		{v1: "/jobs/", legacy: []string{"/jobs/"}, target: "/jobs/", handler: s.handleJobByID, policy: readWrite(auth.ScopeJobsWrite)},
		{v1: "/runs", legacy: []string{"/runs"}, target: "/runs", handler: s.handleRuns, policy: readWrite(auth.ScopeJobsWrite)},
		{v1: "/runs/", legacy: []string{"/runs/"}, target: "/runs/", handler: s.handleRunByID, policy: readWrite(auth.ScopeJobsWrite)},
		{v1: "/runs/submit", legacy: []string{"/api/runs/submit"}, target: "/api/runs/submit", handler: s.handleRunSubmit, policy: readWrite(auth.ScopeJobsWrite)},
		{v1: "/runs/validate", legacy: []string{"/api/runs/validate"}, target: "/api/runs/validate", handler: s.handleRunValidate, policy: validatePolicy},
		{v1: "/source-engines", legacy: []string{"/api/source-engines"}, target: "/api/source-engines", handler: s.handleSourceEngines, policy: readWrite(auth.ScopeJobsWrite)},
		{v1: "/maintenance/submit", legacy: []string{"/api/maintenance/submit"}, target: "/api/maintenance/submit", handler: s.handleMaintenanceSubmit, policy: readWrite(auth.ScopeJobsWrite)},
		{v1: "/maintenance", target: "/maintenance", handler: s.handleMaintenance, policy: readWrite(auth.ScopeJobsWrite)},
		{v1: "/maintenance/", target: "/maintenance/", handler: s.handleMaintenanceByID, policy: readWrite(auth.ScopeJobsWrite)},
		{v1: "/credentials", legacy: nil, target: "/credentials", handler: s.handleCredentials, policy: always(auth.ScopeCredentialsAdmin)},
		{v1: "/credentials/", legacy: nil, target: "/credentials/", handler: s.handleCredentialByID, policy: always(auth.ScopeCredentialsAdmin)},
		{v1: "/sse", legacy: []string{"/sse"}, target: "/sse", handler: SSEHandler(s.log, s.st, s.bc), policy: readWrite(auth.ScopeJobsWrite)},
	}
}

// registerAPIRoutes serves every API resource under /api/v1 and keeps the
// older paths working as deprecated aliases.
func (s *Server) registerAPIRoutes(mux *http.ServeMux) {
	for _, route := range s.apiRoutes() {
		// The scope check wraps the path rewrite so v1 and every legacy
		// alias share one policy.
		handler := authorized(route.policy, route.handler)
		mux.HandleFunc(apiV1Prefix+route.v1, withPathPrefix(apiV1Prefix+route.v1, route.target, handler))
		for _, legacy := range route.legacy {
			mux.HandleFunc(legacy, deprecated(legacy, apiV1Prefix+route.v1, withPathPrefix(legacy, route.target, handler)))
		}
	}
	// /api/runs and /api/runs/{id}/... were a second copy of /runs.
	mux.HandleFunc("/api/runs", deprecated("/api/runs", apiV1Prefix+"/runs", authorized(readWrite(auth.ScopeJobsWrite), s.handleAPIRuns)))
	mux.HandleFunc("/api/runs/", deprecated("/api/runs/", apiV1Prefix+"/runs/", authorized(readWrite(auth.ScopeJobsWrite), s.handleAPIRunByID)))
	// POST /api/jobs/{id}/runs takes {mode, iceberg} and returns run URLs; its
	// successor POST /api/v1/jobs/{id}/runs is the /jobs/{id}/runs contract
	// ({registration_config} → {run, tasks}) that the CLI uses.
	mux.HandleFunc("/api/jobs/", deprecated("/api/jobs/", apiV1Prefix+"/jobs/", authorized(readWrite(auth.ScopeJobsWrite), s.handleAPIJobByID)))
}

// withPathPrefix rewrites the request path from prefix `from` to `to` before
// calling next, so a handler written for one path serves another.
func withPathPrefix(from, to string, next http.HandlerFunc) http.HandlerFunc {
	if from == to {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		next(w, cloneRequestWithPath(r, to+strings.TrimPrefix(r.URL.Path, from)))
	}
}

// deprecated marks responses from a legacy path (RFC 9745 Deprecation header)
// and names its /api/v1 successor (RFC 8288 Link).
func deprecated(legacyPrefix, successorPrefix string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		successor := successorPrefix + strings.TrimPrefix(r.URL.Path, legacyPrefix)
		w.Header().Set("Deprecation", "true")
		w.Header().Add("Link", "<"+successor+`>; rel="successor-version"`)
		next(w, r)
	}
}

// unversionedPath maps /api/v1/x to /x so path-based policy (remote-ops
// guard, auth exemptions) treats versioned and legacy paths alike.
func unversionedPath(path string) string {
	if path == apiV1Prefix {
		return "/"
	}
	if strings.HasPrefix(path, apiV1Prefix+"/") {
		return strings.TrimPrefix(path, apiV1Prefix)
	}
	return path
}

// validatePolicy lets read-only principals dry-run a run submission:
// POST /runs/validate checks a request and persists nothing.
func validatePolicy(string, string) auth.Scope { return auth.ScopeRead }
