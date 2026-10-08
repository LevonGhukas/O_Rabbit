package httpapi

import (
	"net/http"
	"strings"
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
}

func (s *Server) apiRoutes() []apiRoute {
	return []apiRoute{
		{v1: "/workers", legacy: []string{"/workers", "/api/workers"}, target: "/workers", handler: s.handleWorkers},
		{v1: "/workers/", legacy: []string{"/workers/"}, target: "/workers/", handler: s.handleWorkerRoutes},
		{v1: "/servers", legacy: []string{"/servers"}, target: "/servers", handler: s.handleServers},
		{v1: "/servers/", legacy: []string{"/servers/"}, target: "/servers/", handler: s.handleServerByID},
		{v1: "/deployments", legacy: []string{"/deployments"}, target: "/deployments", handler: s.handleDeployments},
		{v1: "/deployments/", legacy: []string{"/deployments/"}, target: "/deployments/", handler: s.handleDeploymentByID},
		{v1: "/executions/", legacy: []string{"/executions/"}, target: "/executions/", handler: s.handleExecutionByID},
		{v1: "/connections", legacy: []string{"/connections"}, target: "/connections", handler: s.handleConnections},
		{v1: "/connections/", legacy: []string{"/connections/"}, target: "/connections/", handler: s.handleConnectionByID},
		{v1: "/jobs", legacy: []string{"/jobs"}, target: "/jobs", handler: s.handleJobs},
		{v1: "/jobs/", legacy: []string{"/jobs/"}, target: "/jobs/", handler: s.handleJobByID},
		{v1: "/runs", legacy: []string{"/runs"}, target: "/runs", handler: s.handleRuns},
		{v1: "/runs/", legacy: []string{"/runs/"}, target: "/runs/", handler: s.handleRunByID},
		{v1: "/runs/submit", legacy: []string{"/api/runs/submit"}, target: "/api/runs/submit", handler: s.handleRunSubmit},
		{v1: "/runs/validate", legacy: []string{"/api/runs/validate"}, target: "/api/runs/validate", handler: s.handleRunValidate},
		{v1: "/source-engines", legacy: []string{"/api/source-engines"}, target: "/api/source-engines", handler: s.handleSourceEngines},
		{v1: "/maintenance/submit", legacy: []string{"/api/maintenance/submit"}, target: "/api/maintenance/submit", handler: s.handleMaintenanceSubmit},
		{v1: "/sse", legacy: []string{"/sse"}, target: "/sse", handler: SSEHandler(s.log, s.st, s.bc)},
	}
}

// registerAPIRoutes serves every API resource under /api/v1 and keeps the
// older paths working as deprecated aliases.
func (s *Server) registerAPIRoutes(mux *http.ServeMux) {
	for _, route := range s.apiRoutes() {
		mux.HandleFunc(apiV1Prefix+route.v1, withPathPrefix(apiV1Prefix+route.v1, route.target, route.handler))
		for _, legacy := range route.legacy {
			mux.HandleFunc(legacy, deprecated(legacy, apiV1Prefix+route.v1, withPathPrefix(legacy, route.target, route.handler)))
		}
	}
	// /api/runs and /api/runs/{id}/... were a second copy of /runs.
	mux.HandleFunc("/api/runs", deprecated("/api/runs", apiV1Prefix+"/runs", s.handleAPIRuns))
	mux.HandleFunc("/api/runs/", deprecated("/api/runs/", apiV1Prefix+"/runs/", s.handleAPIRunByID))
	// POST /api/jobs/{id}/runs takes {mode, iceberg} and returns run URLs; its
	// successor POST /api/v1/jobs/{id}/runs is the /jobs/{id}/runs contract
	// ({registration_config} → {run, tasks}) that the CLI uses.
	mux.HandleFunc("/api/jobs/", deprecated("/api/jobs/", apiV1Prefix+"/jobs/", s.handleAPIJobByID))
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
