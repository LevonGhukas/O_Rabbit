package httpapi

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
)

// scopePolicy names the scope a request needs, given its method and
// canonical (unversioned, non-/api) path. An empty scope means no scope
// grants the request, so it is denied: routes fail closed.
type scopePolicy func(method, path string) auth.Scope

func isReadMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead
}

// readWrite needs read for GET/HEAD and write for every other method
// (including OPTIONS).
func readWrite(write auth.Scope) scopePolicy {
	return func(method, _ string) auth.Scope {
		if isReadMethod(method) {
			return auth.ScopeRead
		}
		return write
	}
}

// always needs scope for every method, for routes whose reads are sensitive
// too (worker identities, credentials, remote operations).
func always(scope auth.Scope) scopePolicy {
	return func(string, string) auth.Scope { return scope }
}

// policyPath reduces a request path to the form policies are written in:
// /api/v1/runs/x and /api/runs/x both become /runs/x.
func policyPath(path string) string {
	path = unversionedPath(path)
	if strings.HasPrefix(path, "/api/") {
		return strings.TrimPrefix(path, "/api")
	}
	return path
}

// authorized enforces policy for the principal that withAuthentication put in
// the request context.
func authorized(policy scopePolicy, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, ok := auth.PrincipalFrom(r.Context())
		if !ok {
			writeUnauthorized(w)
			return
		}
		var required auth.Scope
		if policy != nil {
			required = policy(r.Method, policyPath(r.URL.Path))
		}
		if required == "" || !principal.Has(required) {
			writeForbidden(w, required)
			return
		}
		next(w, r)
	}
}

func writeForbidden(w http.ResponseWriter, required auth.Scope) {
	msg := "forbidden: no credential scope permits this request"
	if required != "" {
		msg = fmt.Sprintf("forbidden: requires scope %q", required)
	}
	writeAPIError(w, http.StatusForbidden, httperr.CodeForbidden, msg, map[string]any{"required_scope": string(required)})
}
