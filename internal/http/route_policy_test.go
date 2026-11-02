package httpapi

import (
	"net/http"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
)

var mutatingMethods = []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions}

// TestEveryAPIRouteDeclaresAScopeForMutatingMethods fails closed: a new route
// without a policy, or one that lets a read-only credential mutate, fails.
func TestEveryAPIRouteDeclaresAScopeForMutatingMethods(t *testing.T) {
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "")
	for _, route := range srv.apiRoutes() {
		if route.policy == nil {
			t.Errorf("route %s has no scope policy", route.v1)
			continue
		}
		// POST /runs/validate is the one deliberate read-like POST.
		if route.v1 == "/runs/validate" {
			continue
		}
		for _, method := range mutatingMethods {
			scope := route.policy(method, policyPath(apiV1Prefix+route.v1))
			if scope == "" || scope == auth.ScopeRead {
				t.Errorf("route %s %s needs scope %q; mutating methods must require a write scope", method, route.v1, scope)
			}
		}
	}
}

func TestSensitiveRoutesRequireTheDocumentedScopes(t *testing.T) {
	srv := NewServer(nil, nil, nil, testCryptoKey, StatusInfo{}, "")
	policies := map[string]scopePolicy{}
	for _, route := range srv.apiRoutes() {
		policies[route.v1] = route.policy
	}
	cases := []struct {
		route, method string
		want          auth.Scope
	}{
		{"/jobs", "GET", auth.ScopeRead}, {"/jobs", "POST", auth.ScopeJobsWrite},
		{"/runs/", "POST", auth.ScopeJobsWrite}, {"/runs/submit", "POST", auth.ScopeJobsWrite},
		{"/connections", "GET", auth.ScopeRead}, {"/connections/", "DELETE", auth.ScopeSecretsAdmin},
		{"/workers/", "GET", auth.ScopeWorkersAdmin}, {"/workers/", "POST", auth.ScopeWorkersAdmin},
		{"/servers", "GET", auth.ScopeRemoteOps}, {"/executions/", "POST", auth.ScopeRemoteOps},
		{"/credentials", "GET", auth.ScopeCredentialsAdmin}, {"/credentials/", "POST", auth.ScopeCredentialsAdmin},
		{"/runs/validate", "POST", auth.ScopeRead},
	}
	for _, c := range cases {
		if got := policies[c.route](c.method, ""); got != c.want {
			t.Errorf("%s %s requires %q, want %q", c.method, c.route, got, c.want)
		}
	}
}

func TestPolicyPathNormalizesVersionedAndLegacyPaths(t *testing.T) {
	for in, want := range map[string]string{
		"/api/v1/runs/x": "/runs/x", "/api/runs/x": "/runs/x", "/runs/x": "/runs/x", "/api/maintenance/submit": "/maintenance/submit",
	} {
		if got := policyPath(in); got != want {
			t.Errorf("policyPath(%q) = %q, want %q", in, got, want)
		}
	}
}
