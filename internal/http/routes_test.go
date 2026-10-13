package httpapi

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func serveRoute(srv *Server, method, path, token string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func TestV1RoutesServeTheSameResourcesWithoutDeprecation(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "")
	for _, path := range []string{"/api/v1/runs", "/api/v1/jobs", "/api/v1/connections", "/api/v1/workers", "/api/v1/source-engines"} {
		rec := serveRoute(srv, http.MethodGet, path, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Deprecation") != "" {
			t.Fatalf("%s must not be marked deprecated", path)
		}
	}
	if rec := serveRoute(srv, http.MethodGet, "/api/v1/runs/missing", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown run status=%d, want 404", rec.Code)
	}
}

func TestLegacyRoutesStillWorkAndNameTheirSuccessor(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "")
	cases := map[string]string{
		"/runs":               "/api/v1/runs",
		"/api/runs":           "/api/v1/runs",
		"/jobs":               "/api/v1/jobs",
		"/api/workers":        "/api/v1/workers",
		"/api/source-engines": "/api/v1/source-engines",
	}
	for legacy, successor := range cases {
		rec := serveRoute(srv, http.MethodGet, legacy, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d", legacy, rec.Code)
		}
		if rec.Header().Get("Deprecation") != "true" {
			t.Fatalf("%s missing Deprecation header", legacy)
		}
		if link := rec.Header().Get("Link"); !strings.Contains(link, "<"+successor+">") {
			t.Fatalf("%s Link=%q, want successor %s", legacy, link, successor)
		}
	}
	rec := serveRoute(srv, http.MethodGet, "/api/runs/run-9/events", "")
	if link := rec.Header().Get("Link"); !strings.Contains(link, "</api/v1/runs/run-9/events>") {
		t.Fatalf("sub-resource Link=%q", link)
	}
}

func TestVersionedRemoteOperationsKeepTheirOwnGuard(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "api-token")
	// Disabled by default on every path spelling.
	if rec := serveRoute(srv, http.MethodGet, "/api/v1/servers", "api-token"); rec.Code != http.StatusNotFound {
		t.Fatalf("disabled /api/v1/servers status=%d, want 404", rec.Code)
	}
	srv.SetRemoteOpsToken("ops-token")
	// The data-API token must never unlock remote operations via /api/v1.
	if rec := serveRoute(srv, http.MethodGet, "/api/v1/servers", "api-token"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("API token on /api/v1/servers status=%d, want 401", rec.Code)
	}
	if rec := serveRoute(srv, http.MethodGet, "/api/v1/servers", "ops-token"); rec.Code != http.StatusOK {
		t.Fatalf("ops token on /api/v1/servers status=%d, want 200", rec.Code)
	}
	if rec := serveRoute(srv, http.MethodGet, "/api/v1/runs", "ops-token"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("ops token on /api/v1/runs status=%d, want 401", rec.Code)
	}
}

func TestUnversionedPath(t *testing.T) {
	for in, want := range map[string]string{
		"/api/v1/servers/s1": "/servers/s1",
		"/api/v1":            "/",
		"/api/v1x/servers":   "/api/v1x/servers",
		"/runs":              "/runs",
	} {
		if got := unversionedPath(in); got != want {
			t.Fatalf("unversionedPath(%q)=%q want %q", in, got, want)
		}
	}
}

// TestOpenAPISpecCoversEveryV1Route keeps docs/openapi.yaml in step with the
// router: adding an /api/v1 resource without documenting it fails here.
func TestOpenAPISpecCoversEveryV1Route(t *testing.T) {
	raw, err := os.ReadFile("../../docs/openapi.yaml")
	if err != nil {
		t.Fatalf("read spec: %v", err)
	}
	var spec struct {
		Paths map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "")
	for _, route := range srv.apiRoutes() {
		documented := false
		for path := range spec.Paths {
			// "/runs/" (a subtree) is covered by any "/runs/{...}" path.
			if path == route.v1 || (strings.HasSuffix(route.v1, "/") && strings.HasPrefix(path, route.v1)) {
				documented = true
				break
			}
		}
		if !documented {
			t.Errorf("/api/v1%s is served but missing from docs/openapi.yaml", route.v1)
		}
	}
}
