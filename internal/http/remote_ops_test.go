package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func remoteOpsRequest(srv *Server, method, path, token string) int {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	srv.Handler().ServeHTTP(rec, req)
	return rec.Code
}

func TestRemoteOperationsAreDisabledByDefault(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "api-token")
	for _, path := range []string{"/servers", "/servers/s1/ssh/test", "/deployments", "/executions/e1"} {
		if code := remoteOpsRequest(srv, http.MethodGet, path, "api-token"); code != http.StatusNotFound {
			t.Fatalf("%s status=%d, want 404 while remote operations are disabled", path, code)
		}
	}
}

func TestRemoteOperationsRequireTheirOwnToken(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRemoteOpsToken("ops-token")

	if code := remoteOpsRequest(srv, http.MethodGet, "/servers", "api-token"); code != http.StatusUnauthorized {
		t.Fatalf("API token on remote operations status=%d, want 401", code)
	}
	if code := remoteOpsRequest(srv, http.MethodGet, "/servers", ""); code != http.StatusUnauthorized {
		t.Fatalf("no token status=%d, want 401", code)
	}
	if code := remoteOpsRequest(srv, http.MethodGet, "/servers", "ops-token"); code != http.StatusOK {
		t.Fatalf("remote-ops token status=%d, want 200", code)
	}
	// The remote-ops token grants nothing on the data API.
	if code := remoteOpsRequest(srv, http.MethodGet, "/runs", "ops-token"); code != http.StatusUnauthorized {
		t.Fatalf("remote-ops token on data API status=%d, want 401", code)
	}
	if code := remoteOpsRequest(srv, http.MethodGet, "/runs", "api-token"); code != http.StatusOK {
		t.Fatalf("API token on data API status=%d, want 200", code)
	}
}
