package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

// issueTestCredential stores a credential and returns its raw token.
func issueTestCredential(t *testing.T, st *db.Store, name string, expires time.Time, scopes ...auth.Scope) (string, string) {
	t.Helper()
	tok, err := auth.GenerateToken()
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	names := make([]string, len(scopes))
	for i, s := range scopes {
		names[i] = string(s)
	}
	cred := db.APICredential{
		ID: tok.CredentialID, Principal: name, Kind: "automation", Scopes: names,
		TokenSHA256: tok.Hash, CreatedBy: "test",
	}
	if !expires.IsZero() {
		cred.ExpiresAt = db.FormatTimestamp(expires)
	}
	if err := st.CreateAPICredential(context.Background(), cred, nil); err != nil {
		t.Fatalf("create credential: %v", err)
	}
	return tok.Value, tok.CredentialID
}

func authzRequest(srv *Server, method, path, token, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func TestTwoPrincipalsHaveDifferentPermissionsOnTheSameAPI(t *testing.T) {
	st := openTestStore(t)
	for _, c := range []struct{ id, kind, engine string }{{"src-1", "source", "postgres"}, {"tgt-1", "target", "s3"}} {
		createTestConnection(t, st, db.Connection{ID: c.id, Name: c.id, Kind: c.kind, Engine: c.engine, MetadataJSON: []byte(`{}`), SecretEncBlob: []byte(`{}`)})
	}
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	reader, _ := issueTestCredential(t, st, "alice", time.Time{}, auth.ScopeRead)
	writer, _ := issueTestCredential(t, st, "deploy-bot", time.Time{}, auth.ScopeRead, auth.ScopeJobsWrite)

	for _, tok := range []string{reader, writer} {
		if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", tok, ""); rec.Code != http.StatusOK {
			t.Fatalf("GET /jobs status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
	body := `{"name":"job-1","source_connection_id":"src-1","target_connection_id":"tgt-1","source_sql":"SELECT 1","target_namespace":"ns","target_table":"tbl","write_mode":"overwrite","options_json":{"partition_strategy":"single"}}`
	rec := authzRequest(srv, http.MethodPost, "/api/v1/jobs", reader, body)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), `"forbidden"`) || !strings.Contains(rec.Body.String(), "jobs:write") {
		t.Fatalf("read-only POST status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := authzRequest(srv, http.MethodPost, "/api/v1/jobs", writer, body); rec.Code != http.StatusCreated {
		t.Fatalf("writer POST status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestReadOnlyPrincipalCannotMutateOnAnyPath(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	reader, _ := issueTestCredential(t, st, "alice", time.Time{}, auth.ScopeRead)

	cases := []struct{ method, path string }{
		{"POST", "/jobs"}, {"PUT", "/jobs/j1"}, {"DELETE", "/jobs/j1"}, {"POST", "/jobs/j1/runs"},
		{"POST", "/runs/submit"}, {"POST", "/maintenance/submit"},
		{"POST", "/runs/r1/cancel"}, {"POST", "/runs/r1/recover"},
		{"POST", "/runs/r1/registration/retry"}, {"POST", "/runs/r1/registration/cancel"},
		{"POST", "/connections"}, {"PUT", "/connections/c1"}, {"DELETE", "/connections/c1"},
		{"POST", "/workers/enrollment-tokens"}, {"POST", "/workers/identities/w1/revoke"},
		{"GET", "/workers/identities"},
		{"PATCH", "/jobs/j1"}, {"OPTIONS", "/jobs"},
	}
	for _, c := range cases {
		for _, prefix := range []string{"/api/v1", "", "/api"} {
			path := prefix + c.path
			if !legacyPathExists(prefix, c.path) {
				continue
			}
			if rec := authzRequest(srv, c.method, path, reader, "{}"); rec.Code != http.StatusForbidden {
				t.Errorf("%s %s status=%d, want 403 (body=%s)", c.method, path, rec.Code, rec.Body.String())
			}
		}
	}
	for _, c := range []struct{ method, path string }{
		{"POST", "/api/v1/credentials"}, {"GET", "/api/v1/credentials"}, {"POST", "/api/v1/credentials/c1/revoke"},
	} {
		if rec := authzRequest(srv, c.method, c.path, reader, "{}"); rec.Code != http.StatusForbidden {
			t.Errorf("%s %s status=%d, want 403", c.method, c.path, rec.Code)
		}
	}
}

func TestReadScopeStillReadsEverythingNonSensitive(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	reader, _ := issueTestCredential(t, st, "alice", time.Time{}, auth.ScopeRead)
	for _, path := range []string{"/api/v1/runs", "/api/v1/jobs", "/api/v1/connections", "/api/v1/workers", "/status", "/metrics", "/api/runs"} {
		if rec := authzRequest(srv, http.MethodGet, path, reader, ""); rec.Code != http.StatusOK {
			t.Errorf("GET %s status=%d body=%s", path, rec.Code, rec.Body.String())
		}
	}
	if rec := authzRequest(srv, http.MethodGet, "/nope", reader, ""); rec.Code != http.StatusNotFound {
		t.Errorf("unknown route status=%d, want 404", rec.Code)
	}
	if rec := authzRequest(srv, http.MethodGet, "/nope", "", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated unknown route status=%d, want 401", rec.Code)
	}
}

func TestRemoteOperationsNeedTheRemoteOpsScope(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "api-token")
	srv.SetRemoteOpsEnabled(true)
	reader, _ := issueTestCredential(t, st, "alice", time.Time{}, auth.ScopeRead, auth.ScopeJobsWrite)
	ops, _ := issueTestCredential(t, st, "ops", time.Time{}, auth.ScopeRemoteOps)

	for _, prefix := range []string{"/api/v1", ""} {
		path := prefix + "/servers"
		if rec := authzRequest(srv, http.MethodGet, path, reader, ""); rec.Code != http.StatusForbidden {
			t.Errorf("read-only on %s status=%d, want 403", path, rec.Code)
		}
		if rec := authzRequest(srv, http.MethodGet, path, "api-token", ""); rec.Code != http.StatusUnauthorized {
			t.Errorf("legacy HTTP token on %s status=%d, want 401", path, rec.Code)
		}
		if rec := authzRequest(srv, http.MethodGet, path, ops, ""); rec.Code != http.StatusOK {
			t.Errorf("remote_ops credential on %s status=%d, want 200", path, rec.Code)
		}
		if rec := authzRequest(srv, http.MethodPost, prefix+"/deployments", reader, "{}"); rec.Code != http.StatusForbidden {
			t.Errorf("read-only POST %s/deployments status=%d, want 403", prefix, rec.Code)
		}
	}
	// remote_ops grants nothing on the data API.
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/runs", ops, ""); rec.Code != http.StatusForbidden {
		t.Errorf("remote_ops on data API status=%d, want 403", rec.Code)
	}
}

func TestRemoteOperationsDisabledWithoutTokenOrOptIn(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	ops, _ := issueTestCredential(t, st, "ops", time.Time{}, auth.ScopeRemoteOps)
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/servers", ops, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("disabled remote ops status=%d, want 404", rec.Code)
	}
}

func TestInvalidCredentialsAreRejected(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	expired, _ := issueTestCredential(t, st, "old", time.Now().Add(-time.Hour), auth.ScopeRead)
	revoked, revokedID := issueTestCredential(t, st, "gone", time.Time{}, auth.ScopeRead)
	valid, validID := issueTestCredential(t, st, "ok", time.Time{}, auth.ScopeRead)
	if err := st.RevokeAPICredential(context.Background(), revokedID, nil); err != nil {
		t.Fatal(err)
	}
	tampered := valid[:len(valid)-1] + "0"
	if tampered == valid {
		tampered = valid[:len(valid)-1] + "1"
	}
	for name, tok := range map[string]string{
		"expired": expired, "revoked": revoked, "tampered secret": tampered,
		"malformed": "orb_garbage", "unknown id": "orb_deadbeefdeadbeef_" + strings.Repeat("a", 64),
		"wrong id": "orb_" + validID + "x_" + strings.Repeat("a", 64), "no token": "",
	} {
		rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", tok, "")
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: status=%d, want 401", name, rec.Code)
		}
		if rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s: missing WWW-Authenticate", name)
		}
	}
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", valid, ""); rec.Code != http.StatusOK {
		t.Fatalf("valid status=%d", rec.Code)
	}
}

func TestAnonymousModeEndsWhenACredentialExists(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("anonymous status=%d, want 200", rec.Code)
	}
	tok, id := issueTestCredential(t, st, "alice", time.Time{}, auth.ScopeRead)
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("after credential status=%d, want 401", rec.Code)
	}
	// Revoking the last credential must not reopen the API.
	if err := st.RevokeAPICredential(context.Background(), id, nil); err != nil {
		t.Fatal(err)
	}
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", tok, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked token status=%d, want 401", rec.Code)
	}
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("after revoking last credential status=%d, want 401", rec.Code)
	}
}

func TestLegacyTokenIsNeverAnonymousAndKeepsDataAPIScopes(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "api-token")
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token status=%d, want 401", rec.Code)
	}
	for _, path := range []string{"/healthz", "/ready"} {
		if rec := authzRequest(srv, http.MethodGet, path, "", ""); rec.Code == http.StatusUnauthorized {
			t.Fatalf("%s must stay unauthenticated", path)
		}
	}
	if rec := authzRequest(srv, http.MethodPost, "/api/v1/credentials", "api-token", `{"principal":"x","kind":"human","scopes":["read"]}`); rec.Code != http.StatusCreated {
		t.Fatalf("legacy token bootstraps credentials: status=%d body=%s", rec.Code, rec.Body.String())
	}
	// The legacy token keeps working after credentials exist.
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", "api-token", ""); rec.Code != http.StatusOK {
		t.Fatalf("legacy token after credential status=%d", rec.Code)
	}
}

func TestAuditRecordsIdentifyPrincipalWithoutToken(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	tok, id := issueTestCredential(t, st, "deploy-bot", time.Time{}, auth.ScopeRead, auth.ScopeSecretsAdmin)
	body := `{"name":"source-1","kind":"source","engine":"postgres","metadata":{"dsn":"postgres://db"},"secret":{"password":"pw"}}`
	if rec := authzRequest(srv, http.MethodPost, "/api/v1/connections", tok, body); rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	audit := latestAuditRecord(t, st)
	if audit.Principal != "deploy-bot" || audit.CredentialID != id || audit.ActorID != "deploy-bot" || audit.ActorType != "automation" {
		t.Fatalf("audit identity = %+v", audit)
	}
	raw, _ := json.Marshal(audit)
	if strings.Contains(string(raw), tok) || strings.Contains(string(raw), tok[len(tok)-16:]) {
		t.Fatalf("audit record leaks the token: %s", raw)
	}
}

func TestStoredCredentialNeverContainsRawToken(t *testing.T) {
	st := openTestStore(t)
	tok, id := issueTestCredential(t, st, "alice", time.Time{}, auth.ScopeRead)
	got, err := st.GetAPICredential(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(got)
	if got.TokenSHA256 == tok || strings.Contains(got.TokenSHA256, tok) || strings.Contains(string(raw), tok) {
		t.Fatal("raw token persisted or serialized")
	}
	if got.TokenSHA256 != auth.HashToken(tok) {
		t.Fatal("stored value is not the token hash")
	}
}

// legacyPathExists reports whether prefix+path is a served alias: the
// maintenance and run-submit routes only had /api/... legacy paths.
func legacyPathExists(prefix, path string) bool {
	switch {
	case strings.HasPrefix(path, "/maintenance"), path == "/runs/submit":
		return prefix != ""
	case prefix == "/api":
		return strings.HasPrefix(path, "/runs") || strings.HasPrefix(path, "/jobs/")
	}
	return true
}
