package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func adminServer(t *testing.T) (*Server, *db.Store, string) {
	t.Helper()
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	admin, _ := issueTestCredential(t, st, "admin", time.Time{}, auth.DataAPIScopes...)
	return srv, st, admin
}

func TestCredentialCreateReturnsTokenOnceAndAudits(t *testing.T) {
	srv, st, admin := adminServer(t)
	body := `{"principal":"ci-bot","kind":"automation","scopes":["read","jobs:write"],"ttl_seconds":3600,"description":"ci"}`
	rec := authzRequest(srv, http.MethodPost, "/api/v1/credentials", admin, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Credential db.APICredential `json:"credential"`
		Token      string           `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil || !strings.HasPrefix(created.Token, auth.TokenPrefix) {
		t.Fatalf("response = %s (%v)", rec.Body.String(), err)
	}
	if created.Credential.ExpiresAt == "" || strings.Contains(rec.Body.String(), auth.HashToken(created.Token)) {
		t.Fatalf("response exposes hash or lacks expiry: %s", rec.Body.String())
	}
	// The new token works, and only with the scopes it was given.
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", created.Token, ""); rec.Code != http.StatusOK {
		t.Fatalf("new token status=%d", rec.Code)
	}
	if rec := authzRequest(srv, http.MethodPost, "/api/v1/connections", created.Token, "{}"); rec.Code != http.StatusForbidden {
		t.Fatalf("new token on connections status=%d, want 403", rec.Code)
	}
	audit := latestAuditRecordFor(t, st, auditActionCredentialCreate)
	if audit.Principal != "admin" || audit.ResourceID != created.Credential.ID {
		t.Fatalf("audit = %+v", audit)
	}
	if raw, _ := json.Marshal(audit); strings.Contains(string(raw), created.Token) {
		t.Fatal("audit leaks token")
	}
	// Listing never returns tokens or hashes.
	list := authzRequest(srv, http.MethodGet, "/api/v1/credentials", admin, "")
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), "token") || strings.Contains(list.Body.String(), auth.HashToken(created.Token)) {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
}

func TestCredentialRevokeIsAuditedAndEndsAccess(t *testing.T) {
	srv, st, admin := adminServer(t)
	tok, id := issueTestCredential(t, st, "bot", time.Time{}, auth.ScopeRead)
	rec := authzRequest(srv, http.MethodPost, "/api/v1/credentials/"+id+"/revoke", admin, "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "revoked_at") {
		t.Fatalf("revoke status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := authzRequest(srv, http.MethodGet, "/api/v1/jobs", tok, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked token status=%d, want 401", rec.Code)
	}
	if a := latestAuditRecordFor(t, st, auditActionCredentialRevoke); a.ResourceID != id || a.Principal != "admin" {
		t.Fatalf("audit = %+v", a)
	}
	if rec := authzRequest(srv, http.MethodPost, "/api/v1/credentials/missing/revoke", admin, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d, want 404", rec.Code)
	}
}

func TestCredentialCreateValidatesInput(t *testing.T) {
	srv, _, admin := adminServer(t)
	for name, body := range map[string]string{
		"bad json":      `{`,
		"no principal":  `{"kind":"human","scopes":["read"]}`,
		"unknown scope": `{"principal":"x","kind":"human","scopes":["root"]}`,
		"legacy kind":   `{"principal":"x","kind":"legacy","scopes":["read"]}`,
		"short ttl":     `{"principal":"x","kind":"human","scopes":["read"],"ttl_seconds":5}`,
		"no scopes":     `{"principal":"x","kind":"human","scopes":[]}`,
	} {
		if rec := authzRequest(srv, http.MethodPost, "/api/v1/credentials", admin, body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status=%d, want 400", name, rec.Code)
		}
	}
}

func TestCredentialCreateCannotEscalateBeyondCallersScopes(t *testing.T) {
	srv, _, admin := adminServer(t) // holds every scope except remote_ops
	body := `{"principal":"x","kind":"automation","scopes":["remote_ops"]}`
	if rec := authzRequest(srv, http.MethodPost, "/api/v1/credentials", admin, body); rec.Code != http.StatusForbidden {
		t.Fatalf("escalation status=%d, want 403", rec.Code)
	}
}

func latestAuditRecordFor(t *testing.T, st *db.Store, action string) db.AuditRecord {
	t.Helper()
	audits, err := st.ListAuditRecords(context.Background(), 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range audits {
		if a.Action == action {
			return a
		}
	}
	t.Fatalf("no audit record for %s in %+v", action, audits)
	return db.AuditRecord{}
}
