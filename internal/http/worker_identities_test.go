package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func workerAdminRequest(t *testing.T, srv *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer topsecret")
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func TestEnrollmentTokenIsReturnedOnceAndStoredAsDigest(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "topsecret")

	rec := workerAdminRequest(t, srv, http.MethodPost, "/workers/enrollment-tokens", `{"pool":"gpu","ttl_seconds":600,"max_uses":3}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		ID, Token, Pool string
		MaxUses         int `json:"max_uses"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.Token, "orw_") || created.Pool != "gpu" || created.MaxUses != 3 {
		t.Fatalf("created=%+v", created)
	}
	audit := latestAuditRecord(t, st)
	if audit.Action != auditActionWorkerEnrollmentTokenCreate || strings.Contains(string(audit.AfterJSON), created.Token) {
		t.Fatalf("audit must record creation without the token: %+v", audit)
	}
	digest := sha256.Sum256([]byte(created.Token))
	now := time.Now()
	if _, err := st.EnrollWorkerIdentity(context.Background(), hex.EncodeToString(digest[:]), "11111111-1111-4111-8111-111111111111", "w", "h", "s", now.Add(time.Hour), now); err != nil {
		t.Fatalf("token must be usable through its digest: %v", err)
	}
}

func TestEnrollmentTokenValidation(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "topsecret")
	for _, body := range []string{`{"pool":"GPU!"}`, `{"ttl_seconds":10}`, `{"ttl_seconds":999999}`, `{"max_uses":-1}`, `{"max_uses":5000}`} {
		if rec := workerAdminRequest(t, srv, http.MethodPost, "/workers/enrollment-tokens", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d", body, rec.Code)
		}
	}
	unauth := httptest.NewRecorder()
	srv.Handler().ServeHTTP(unauth, httptest.NewRequest(http.MethodPost, "/workers/enrollment-tokens", nil))
	if unauth.Code != http.StatusUnauthorized {
		t.Fatalf("token creation must require authentication, status=%d", unauth.Code)
	}
}

func TestRevokeWorkerIdentity(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "topsecret")
	ctx, now := context.Background(), time.Now()
	if _, err := st.CreateEnrollmentTokenAudited(ctx, "tok", "digest", "default", 1, now.Add(time.Hour), db.AuditRecord{Action: "test", ResourceType: "test", ResourceID: "test"}); err != nil {
		t.Fatal(err)
	}
	id := "22222222-2222-4222-8222-222222222222"
	if _, err := st.EnrollWorkerIdentity(ctx, "digest", id, "w", "h", "s", now.Add(time.Hour), now); err != nil {
		t.Fatal(err)
	}

	if rec := workerAdminRequest(t, srv, http.MethodPost, "/workers/identities/"+id+"/revoke", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"REVOKED"`) {
		t.Fatalf("revoke status=%d body=%s", rec.Code, rec.Body.String())
	}
	if latestAuditRecord(t, st).Action != auditActionWorkerRevoke {
		t.Fatal("revocation must be audited")
	}
	if rec := workerAdminRequest(t, srv, http.MethodPost, "/workers/identities/missing/revoke", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown identity status=%d", rec.Code)
	}
	rec := workerAdminRequest(t, srv, http.MethodGet, "/workers/identities", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), id) {
		t.Fatalf("list status=%d body=%s", rec.Code, rec.Body.String())
	}
}
