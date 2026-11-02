package db

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestCredential(id string) APICredential {
	return APICredential{
		ID: id, Principal: "ci-bot", Kind: "automation", Scopes: []string{"read", "jobs:write"},
		TokenSHA256: "hash-" + id, CreatedBy: "cli", Description: "ci",
	}
}

func TestAPICredentialCreateGetList(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	audit := &AuditRecord{ActorType: "cli", Action: "credential.create", ResourceType: "credential", ResourceID: "c1"}

	if err := st.CreateAPICredential(ctx, newTestCredential("c1"), audit); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := st.GetAPICredential(ctx, "c1")
	if err != nil || got.Principal != "ci-bot" || len(got.Scopes) != 2 || got.TokenSHA256 != "hash-c1" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if got.CreatedAt == "" || got.RevokedAt != "" {
		t.Fatalf("timestamps = %+v", got)
	}
	list, err := st.ListAPICredentials(ctx)
	if err != nil || len(list) != 1 || list[0].TokenSHA256 != "" {
		t.Fatalf("list must omit hashes: %+v, %v", list, err)
	}
	audits, _ := st.ListAuditRecords(ctx, 10)
	if len(audits) != 1 || audits[0].Action != "credential.create" {
		t.Fatalf("audit = %+v", audits)
	}
}

func TestAPICredentialDuplicateHashRejected(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	a, b := newTestCredential("a"), newTestCredential("b")
	b.TokenSHA256 = a.TokenSHA256
	if err := st.CreateAPICredential(ctx, a, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateAPICredential(ctx, b, nil); err == nil {
		t.Fatal("duplicate hash accepted")
	}
}

func TestAPICredentialRevokeAndActiveCount(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if n, err := st.CountActiveAPICredentials(ctx); err != nil || n != 0 {
		t.Fatalf("empty count = %d, %v", n, err)
	}
	_ = st.CreateAPICredential(ctx, newTestCredential("c1"), nil)
	expired := newTestCredential("c2")
	expired.ExpiresAt = FormatTimestamp(time.Now().Add(-time.Hour))
	_ = st.CreateAPICredential(ctx, expired, nil)
	if n, _ := st.CountActiveAPICredentials(ctx); n != 1 {
		t.Fatalf("count with expired = %d, want 1", n)
	}
	if err := st.RevokeAPICredential(ctx, "c1", nil); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if n, _ := st.CountActiveAPICredentials(ctx); n != 0 {
		t.Fatalf("count after revoke = %d", n)
	}
	got, _ := st.GetAPICredential(ctx, "c1")
	if got.RevokedAt == "" || got.Active(time.Now()) {
		t.Fatalf("revoked credential active: %+v", got)
	}
	if err := st.RevokeAPICredential(ctx, "missing", nil); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("revoke missing = %v", err)
	}
}

func TestAPICredentialActiveHonorsExpiry(t *testing.T) {
	now := time.Now()
	c := newTestCredential("x")
	if !c.Active(now) {
		t.Fatal("no-expiry credential inactive")
	}
	c.ExpiresAt = FormatTimestamp(now.Add(time.Minute))
	if !c.Active(now) || c.Active(now.Add(time.Hour)) {
		t.Fatal("expiry not honored")
	}
	c.ExpiresAt = "garbage"
	if c.Active(now) {
		t.Fatal("unparseable expiry must fail closed")
	}
}

func TestAuditRecordCarriesPrincipal(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	rec := AuditRecord{ActorType: "human", ActorID: "alice", Principal: "alice", CredentialID: "c1", Action: "job.create", ResourceType: "job", ResourceID: "j1"}
	if err := st.InsertAuditRecord(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, _ := st.ListAuditRecords(ctx, 1)
	if len(got) != 1 || got[0].Principal != "alice" || got[0].CredentialID != "c1" {
		t.Fatalf("audit = %+v", got)
	}
}

func TestAPICredentialsAreLeadershipFenced(t *testing.T) {
	found := false
	for _, table := range fencedTables {
		found = found || table == "api_credentials"
	}
	if !found {
		t.Fatal("api_credentials must be fenced against stale masters")
	}
}

func TestHasAPICredentialsCountsRevokedRows(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if has, err := st.HasAPICredentials(ctx); err != nil || has {
		t.Fatalf("empty = %v, %v", has, err)
	}
	_ = st.CreateAPICredential(ctx, newTestCredential("c1"), nil)
	_ = st.RevokeAPICredential(ctx, "c1", nil)
	if has, _ := st.HasAPICredentials(ctx); !has {
		t.Fatal("revoked credential must still disable anonymous mode")
	}
}
