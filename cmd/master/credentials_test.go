package main

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func runCreds(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := runCredentials(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestCredentialsCreateListRevoke(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "creds.sqlite")
	code, out, errOut := runCreds(t, "create", "--db", dbPath, "--principal", "ci-bot", "--kind", "automation", "--scopes", "read,jobs:write", "--ttl", "24h", "--description", "ci")
	if code != exitOK {
		t.Fatalf("create code=%d stderr=%s", code, errOut)
	}
	token := tokenFromOutput(t, out)

	st, err := db.Open(context.Background(), db.Config{Path: dbPath}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	creds, _ := st.ListAPICredentials(context.Background())
	if len(creds) != 1 || creds[0].Principal != "ci-bot" || creds[0].ExpiresAt == "" || len(creds[0].Scopes) != 2 {
		t.Fatalf("stored = %+v", creds)
	}
	stored, _ := st.GetAPICredential(context.Background(), creds[0].ID)
	if stored.TokenSHA256 != auth.HashToken(token) || strings.Contains(stored.TokenSHA256, token) {
		t.Fatal("stored hash does not match token or contains it")
	}
	audit, _ := st.ListAuditRecords(context.Background(), 5)
	if len(audit) != 1 || audit[0].ActorType != "cli" || audit[0].Action != "credential.create" || strings.Contains(string(audit[0].MetadataJSON), token) {
		t.Fatalf("audit = %+v", audit)
	}

	code, out, _ = runCreds(t, "list", "--db", dbPath)
	if code != exitOK || !strings.Contains(out, "ci-bot") || strings.Contains(out, token) {
		t.Fatalf("list code=%d out=%s", code, out)
	}

	if code, _, errOut := runCreds(t, "revoke", "--db", dbPath, creds[0].ID); code != exitOK {
		t.Fatalf("revoke code=%d stderr=%s", code, errOut)
	}
	got, _ := st.GetAPICredential(context.Background(), creds[0].ID)
	if got.RevokedAt == "" {
		t.Fatal("credential not revoked")
	}
	audit, _ = st.ListAuditRecords(context.Background(), 5)
	if audit[0].Action != "credential.revoke" || audit[0].ActorType != "cli" {
		t.Fatalf("revoke audit = %+v", audit[0])
	}
}

func TestCredentialsRejectsBadInput(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "creds.sqlite")
	for name, args := range map[string][]string{
		"no subcommand":  {},
		"unknown":        {"explode"},
		"no principal":   {"create", "--db", dbPath, "--kind", "human", "--scopes", "read"},
		"bad scope":      {"create", "--db", dbPath, "--principal", "x", "--kind", "human", "--scopes", "root"},
		"bad kind":       {"create", "--db", dbPath, "--principal", "x", "--kind", "legacy", "--scopes", "read"},
		"revoke no id":   {"revoke", "--db", dbPath},
		"revoke missing": {"revoke", "--db", dbPath, "nope"},
	} {
		if code, _, _ := runCreds(t, args...); code == exitOK {
			t.Errorf("%s: exit 0, want failure", name)
		}
	}
}

func tokenFromOutput(t *testing.T, out string) string {
	t.Helper()
	for _, field := range strings.Fields(out) {
		if strings.HasPrefix(field, auth.TokenPrefix) {
			return field
		}
	}
	t.Fatalf("no token in output %q", out)
	return ""
}
