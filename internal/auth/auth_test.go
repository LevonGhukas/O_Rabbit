package auth

import (
	"context"
	"strings"
	"testing"
)

func TestGenerateTokenRoundTrip(t *testing.T) {
	tok, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if !strings.HasPrefix(tok.Value, TokenPrefix) {
		t.Fatalf("token %q lacks prefix", tok.Value)
	}
	id, secret, ok := ParseToken(tok.Value)
	if !ok || id != tok.CredentialID || secret == "" {
		t.Fatalf("ParseToken(%q) = %q %q %v", tok.Value, id, secret, ok)
	}
	if tok.Hash != HashToken(tok.Value) || strings.Contains(tok.Hash, tok.Value) {
		t.Fatalf("hash mismatch: %q", tok.Hash)
	}
}

func TestParseTokenRejectsMalformed(t *testing.T) {
	for _, raw := range []string{"", "orb_", "orb_abc", "orb__secret", "orb_id_", "xyz_id_secret", "plain-token", "orb_a_b_c"} {
		if _, _, ok := ParseToken(raw); ok {
			t.Errorf("ParseToken(%q) accepted", raw)
		}
	}
}

func TestTokensAreUnique(t *testing.T) {
	a, _ := GenerateToken()
	b, _ := GenerateToken()
	if a.Value == b.Value || a.CredentialID == b.CredentialID {
		t.Fatal("generated tokens collide")
	}
}

func TestParseScopes(t *testing.T) {
	got, err := ParseScopes([]string{"read", "jobs:write", "read"})
	if err != nil || len(got) != 2 {
		t.Fatalf("ParseScopes = %v, %v", got, err)
	}
	if _, err := ParseScopes([]string{"read", "root"}); err == nil {
		t.Fatal("unknown scope accepted")
	}
	if _, err := ParseScopes(nil); err == nil {
		t.Fatal("empty scopes accepted")
	}
}

func TestPrincipalHas(t *testing.T) {
	p := Principal{Scopes: []Scope{ScopeRead}}
	if !p.Has(ScopeRead) || p.Has(ScopeJobsWrite) {
		t.Fatalf("Has misbehaves: %+v", p)
	}
}

func TestPrincipalContext(t *testing.T) {
	if _, ok := PrincipalFrom(context.Background()); ok {
		t.Fatal("empty context has principal")
	}
	ctx := WithPrincipal(context.Background(), Principal{Name: "ci"})
	if p, ok := PrincipalFrom(ctx); !ok || p.Name != "ci" {
		t.Fatalf("PrincipalFrom = %+v %v", p, ok)
	}
}

func TestParseKind(t *testing.T) {
	for _, k := range []string{"human", "automation"} {
		if _, err := ParseKind(k); err != nil {
			t.Errorf("ParseKind(%q): %v", k, err)
		}
	}
	for _, k := range []string{"", "legacy", "anonymous", "root"} {
		if _, err := ParseKind(k); err == nil {
			t.Errorf("ParseKind(%q) accepted", k)
		}
	}
}

func TestFingerprintHidesToken(t *testing.T) {
	fp := Fingerprint("topsecret")
	if strings.Contains(fp, "topsecret") || !strings.HasPrefix(fp, "sha256:") {
		t.Fatalf("fingerprint %q", fp)
	}
}
