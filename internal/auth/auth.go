// Package auth defines the scoped identities behind the control-plane API:
// who is calling (Principal), what they may do (Scope), and the format of the
// bearer credentials issued to them.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Scope is one permission a credential can carry.
type Scope string

const (
	ScopeRead             Scope = "read"
	ScopeJobsWrite        Scope = "jobs:write"
	ScopeSecretsAdmin     Scope = "secrets:admin"
	ScopeWorkersAdmin     Scope = "workers:admin"
	ScopeRemoteOps        Scope = "remote_ops"
	ScopeCredentialsAdmin Scope = "credentials:admin"
)

// AllScopes lists every valid scope.
var AllScopes = []Scope{ScopeRead, ScopeJobsWrite, ScopeSecretsAdmin, ScopeWorkersAdmin, ScopeRemoteOps, ScopeCredentialsAdmin}

// DataAPIScopes is every scope except remote_ops. Remote operations run
// commands on other hosts, so they are never implied by data-API access.
var DataAPIScopes = []Scope{ScopeRead, ScopeJobsWrite, ScopeSecretsAdmin, ScopeWorkersAdmin, ScopeCredentialsAdmin}

// Kind classifies a principal.
type Kind string

const (
	KindHuman      Kind = "human"
	KindAutomation Kind = "automation"
	// KindLegacy is a shared ORABBIT_*_AUTH_TOKEN; KindAnonymous is an
	// unauthenticated caller on a deployment with no credentials at all.
	KindLegacy    Kind = "legacy"
	KindAnonymous Kind = "anonymous"
)

// Principal is the authenticated caller of one request.
type Principal struct {
	Name         string
	Kind         Kind
	CredentialID string
	Scopes       []Scope
}

// Has reports whether the principal carries scope.
func (p Principal) Has(scope Scope) bool {
	for _, s := range p.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

type principalKey struct{}

// WithPrincipal returns ctx carrying p.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom returns the principal authenticated for ctx, if any.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// ParseKind validates a kind for a stored credential. Legacy and anonymous
// are runtime-only and cannot be issued.
func ParseKind(raw string) (Kind, error) {
	switch k := Kind(strings.TrimSpace(raw)); k {
	case KindHuman, KindAutomation:
		return k, nil
	}
	return "", fmt.Errorf("kind must be %q or %q", KindHuman, KindAutomation)
}

// ParseScopes validates, de-duplicates and sorts scope names.
func ParseScopes(raw []string) ([]Scope, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("at least one scope is required")
	}
	seen := map[Scope]bool{}
	for _, name := range raw {
		s := Scope(strings.TrimSpace(name))
		if !validScope(s) {
			return nil, fmt.Errorf("unknown scope %q (valid: %s)", name, ScopeList())
		}
		seen[s] = true
	}
	out := make([]Scope, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// ScopeList renders every valid scope for error messages and help text.
func ScopeList() string {
	names := make([]string, len(AllScopes))
	for i, s := range AllScopes {
		names[i] = string(s)
	}
	return strings.Join(names, ", ")
}

func validScope(s Scope) bool {
	for _, known := range AllScopes {
		if s == known {
			return true
		}
	}
	return false
}

// TokenPrefix starts every issued credential: orb_<credentialID>_<secret>.
// ID and secret are hex, so they never contain the "_" separator.
const TokenPrefix = "orb_"

const (
	credentialIDBytes = 8
	tokenSecretBytes  = 32
	tokenParts        = 3
)

// Token is a freshly issued credential. Value is shown once; only Hash is
// stored.
type Token struct {
	Value        string
	CredentialID string
	Hash         string
}

// GenerateToken creates a new random credential.
func GenerateToken() (Token, error) {
	id, err := randomHex(credentialIDBytes)
	if err != nil {
		return Token{}, err
	}
	secret, err := randomHex(tokenSecretBytes)
	if err != nil {
		return Token{}, err
	}
	value := TokenPrefix + id + "_" + secret
	return Token{Value: value, CredentialID: id, Hash: HashToken(value)}, nil
}

// ParseToken splits a presented credential into its ID and secret. It does
// not authenticate: callers must still compare HashToken(raw) in constant
// time against the stored hash.
func ParseToken(raw string) (id, secret string, ok bool) {
	if !strings.HasPrefix(raw, TokenPrefix) {
		return "", "", false
	}
	parts := strings.Split(raw, "_")
	if len(parts) != tokenParts || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// HashToken returns the hex SHA-256 stored in place of the credential. The
// secret is 256 bits of randomness, so a fast hash is sufficient.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Fingerprint is a short, non-reversible identifier for a legacy shared
// token, safe to write to audit logs.
func Fingerprint(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return "sha256:" + hex.EncodeToString(sum[:8])
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
