package auth

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

// Bounds on issued credentials.
const (
	MaxPrincipalLength   = 128
	MaxDescriptionLength = 256
	MinTTL               = time.Minute
	MaxTTL               = 365 * 24 * time.Hour
)

var principalNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@:-]*$`)

// IssueRequest is a validated-on-use request for a new credential. A zero TTL
// means the credential never expires.
type IssueRequest struct {
	Principal   string
	Kind        string
	Scopes      []string
	TTL         time.Duration
	Description string
}

// Issue validates req and returns the credential row to store together with
// the one-time token. Only the token's hash is in the row.
func Issue(req IssueRequest, createdBy string, now time.Time) (db.APICredential, Token, error) {
	principal := strings.TrimSpace(req.Principal)
	if principal == "" || len(principal) > MaxPrincipalLength || !principalNamePattern.MatchString(principal) {
		return db.APICredential{}, Token{}, fmt.Errorf("principal must be 1-%d characters of letters, digits and . _ @ : -", MaxPrincipalLength)
	}
	kind, err := ParseKind(req.Kind)
	if err != nil {
		return db.APICredential{}, Token{}, err
	}
	scopes, err := ParseScopes(req.Scopes)
	if err != nil {
		return db.APICredential{}, Token{}, err
	}
	description := strings.TrimSpace(req.Description)
	if len(description) > MaxDescriptionLength {
		return db.APICredential{}, Token{}, fmt.Errorf("description must be at most %d characters", MaxDescriptionLength)
	}
	if req.TTL != 0 && (req.TTL < MinTTL || req.TTL > MaxTTL) {
		return db.APICredential{}, Token{}, fmt.Errorf("ttl must be between %s and %s", MinTTL, MaxTTL)
	}
	tok, err := GenerateToken()
	if err != nil {
		return db.APICredential{}, Token{}, err
	}
	cred := db.APICredential{
		ID: tok.CredentialID, Principal: principal, Kind: string(kind), Scopes: scopeNames(scopes),
		TokenSHA256: tok.Hash, CreatedBy: createdBy, Description: description,
	}
	if req.TTL != 0 {
		cred.ExpiresAt = db.FormatTimestamp(now.Add(req.TTL))
	}
	return cred, tok, nil
}

func scopeNames(scopes []Scope) []string {
	out := make([]string, len(scopes))
	for i, s := range scopes {
		out[i] = string(s)
	}
	return out
}
