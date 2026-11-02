package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

type createCredentialRequest struct {
	Principal   string   `json:"principal"`
	Kind        string   `json:"kind"`
	Scopes      []string `json:"scopes"`
	TTLSeconds  int64    `json:"ttl_seconds,omitempty"`
	Description string   `json:"description,omitempty"`
}

type createCredentialResponse struct {
	Credential db.APICredential `json:"credential"`
	// Token is returned exactly once; only its SHA-256 is stored.
	Token string `json:"token"`
}

// handleCredentials serves GET (list) and POST (create) /credentials.
func (s *Server) handleCredentials(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		creds, err := s.st.ListAPICredentials(r.Context())
		if err != nil {
			writeInternalError(w, "failed to list credentials")
			return
		}
		if creds == nil {
			creds = []db.APICredential{}
		}
		writeJSON(w, http.StatusOK, creds)
	case http.MethodPost:
		s.handleCreateCredential(w, r)
	default:
		writeMethodNotAllowed(w, r.Method, http.MethodGet, http.MethodPost)
	}
}

func (s *Server) handleCreateCredential(w http.ResponseWriter, r *http.Request) {
	var req createCredentialRequest
	if err := readJSON(r, &req); err != nil {
		writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
		return
	}
	caller, _ := auth.PrincipalFrom(r.Context())
	cred, tok, err := auth.Issue(auth.IssueRequest{
		Principal: req.Principal, Kind: req.Kind, Scopes: req.Scopes,
		TTL: time.Duration(req.TTLSeconds) * time.Second, Description: req.Description,
	}, caller.Name, time.Now())
	if err != nil {
		writeInvalidInput(w, err.Error(), nil)
		return
	}
	// No privilege escalation: a caller can only grant scopes it holds.
	for _, name := range cred.Scopes {
		if !caller.Has(auth.Scope(name)) {
			writeForbidden(w, auth.Scope(name))
			return
		}
	}
	audit, err := s.newAuditRecord(r, auditActionCredentialCreate, "credential", cred.ID, credentialAuditMetadata(cred))
	if err != nil {
		writeInternalError(w, "failed to create credential")
		return
	}
	if err := s.st.CreateAPICredential(r.Context(), cred, &audit); err != nil {
		writeInternalError(w, "failed to create credential")
		return
	}
	cred.TokenSHA256 = ""
	writeJSON(w, http.StatusCreated, createCredentialResponse{Credential: cred, Token: tok.Value})
}

// handleCredentialByID serves POST /credentials/{id}/revoke.
func (s *Server) handleCredentialByID(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/credentials/"), "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "revoke" {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	id := parts[0]
	audit, err := s.newAuditRecord(r, auditActionCredentialRevoke, "credential", id, nil)
	if err != nil {
		writeInternalError(w, "failed to revoke credential")
		return
	}
	if err := s.st.RevokeAPICredential(r.Context(), id, &audit); err != nil {
		if errors.Is(err, db.ErrCredentialNotFound) {
			writeNotFound(w, "credential", nil)
			return
		}
		writeInternalError(w, "failed to revoke credential")
		return
	}
	cred, err := s.st.GetAPICredential(r.Context(), id)
	if err != nil {
		writeInternalError(w, "failed to revoke credential")
		return
	}
	cred.TokenSHA256 = ""
	writeJSON(w, http.StatusOK, cred)
}

func credentialAuditMetadata(c db.APICredential) map[string]any {
	return map[string]any{"principal": c.Principal, "kind": c.Kind, "scopes": c.Scopes, "expires_at": c.ExpiresAt}
}
