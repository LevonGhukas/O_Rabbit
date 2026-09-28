package httpapi

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

const (
	auditActionWorkerEnrollmentTokenCreate = "worker.enrollment_token_create"
	auditActionWorkerRevoke                = "worker.revoke"

	defaultEnrollmentTokenTTL = time.Hour
	maxEnrollmentTokenTTL     = 24 * time.Hour
	maxEnrollmentTokenUses    = 1000
)

type enrollmentTokenRequest struct {
	Pool       string `json:"pool"`
	TTLSeconds int    `json:"ttl_seconds"`
	MaxUses    int    `json:"max_uses"`
}

type enrollmentTokenResponse struct {
	db.EnrollmentToken
	Token string `json:"token"`
}

// handleWorkerRoutes serves worker identity administration:
//
//	POST /workers/enrollment-tokens         create a short-lived enrollment token
//	GET  /workers/identities                list master-issued worker identities
//	POST /workers/identities/{id}/revoke    revoke an identity immediately
func (s *Server) handleWorkerRoutes(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/workers/"), "/"), "/")
	switch {
	case len(parts) == 1 && parts[0] == "enrollment-tokens":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, r.Method, http.MethodPost)
			return
		}
		s.handleCreateEnrollmentToken(w, r)
	case len(parts) == 1 && parts[0] == "identities":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, r.Method, http.MethodGet)
			return
		}
		identities, err := s.st.ListWorkerIdentities(r.Context())
		if err != nil {
			writeInternalError(w, "failed to list worker identities")
			return
		}
		writeJSON(w, http.StatusOK, identities)
	case len(parts) == 3 && parts[0] == "identities" && parts[2] == "revoke" && parts[1] != "":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, r.Method, http.MethodPost)
			return
		}
		s.handleRevokeWorkerIdentity(w, r, parts[1])
	default:
		writeUnknownRoute(w, r.URL.Path)
	}
}

func (s *Server) handleCreateEnrollmentToken(w http.ResponseWriter, r *http.Request) {
	var req enrollmentTokenRequest
	if err := readOptionalJSON(r, &req); err != nil {
		writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
		return
	}
	pool := strings.TrimSpace(req.Pool)
	if pool == "" {
		pool = db.DefaultWorkerPool
	}
	if !jobopts.ValidWorkerPool(pool) {
		writeInvalidInput(w, "pool must be 1-63 lowercase letters, digits, '-' or '_', starting with a letter or digit", map[string]any{"field": "pool"})
		return
	}
	ttl := defaultEnrollmentTokenTTL
	if req.TTLSeconds != 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}
	if ttl < time.Minute || ttl > maxEnrollmentTokenTTL {
		writeInvalidInput(w, "ttl_seconds must be between 60 and 86400", map[string]any{"field": "ttl_seconds"})
		return
	}
	maxUses := req.MaxUses
	if maxUses == 0 {
		maxUses = 1
	}
	if maxUses < 1 || maxUses > maxEnrollmentTokenUses {
		writeInvalidInput(w, "max_uses must be between 1 and 1000", map[string]any{"field": "max_uses"})
		return
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		writeInternalError(w, "failed to create enrollment token")
		return
	}
	token := "orw_" + base64.RawURLEncoding.EncodeToString(secret)
	digest := sha256.Sum256([]byte(token))
	id := newID()
	audit, err := s.newAuditRecord(r, auditActionWorkerEnrollmentTokenCreate, "worker_enrollment_token", id, nil)
	if err != nil {
		writeInternalError(w, "failed to create enrollment token")
		return
	}
	created, err := s.st.CreateEnrollmentTokenAudited(r.Context(), id, hex.EncodeToString(digest[:]), pool, maxUses, time.Now().Add(ttl), audit)
	if err != nil {
		writeInternalError(w, "failed to create enrollment token")
		return
	}
	// The token is returned exactly once; only its digest is stored.
	writeJSON(w, http.StatusCreated, enrollmentTokenResponse{EnrollmentToken: created, Token: token})
}

func (s *Server) handleRevokeWorkerIdentity(w http.ResponseWriter, r *http.Request, id string) {
	audit, err := s.newAuditRecord(r, auditActionWorkerRevoke, "worker_identity", id, nil)
	if err != nil {
		writeInternalError(w, "failed to revoke worker identity")
		return
	}
	revoked, err := s.st.RevokeWorkerIdentityAudited(r.Context(), id, time.Now(), audit)
	if err != nil {
		if handleLookupError(w, err, "worker identity") {
			return
		}
		writeInternalError(w, "failed to revoke worker identity")
		return
	}
	writeJSON(w, http.StatusOK, revoked)
}
