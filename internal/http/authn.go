package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
)

const (
	realmData      = `Bearer realm="orabbit"`
	realmRemoteOps = `Bearer realm="orabbit-remote-ops"`

	principalAnonymous       = "anonymous"
	principalLegacyHTTP      = "legacy-http-token"
	principalLegacyRemoteOps = "legacy-remote-ops-token"
)

// SetRemoteOpsEnabled enables the remote operations API without a shared
// token: it is then reachable only by credentials carrying the remote_ops
// scope. SetRemoteOpsToken also enables it, for the legacy token.
func (s *Server) SetRemoteOpsEnabled(enabled bool) {
	s.remoteOpsOptIn = enabled
}

func (s *Server) remoteOpsEnabled() bool {
	return s.remoteOpsOptIn || s.remoteOpsToken != ""
}

// withAuthentication resolves the caller of every request except health
// probes into an auth.Principal and stores it in the request context. Scope
// checks happen per route (see authorized). Remote operations are 404 until
// enabled, and legacy shared tokens only authenticate in their own realm: the
// HTTP token never reaches remote operations and the remote-ops token never
// reaches the data API.
func (s *Server) withAuthentication(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/ready" {
			next.ServeHTTP(w, r)
			return
		}
		remote := isRemoteOpsPath(r.URL.Path)
		if remote && !s.remoteOpsEnabled() {
			writeAPIError(w, http.StatusNotFound, httperr.CodeNotFound, "remote operations are disabled; set ORABBIT_REMOTE_OPS_AUTH_TOKEN or ORABBIT_REMOTE_OPS_ENABLED=true to enable them", nil)
			return
		}
		principal, ok, err := s.resolvePrincipal(r, remote)
		if err != nil {
			s.log.Error("resolve principal", "err", err)
			writeInternalError(w, "")
			return
		}
		if !ok {
			realm := realmData
			if remote {
				realm = realmRemoteOps
			}
			w.Header().Set("WWW-Authenticate", realm)
			writeUnauthorized(w)
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), principal)))
	})
}

// resolvePrincipal maps the presented bearer token to a principal. ok is
// false when the caller is not authenticated.
func (s *Server) resolvePrincipal(r *http.Request, remote bool) (auth.Principal, bool, error) {
	header := r.Header.Get("Authorization")
	if token, found := bearerToken(header); found && strings.HasPrefix(token, auth.TokenPrefix) {
		return s.principalFromCredential(r, token)
	}
	if remote {
		if s.remoteOpsToken != "" && constantTimeBearerMatch(header, s.remoteOpsToken) {
			return legacyPrincipal(principalLegacyRemoteOps, s.remoteOpsToken, auth.ScopeRemoteOps), true, nil
		}
		return auth.Principal{}, false, nil
	}
	if s.authToken != "" {
		if constantTimeBearerMatch(header, s.authToken) {
			return legacyPrincipal(principalLegacyHTTP, s.authToken, auth.DataAPIScopes...), true, nil
		}
		return auth.Principal{}, false, nil
	}
	return s.anonymousPrincipal(r)
}

// anonymousPrincipal preserves the open loopback deployment: with no legacy
// token and no credentials ever issued, callers get the data-API scopes. The
// check is per request so a credential created by the CLI takes effect at
// once.
func (s *Server) anonymousPrincipal(r *http.Request) (auth.Principal, bool, error) {
	if s.st != nil {
		has, err := s.st.HasAPICredentials(r.Context())
		if err != nil || has {
			return auth.Principal{}, false, err
		}
	}
	return auth.Principal{Name: principalAnonymous, Kind: auth.KindAnonymous, Scopes: auth.DataAPIScopes}, true, nil
}

func (s *Server) principalFromCredential(r *http.Request, token string) (auth.Principal, bool, error) {
	id, _, ok := auth.ParseToken(token)
	if !ok || s.st == nil {
		return auth.Principal{}, false, nil
	}
	cred, err := s.st.GetAPICredential(r.Context(), id)
	if err != nil {
		if errors.Is(err, db.ErrCredentialNotFound) {
			return auth.Principal{}, false, nil
		}
		return auth.Principal{}, false, err
	}
	if subtle.ConstantTimeCompare([]byte(auth.HashToken(token)), []byte(cred.TokenSHA256)) != 1 || !cred.Active(time.Now()) {
		return auth.Principal{}, false, nil
	}
	scopes := make([]auth.Scope, len(cred.Scopes))
	for i, name := range cred.Scopes {
		scopes[i] = auth.Scope(name)
	}
	return auth.Principal{Name: cred.Principal, Kind: auth.Kind(cred.Kind), CredentialID: cred.ID, Scopes: scopes}, true, nil
}

func legacyPrincipal(name, token string, scopes ...auth.Scope) auth.Principal {
	return auth.Principal{Name: name, Kind: auth.KindLegacy, CredentialID: auth.Fingerprint(token), Scopes: scopes}
}

// bearerToken extracts the token from a "Bearer <token>" header.
func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	return strings.TrimPrefix(header, prefix), true
}

func constantTimeBearerMatch(header, expectedToken string) bool {
	presented, validScheme := bearerToken(header)
	presentedDigest := sha256.Sum256([]byte(presented))
	expectedDigest := sha256.Sum256([]byte(expectedToken))
	return validScheme && subtle.ConstantTimeCompare(presentedDigest[:], expectedDigest[:]) == 1
}
