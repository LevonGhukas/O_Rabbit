package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

const (
	auditActionConnectionCreate = "connection.create"
	auditActionConnectionUpdate = "connection.update"
	auditActionConnectionDelete = "connection.delete"
	auditActionJobCreate        = "job.create"
	auditActionJobUpdate        = "job.update"
	auditActionJobDelete        = "job.delete"
	auditActionJobRunStart      = "job.run_start"
)

func (s *Server) newAuditRecord(r *http.Request, action, resourceType, resourceID string, metadata any) (db.AuditRecord, error) {
	meta, err := marshalAuditMetadata(metadata)
	if err != nil {
		return db.AuditRecord{}, err
	}
	actorType, actorID := s.auditActor()
	return db.AuditRecord{
		ActorType:    actorType,
		ActorID:      actorID,
		Action:       action,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		RequestID:    strings.TrimSpace(r.Header.Get("X-Request-ID")),
		MetadataJSON: meta,
	}, nil
}

func (s *Server) auditActor() (string, string) {
	if strings.TrimSpace(s.authToken) == "" {
		return "anonymous", ""
	}
	return "token", tokenFingerprint(s.authToken)
}

func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return "sha256:" + hex.EncodeToString(sum[:8])
}

func marshalAuditMetadata(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}
