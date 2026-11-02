package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
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
	auditActionCredentialCreate = "credential.create"
	auditActionCredentialRevoke = "credential.revoke"
)

func (s *Server) newAuditRecord(r *http.Request, action, resourceType, resourceID string, metadata any) (db.AuditRecord, error) {
	meta, err := marshalAuditMetadata(metadata)
	if err != nil {
		return db.AuditRecord{}, err
	}
	actor := auditActor(r)
	return db.AuditRecord{
		ActorType:    actor.actorType,
		ActorID:      actor.name,
		Principal:    actor.name,
		CredentialID: actor.credentialID,
		Action:       action,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		RequestID:    strings.TrimSpace(r.Header.Get("X-Request-ID")),
		MetadataJSON: meta,
	}, nil
}

type actorIdentity struct{ actorType, name, credentialID string }

// auditActor identifies who made the request from the authenticated
// principal. It carries credential IDs and legacy-token fingerprints, never
// the credential itself.
func auditActor(r *http.Request) actorIdentity {
	p, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		return actorIdentity{actorType: string(auth.KindAnonymous), name: principalAnonymous}
	}
	return actorIdentity{actorType: string(p.Kind), name: p.Name, credentialID: p.CredentialID}
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
