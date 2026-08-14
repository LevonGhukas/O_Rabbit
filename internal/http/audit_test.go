package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func TestConnectionCreateWritesAuditRecord(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "topsecret")

	reqBody := `{
		"name":"source-1",
		"kind":"source",
		"engine":"postgres",
		"metadata":{"dsn":"postgres://db"},
		"secret":{"password":"supersecret"}
	}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/connections", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer topsecret")
	req.Header.Set("X-Request-ID", "req-connection-create")

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	audit := latestAuditRecord(t, st)
	if audit.Action != auditActionConnectionCreate {
		t.Fatalf("action=%q want=%q", audit.Action, auditActionConnectionCreate)
	}
	if audit.ResourceType != "connection" {
		t.Fatalf("resource_type=%q want=%q", audit.ResourceType, "connection")
	}
	if audit.ActorType != "token" {
		t.Fatalf("actor_type=%q want=%q", audit.ActorType, "token")
	}
	if audit.ActorID != tokenFingerprint("topsecret") {
		t.Fatalf("actor_id=%q want=%q", audit.ActorID, tokenFingerprint("topsecret"))
	}
	if audit.RequestID != "req-connection-create" {
		t.Fatalf("request_id=%q want=%q", audit.RequestID, "req-connection-create")
	}
	if len(audit.BeforeJSON) != 0 {
		t.Fatalf("before_json should be empty on create, got %s", string(audit.BeforeJSON))
	}
	after := decodeAuditPayloadMap(t, audit.AfterJSON)
	assertNoSecretFields(t, after)
	if after["name"] != "source-1" {
		t.Fatalf("after.name=%v want=%q", after["name"], "source-1")
	}
	if strings.TrimSpace(stringValue(after["id"])) == "" {
		t.Fatalf("expected non-empty connection id in after payload")
	}
}

func TestConnectionUpdateWritesAuditBeforeAfter(t *testing.T) {
	st := openTestStore(t)
	createTestConnection(t, st, db.Connection{
		ID:            "conn-update",
		Name:          "source-old",
		Kind:          "source",
		Engine:        "postgres",
		MetadataJSON:  []byte(`{"dsn":"postgres://old"}`),
		SecretEncBlob: []byte(`plaintext-secret`),
	})
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "topsecret")

	reqBody := `{
		"name":"source-new",
		"kind":"source",
		"engine":"postgres",
		"metadata":{"dsn":"postgres://new"}
	}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/connections/conn-update", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer topsecret")

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	audit := latestAuditRecord(t, st)
	if audit.Action != auditActionConnectionUpdate {
		t.Fatalf("action=%q want=%q", audit.Action, auditActionConnectionUpdate)
	}
	before := decodeAuditPayloadMap(t, audit.BeforeJSON)
	after := decodeAuditPayloadMap(t, audit.AfterJSON)
	assertNoSecretFields(t, before)
	assertNoSecretFields(t, after)
	if before["name"] != "source-old" {
		t.Fatalf("before.name=%v want=%q", before["name"], "source-old")
	}
	if after["name"] != "source-new" {
		t.Fatalf("after.name=%v want=%q", after["name"], "source-new")
	}
}

func TestConnectionDeleteWritesBeforeOnlyAuditRecord(t *testing.T) {
	st := openTestStore(t)
	createTestConnection(t, st, db.Connection{
		ID:            "conn-delete",
		Name:          "source-delete",
		Kind:          "source",
		Engine:        "postgres",
		MetadataJSON:  []byte(`{"dsn":"postgres://delete"}`),
		SecretEncBlob: []byte(`plaintext-secret`),
	})
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/connections/conn-delete", nil)

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	audit := latestAuditRecord(t, st)
	if audit.Action != auditActionConnectionDelete {
		t.Fatalf("action=%q want=%q", audit.Action, auditActionConnectionDelete)
	}
	if audit.ActorType != "anonymous" {
		t.Fatalf("actor_type=%q want=%q", audit.ActorType, "anonymous")
	}
	before := decodeAuditPayloadMap(t, audit.BeforeJSON)
	assertNoSecretFields(t, before)
	if before["id"] != "conn-delete" {
		t.Fatalf("before.id=%v want=%q", before["id"], "conn-delete")
	}
	if len(audit.AfterJSON) != 0 {
		t.Fatalf("after_json should be empty on delete, got %s", string(audit.AfterJSON))
	}
}

func TestJobCreateUpdateDeleteWriteAuditRecords(t *testing.T) {
	st := openTestStore(t)
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")

	createBody := `{
		"name":"job-1",
		"source_connection_id":"src-1",
		"target_connection_id":"tgt-1",
		"source_sql":"SELECT 1",
		"target_namespace":"ns",
		"target_table":"tbl",
		"write_mode":"append",
		"incremental":false,
		"options_json":{"partition_strategy":"single"}
	}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(createBody))
	srv.Handler().ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status=%d want=%d body=%s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	var created db.Job
	decodeJSONBody(t, createRec, &created)

	updateBody := `{
		"name":"job-1-updated",
		"source_connection_id":"src-2",
		"target_connection_id":"tgt-2",
		"source_sql":"SELECT 2",
		"target_namespace":"ns2",
		"target_table":"tbl2",
		"write_mode":"replace",
		"incremental":true,
		"hwm_column":"updated_at",
		"options_json":{"partition_strategy":"single"}
	}`
	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPut, "/jobs/"+created.ID, strings.NewReader(updateBody))
	srv.Handler().ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status=%d want=%d body=%s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	deleteRec := httptest.NewRecorder()
	deleteReq := httptest.NewRequest(http.MethodDelete, "/jobs/"+created.ID, nil)
	srv.Handler().ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d want=%d body=%s", deleteRec.Code, http.StatusNoContent, deleteRec.Body.String())
	}

	audits, err := st.ListAuditRecords(context.Background(), 10)
	if err != nil {
		t.Fatalf("list audit records: %v", err)
	}
	if len(audits) != 3 {
		t.Fatalf("audit count=%d want=3", len(audits))
	}
	if audits[0].Action != auditActionJobDelete {
		t.Fatalf("latest action=%q want=%q", audits[0].Action, auditActionJobDelete)
	}
	if audits[1].Action != auditActionJobUpdate {
		t.Fatalf("middle action=%q want=%q", audits[1].Action, auditActionJobUpdate)
	}
	if audits[2].Action != auditActionJobCreate {
		t.Fatalf("oldest action=%q want=%q", audits[2].Action, auditActionJobCreate)
	}

	updateBefore := decodeAuditPayloadMap(t, audits[1].BeforeJSON)
	updateAfter := decodeAuditPayloadMap(t, audits[1].AfterJSON)
	if updateBefore["name"] != "job-1" {
		t.Fatalf("update before.name=%v want=%q", updateBefore["name"], "job-1")
	}
	if updateAfter["name"] != "job-1-updated" {
		t.Fatalf("update after.name=%v want=%q", updateAfter["name"], "job-1-updated")
	}
	deleteBefore := decodeAuditPayloadMap(t, audits[0].BeforeJSON)
	if deleteBefore["id"] != created.ID {
		t.Fatalf("delete before.id=%v want=%q", deleteBefore["id"], created.ID)
	}
	if len(audits[0].AfterJSON) != 0 {
		t.Fatalf("delete after_json should be empty, got %s", string(audits[0].AfterJSON))
	}
}

func TestRunStartWritesCompactAuditRecord(t *testing.T) {
	st := openTestStore(t)
	createTestConnection(t, st, db.Connection{
		ID:            "src-run",
		Name:          "src-run",
		Kind:          "source",
		Engine:        "postgres",
		MetadataJSON:  []byte(`{}`),
		SecretEncBlob: []byte(`{}`),
	})
	createTestConnection(t, st, db.Connection{
		ID:            "tgt-run",
		Name:          "tgt-run",
		Kind:          "target",
		Engine:        "s3",
		MetadataJSON:  []byte(`{"prefix":"exports"}`),
		SecretEncBlob: []byte(`{}`),
	})
	if err := st.CreateJob(context.Background(), db.Job{
		ID:                 "job-run",
		Name:               "job-run",
		SourceConnectionID: "src-run",
		TargetConnectionID: "tgt-run",
		TargetTable:        "tbl_run",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{"partition_strategy":"single"}`),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "topsecret")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs/job-run/runs", nil)
	req.Header.Set("Authorization", "Bearer topsecret")
	req.Header.Set("X-Request-ID", "req-run-start")

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	audit := latestAuditRecord(t, st)
	if audit.Action != auditActionJobRunStart {
		t.Fatalf("action=%q want=%q", audit.Action, auditActionJobRunStart)
	}
	if audit.ResourceType != "run" {
		t.Fatalf("resource_type=%q want=%q", audit.ResourceType, "run")
	}
	if strings.TrimSpace(audit.ResourceID) == "" {
		t.Fatalf("expected non-empty run resource id")
	}
	if audit.RequestID != "req-run-start" {
		t.Fatalf("request_id=%q want=%q", audit.RequestID, "req-run-start")
	}
	if len(audit.BeforeJSON) != 0 {
		t.Fatalf("before_json should be empty on run start, got %s", string(audit.BeforeJSON))
	}
	after := decodeAuditPayloadMap(t, audit.AfterJSON)
	if after["status"] != "RUNNING" {
		t.Fatalf("after.status=%v want=%q", after["status"], "RUNNING")
	}
	if _, ok := after["tasks"]; ok {
		t.Fatalf("run start audit after_json should not contain tasks array")
	}
	meta := decodeAuditPayloadMap(t, audit.MetadataJSON)
	if meta["task_count"] != float64(1) {
		t.Fatalf("task_count=%v want=1", meta["task_count"])
	}
}

func latestAuditRecord(t *testing.T, st *db.Store) db.AuditRecord {
	t.Helper()
	audits, err := st.ListAuditRecords(context.Background(), 10)
	if err != nil {
		t.Fatalf("list audit records: %v", err)
	}
	if len(audits) == 0 {
		t.Fatalf("expected at least one audit record")
	}
	return audits[0]
}

func decodeAuditPayloadMap(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode audit payload: %v\npayload=%s", err, string(raw))
	}
	return m
}

func assertNoSecretFields(t *testing.T, payload map[string]any) {
	t.Helper()
	if payload == nil {
		t.Fatalf("expected audit payload")
	}
	if _, ok := payload["secret"]; ok {
		t.Fatalf("unexpected secret field in audit payload")
	}
	if _, ok := payload["secret_enc_blob"]; ok {
		t.Fatalf("unexpected secret_enc_blob field in audit payload")
	}
}
