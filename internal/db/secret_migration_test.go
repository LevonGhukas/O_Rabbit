package db

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMigrateLegacySecretsSealsRegistrationConfigsAndOverrides(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	reg := insertRegistrationFixture(t, st, "run-legacy", "dataset", 1, RegistrationFailed)
	legacyConfig := `{"enabled":true,"s3":{"secret_access_key":"legacy-secret"}}`
	if _, err := st.db.Exec(`UPDATE runs SET registration_config_json=? WHERE id=?`, legacyConfig, reg.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`UPDATE iceberg_registrations SET retry_override_config_json=? WHERE id=?`, `{"bearer_token":"legacy-token"}`, reg.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := st.GetRun(ctx, reg.RunID); err == nil || !strings.Contains(err.Error(), "unencrypted") {
		t.Fatalf("plaintext registration config must be refused before migration, got %v", err)
	}

	if err := st.MigrateLegacySecrets(ctx, st.masterKey); err != nil {
		t.Fatal(err)
	}
	if err := st.MigrateLegacySecrets(ctx, st.masterKey); err != nil {
		t.Fatalf("migration must be idempotent: %v", err)
	}

	assertNoPlaintext(t, st, `SELECT registration_config_json FROM runs WHERE id=?`, reg.RunID, "legacy-secret")
	assertNoPlaintext(t, st, `SELECT retry_override_config_json FROM iceberg_registrations WHERE id=?`, reg.ID, "legacy-token")

	run, err := st.GetRun(ctx, reg.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if string(run.RegistrationConfigJSON) != legacyConfig {
		t.Fatalf("registration config=%s", run.RegistrationConfigJSON)
	}
	got, err := st.GetRegistrationForRun(ctx, reg.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.RetryOverrideConfigJSON) != `{"bearer_token":"legacy-token"}` {
		t.Fatalf("retry override=%s", got.RetryOverrideConfigJSON)
	}
}

func TestRegistrationRetryOverrideIsEncryptedAtRest(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	reg := insertRegistrationFixture(t, st, "run-override", "dataset", 1, RegistrationFailed)
	if _, err := st.db.Exec(`UPDATE runs SET status='SUCCEEDED', commit_phase='COMPLETE' WHERE id=?`, reg.RunID); err != nil {
		t.Fatal(err)
	}
	override, _ := json.Marshal(map[string]any{"s3": map[string]any{"secret_access_key": "override-secret"}})

	queued, ok, err := st.RequeueRegistrationManual(ctx, reg.RunID, override, time.Now())
	if err != nil || !ok {
		t.Fatalf("requeue ok=%v err=%v", ok, err)
	}
	if string(queued.RetryOverrideConfigJSON) != string(override) {
		t.Fatalf("returned override=%s", queued.RetryOverrideConfigJSON)
	}
	assertNoPlaintext(t, st, `SELECT retry_override_config_json FROM iceberg_registrations WHERE id=?`, reg.ID, "override-secret")

	got, err := st.GetRegistrationForRun(ctx, reg.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.RetryOverrideConfigJSON) != string(override) {
		t.Fatalf("stored override=%s", got.RetryOverrideConfigJSON)
	}
}

func assertNoPlaintext(t *testing.T, st *Store, query, id, secret string) {
	t.Helper()
	var stored string
	if err := st.db.QueryRow(query, id).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stored, encryptedRegistrationConfigPrefix) || strings.Contains(stored, secret) {
		t.Fatalf("value is not sealed at rest: %q", stored)
	}
}
