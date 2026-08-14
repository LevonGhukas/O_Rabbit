package db

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	secretcrypto "github.com/LevonGhukas/O_Rabbit/internal/crypto"
)

func testMasterKey(t *testing.T) secretcrypto.Key {
	t.Helper()
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	prev, hadPrev := os.LookupEnv("ORABBIT_MASTER_KEY")
	if err := os.Setenv("ORABBIT_MASTER_KEY", base64.StdEncoding.EncodeToString(raw)); err != nil {
		t.Fatalf("set ORABBIT_MASTER_KEY: %v", err)
	}
	t.Cleanup(func() {
		if hadPrev {
			_ = os.Setenv("ORABBIT_MASTER_KEY", prev)
			return
		}
		_ = os.Unsetenv("ORABBIT_MASTER_KEY")
	})
	k, err := secretcrypto.LoadMasterKeyFromEnv()
	if err != nil {
		t.Fatalf("load master key: %v", err)
	}
	return k
}

func TestServerCRUDAndConnectionResult(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	created, err := st.CreateServer(ctx, Server{
		Name:       "vps-1",
		Host:       "10.0.0.10",
		SSHUser:    "deploy",
		ProjectDir: "/opt/orabbit",
	})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	if created.SSHPort != DefaultSSHPort {
		t.Fatalf("ssh_port=%d want %d", created.SSHPort, DefaultSSHPort)
	}

	got, err := st.GetServer(ctx, created.ID)
	if err != nil {
		t.Fatalf("get server: %v", err)
	}
	if got.Name != "vps-1" {
		t.Fatalf("name=%q want vps-1", got.Name)
	}

	updated, err := st.UpdateServer(ctx, got, Server{
		Name:          "vps-1-renamed",
		Host:          got.Host,
		SSHPort:       2222,
		SSHUser:       got.SSHUser,
		ProjectDir:    "/srv/orabbit",
		RoleHintsJSON: json.RawMessage(`["master","worker"]`),
		LabelsJSON:    json.RawMessage(`{"region":"eu-central"}`),
	})
	if err != nil {
		t.Fatalf("update server: %v", err)
	}
	if updated.SSHPort != 2222 {
		t.Fatalf("updated ssh_port=%d want 2222", updated.SSHPort)
	}

	lastSeen := nowUTC()
	lastErr := "timeout"
	if err := st.UpdateServerConnectionResult(ctx, created.ID, ServerConnectionResult{
		LastSeenAt: &lastSeen,
		LastError:  &lastErr,
	}); err != nil {
		t.Fatalf("update connection result: %v", err)
	}

	got, err = st.GetServer(ctx, created.ID)
	if err != nil {
		t.Fatalf("get updated server: %v", err)
	}
	if got.LastSeenAt == nil || *got.LastSeenAt != lastSeen {
		t.Fatalf("last_seen_at=%v want %s", got.LastSeenAt, lastSeen)
	}
	if got.LastError == nil || *got.LastError != lastErr {
		t.Fatalf("last_error=%v want %s", got.LastError, lastErr)
	}

	servers, err := st.ListServers(ctx)
	if err != nil {
		t.Fatalf("list servers: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("server count=%d want 1", len(servers))
	}

	if err := st.DeleteServer(ctx, created.ID); err != nil {
		t.Fatalf("delete server: %v", err)
	}
	if _, err := st.GetServer(ctx, created.ID); err == nil {
		t.Fatal("expected deleted server lookup to fail")
	}
}

func TestServerCredentialEncryptionRequiresMasterKey(t *testing.T) {
	if _, err := EncryptServerCredential(secretcrypto.Key{}, "server-1", ServerCredentialSecret{
		AuthType: serverAuthTypePassword,
		Username: "deploy",
		Password: "secret",
	}); err == nil {
		t.Fatal("expected missing master key error")
	}
}

func TestServerCredentialRoundTripAndCRUD(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	k := testMasterKey(t)

	server, err := st.CreateServer(ctx, Server{
		Name:       "vps-cred",
		Host:       "10.0.0.11",
		SSHUser:    "deploy",
		ProjectDir: "/opt/orabbit",
	})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	cred, err := EncryptServerCredential(k, server.ID, ServerCredentialSecret{
		AuthType:           serverAuthTypePassword,
		Username:           "deploy",
		Password:           "pw-123",
		HostKeyFingerprint: "SHA256:abc",
	})
	if err != nil {
		t.Fatalf("encrypt credential: %v", err)
	}
	created, err := st.CreateServerCredential(ctx, cred)
	if err != nil {
		t.Fatalf("create credential: %v", err)
	}

	got, err := st.GetServerCredential(ctx, server.ID)
	if err != nil {
		t.Fatalf("get credential: %v", err)
	}
	plain, err := DecryptServerCredential(k, got)
	if err != nil {
		t.Fatalf("decrypt credential: %v", err)
	}
	if plain.Password != "pw-123" {
		t.Fatalf("password=%q want pw-123", plain.Password)
	}

	replacement, err := EncryptServerCredential(k, server.ID, ServerCredentialSecret{
		AuthType:           serverAuthTypePrivateKey,
		Username:           "deploy",
		PrivateKey:         "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
		Passphrase:         "passphrase",
		HostKeyFingerprint: "SHA256:def",
	})
	if err != nil {
		t.Fatalf("encrypt replacement credential: %v", err)
	}
	replacement.ID = created.ID
	updated, err := st.UpdateServerCredential(ctx, created, replacement)
	if err != nil {
		t.Fatalf("update credential: %v", err)
	}
	if updated.AuthType != serverAuthTypePrivateKey {
		t.Fatalf("updated auth_type=%q want private_key", updated.AuthType)
	}

	got, err = st.GetServerCredential(ctx, server.ID)
	if err != nil {
		t.Fatalf("get updated credential: %v", err)
	}
	plain, err = DecryptServerCredential(k, got)
	if err != nil {
		t.Fatalf("decrypt updated credential: %v", err)
	}
	if plain.PrivateKey == "" || plain.Passphrase != "passphrase" {
		t.Fatalf("unexpected decrypted private key credential: %+v", plain)
	}

	if err := st.DeleteServerCredential(ctx, server.ID); err != nil {
		t.Fatalf("delete credential: %v", err)
	}
	if _, err := st.GetServerCredential(ctx, server.ID); err == nil {
		t.Fatal("expected deleted credential lookup to fail")
	}
}

func TestCommandExecutionLifecycle(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	server, err := st.CreateServer(ctx, Server{
		Name:       "vps-exec",
		Host:       "10.0.0.12",
		SSHUser:    "deploy",
		ProjectDir: "/opt/orabbit",
	})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	execRec, err := st.CreateCommandExecution(ctx, CommandExecution{
		ServerID:    server.ID,
		Kind:        "deploy",
		AllowlistID: "deploy-master",
		ParamsJSON:  json.RawMessage(`{"component":"master"}`),
		RequestedBy: "local-admin",
	})
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}
	if execRec.Status != "PENDING" {
		t.Fatalf("status=%q want PENDING", execRec.Status)
	}

	if err := st.UpdateCommandExecutionStatus(ctx, execRec.ID, CommandExecutionUpdate{Status: "RUNNING"}); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := st.AppendCommandExecutionOutput(ctx, execRec.ID, "stdout line 1\n", "stderr line 1\n"); err != nil {
		t.Fatalf("append output: %v", err)
	}
	exitCode := 0
	if err := st.CompleteCommandExecution(ctx, execRec.ID, "SUCCEEDED", exitCode, "stdout line 1\nstdout line 2\n", "stderr line 1\n", nil); err != nil {
		t.Fatalf("complete execution: %v", err)
	}

	got, err := st.GetCommandExecution(ctx, execRec.ID)
	if err != nil {
		t.Fatalf("get execution: %v", err)
	}
	if got.Status != "SUCCEEDED" {
		t.Fatalf("status=%q want SUCCEEDED", got.Status)
	}
	if got.ExitCode == nil || *got.ExitCode != 0 {
		t.Fatalf("exit_code=%v want 0", got.ExitCode)
	}
	if got.StartedAt == nil || got.FinishedAt == nil {
		t.Fatalf("expected started_at and finished_at to be set: %+v", got)
	}
	if got.StdoutTail == "" || got.StderrTail == "" {
		t.Fatalf("expected stdout/stderr tails to be captured: %+v", got)
	}

	execs, err := st.ListCommandExecutionsByServer(ctx, server.ID, 10)
	if err != nil {
		t.Fatalf("list executions: %v", err)
	}
	if len(execs) != 1 {
		t.Fatalf("execution count=%d want 1", len(execs))
	}
}

func TestDeploymentAndConfigVersionPersistence(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	k := testMasterKey(t)

	server, err := st.CreateServer(ctx, Server{
		Name:       "vps-deploy",
		Host:       "10.0.0.13",
		SSHUser:    "deploy",
		ProjectDir: "/opt/orabbit",
	})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	dep, err := st.CreateDeployment(ctx, Deployment{
		ServerID:  server.ID,
		Component: "master",
		ScriptID:  "deploy-master.sh",
		Status:    "PENDING",
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	executionID := "exec-123"
	if err := st.UpdateDeploymentStatus(ctx, dep.ID, DeploymentUpdate{
		Status:      "RUNNING",
		ExecutionID: &executionID,
	}); err != nil {
		t.Fatalf("mark deployment running: %v", err)
	}
	if err := st.UpdateDeploymentStatus(ctx, dep.ID, DeploymentUpdate{Status: "SUCCEEDED"}); err != nil {
		t.Fatalf("mark deployment succeeded: %v", err)
	}

	gotDep, err := st.GetDeployment(ctx, dep.ID)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if gotDep.ExecutionID != executionID || gotDep.Status != "SUCCEEDED" {
		t.Fatalf("unexpected deployment state: %+v", gotDep)
	}

	deps, err := st.ListDeployments(ctx, server.ID, 10)
	if err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deps) != 1 {
		t.Fatalf("deployment count=%d want 1", len(deps))
	}

	content1, err := EncryptConfigVersionContent(k, "server:"+server.ID+":config:master-env:v1", []byte("MASTER_HTTP_ADDR=:9100"))
	if err != nil {
		t.Fatalf("encrypt config v1: %v", err)
	}
	cfg1, err := st.SaveConfigVersion(ctx, ConfigVersion{
		ServerID:             server.ID,
		ConfigID:             "master-env",
		ContentEnc:           content1,
		ValidationStatus:     "valid",
		ValidationErrorsJSON: json.RawMessage(`[]`),
	})
	if err != nil {
		t.Fatalf("save config v1: %v", err)
	}
	if cfg1.Version != 1 {
		t.Fatalf("config version=%d want 1", cfg1.Version)
	}

	content2, err := EncryptConfigVersionContent(k, "server:"+server.ID+":config:master-env:v2", []byte("MASTER_HTTP_ADDR=:9200"))
	if err != nil {
		t.Fatalf("encrypt config v2: %v", err)
	}
	cfg2, err := st.SaveConfigVersion(ctx, ConfigVersion{
		ServerID:             server.ID,
		ConfigID:             "master-env",
		ContentEnc:           content2,
		ValidationStatus:     "valid",
		ValidationErrorsJSON: json.RawMessage(`[]`),
	})
	if err != nil {
		t.Fatalf("save config v2: %v", err)
	}
	if cfg2.Version != 2 {
		t.Fatalf("config version=%d want 2", cfg2.Version)
	}

	latest, err := st.GetLatestConfigVersion(ctx, server.ID, "master-env")
	if err != nil {
		t.Fatalf("get latest config version: %v", err)
	}
	if latest.Version != 2 {
		t.Fatalf("latest version=%d want 2", latest.Version)
	}

	versions, err := st.ListConfigVersions(ctx, server.ID, "master-env", 10)
	if err != nil {
		t.Fatalf("list config versions: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("config version count=%d want 2", len(versions))
	}
}
