package configs

import (
	"strings"
	"testing"
)

func TestValidateConfigRejectsMalformedEnvLine(t *testing.T) {
	result := ValidateConfig("master-env", "ORABBIT_DB_PATH=/tmp/master.sqlite\nBADLINE\n")
	if result.OK {
		t.Fatal("expected validation failure")
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected validation errors")
	}
}

func TestValidateConfigAcceptsWellFormedWorkerEnv(t *testing.T) {
	result := ValidateConfig("worker-env", strings.Join([]string{
		"ORABBIT_MASTER_GRPC_ADDR=1.2.3.4:9102",
		"ORABBIT_GRPC_INSECURE=true",
		"ORABBIT_WORKER_AUTH_TOKEN=worker-secret",
		"ORABBIT_WORKER_ID=worker-1",
		"ORABBIT_WORKER_ADVERTISE_ADDR=1.2.3.5",
		"ORABBIT_WORKER_POLL=2s",
		"ORABBIT_LOG_LEVEL=INFO",
		"ORABBIT_LOG_FORMAT=json",
		"AWS_EC2_METADATA_DISABLED=true",
	}, "\n"))
	if !result.OK {
		t.Fatalf("expected valid worker env, got errors=%v warnings=%v", result.Errors, result.Warnings)
	}
}

func TestMaskConfigContentMasksSecrets(t *testing.T) {
	masked := MaskConfigContent("minio-env", "MINIO_ROOT_USER=admin\nMINIO_ROOT_PASSWORD=secret\nAWS_SECRET_ACCESS_KEY=abc\n")
	if masked == "" {
		t.Fatal("expected masked content")
	}
	if contains(masked, "secret") || contains(masked, "abc") {
		t.Fatalf("secret leaked in masked config: %q", masked)
	}
	if !contains(masked, "MINIO_ROOT_PASSWORD=********") {
		t.Fatalf("password not masked: %q", masked)
	}
}

func TestValidateConfigIDAllowlist(t *testing.T) {
	if _, err := ValidateConfigID("unknown-config"); err == nil {
		t.Fatal("expected unsupported config_id error")
	}
}

func TestResolveConfigPathUsesAllowlist(t *testing.T) {
	def, resolved, err := ResolveConfigPath("/root/O_Rabbit", "worker-env")
	if err != nil {
		t.Fatalf("ResolveConfigPath: %v", err)
	}
	if def.RelativePath != ".env.worker" {
		t.Fatalf("relative path=%q want .env.worker", def.RelativePath)
	}
	if resolved != "/root/O_Rabbit/.env.worker" {
		t.Fatalf("resolved path=%q", resolved)
	}
}

func contains(s string, want string) bool {
	return strings.Contains(s, want)
}
