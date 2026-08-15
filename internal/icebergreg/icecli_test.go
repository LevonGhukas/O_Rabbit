package icebergreg

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"

	"gopkg.in/yaml.v3"
)

func TestBuildBaseIceConfigYAMLFallsBackToResolvedRunConfig(t *testing.T) {
	raw, err := buildBaseIceConfigYAML(RunConfig{
		Enabled:     true,
		Engine:      "ice",
		URI:         "http://catalog:8181",
		BearerToken: "token",
		S3: RunConfigS3{
			Endpoint:        "http://minio:9000",
			Region:          "us-east-1",
			PathStyleAccess: true,
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
	})
	if err != nil {
		t.Fatalf("buildBaseIceConfigYAML: %v", err)
	}

	var cfg map[string]any
	if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("yaml unmarshal: %v", err)
	}
	if cfg["uri"] != "http://catalog:8181" {
		t.Fatalf("uri=%v want http://catalog:8181", cfg["uri"])
	}
	if cfg["bearerToken"] != "token" {
		t.Fatalf("bearerToken=%v want token", cfg["bearerToken"])
	}
}

func TestBuildBaseIceConfigYAMLAppliesRunOverridesToDefaults(t *testing.T) {
	raw, err := buildBaseIceConfigYAML(RunConfig{
		Enabled:     true,
		Engine:      "ice",
		URI:         "http://run-catalog:8181",
		BearerToken: "",
		ConfigYAML:  "uri: http://default-catalog:8181\nbearerToken: default-token\nhttpCacheDir: data/ice/http/cache\n",
	})
	if err != nil {
		t.Fatalf("buildBaseIceConfigYAML: %v", err)
	}
	var cfg map[string]any
	if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("yaml unmarshal: %v", err)
	}
	if cfg["uri"] != "http://run-catalog:8181" {
		t.Fatalf("uri=%v", cfg["uri"])
	}
	if _, ok := cfg["bearerToken"]; ok {
		t.Fatalf("bearerToken should be cleared: %#v", cfg)
	}
	if cfg["httpCacheDir"] != "data/ice/http/cache" {
		t.Fatalf("httpCacheDir=%v", cfg["httpCacheDir"])
	}
}

func TestRunIceCLIRegisterUsesPersistedSnapshot(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args.txt")
	stdinPath := filepath.Join(dir, "stdin.txt")
	envPath := filepath.Join(dir, "env.txt")
	configPath := filepath.Join(dir, "config.yaml")
	scriptPath := filepath.Join(dir, "ice")

	script := `#!/bin/sh
set -eu
cfg=""
printf '%s\n' "$@" > "$CAPTURE_ARGS"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-c" ]; then
    cfg="$2"
    shift 2
    continue
  fi
  shift
done
cp "$cfg" "$CAPTURE_CONFIG"
env | sort > "$CAPTURE_ENV"
cat > "$CAPTURE_STDIN"
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	t.Setenv("CAPTURE_ARGS", argsPath)
	t.Setenv("CAPTURE_STDIN", stdinPath)
	t.Setenv("CAPTURE_ENV", envPath)
	t.Setenv("CAPTURE_CONFIG", configPath)

	req := RunRequest{
		RunID: "run-ice",
		Registration: RunConfig{
			Enabled: true,
			Engine:  "ice",
			Table:   "mssql.orders",
			ConfigYAML: strings.TrimSpace(`
uri: http://catalog:8181
bearerToken: token
httpCacheDir: data/ice/http/cache
`) + "\n",
		},
		DatasetS3: s3io.Config{
			Bucket: "bucket1",
		},
	}
	regS3 := s3io.Config{
		Endpoint:        "http://minio:9000",
		Region:          "us-east-1",
		ForcePathStyle:  true,
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		SessionToken:    "session-token",
	}
	objs := []icebergObj{
		{key: "exports/orders/part-000001.parquet"},
		{key: "exports/orders/part-000002.parquet"},
	}

	if err := runIceCLIRegister(context.Background(), exec.CommandContext, scriptPath, req, req.Registration, "mssql.orders", regS3, objs); err != nil {
		t.Fatalf("runIceCLIRegister: %v", err)
	}

	argsRaw, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read args: %v", err)
	}
	args := strings.Fields(string(argsRaw))
	if len(args) != 6 {
		t.Fatalf("args=%q", string(argsRaw))
	}
	if args[2] != "insert" || args[3] != "mssql.orders" || args[4] != "--force-no-copy" || args[5] != "-" {
		t.Fatalf("unexpected args=%q", string(argsRaw))
	}
	for _, arg := range args {
		if arg == "-p" || arg == "--use-vended-credentials" {
			t.Fatalf("ice insert must avoid auto-create and credential vending args=%q", string(argsRaw))
		}
	}

	stdinRaw, err := os.ReadFile(stdinPath)
	if err != nil {
		t.Fatalf("read stdin: %v", err)
	}
	wantStdin := "" +
		"s3://bucket1/exports/orders/part-000001.parquet\n" +
		"s3://bucket1/exports/orders/part-000002.parquet\n"
	if string(stdinRaw) != wantStdin {
		t.Fatalf("stdin=%q want=%q", string(stdinRaw), wantStdin)
	}

	configRaw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var cfg map[string]any
	if err := yaml.Unmarshal(configRaw, &cfg); err != nil {
		t.Fatalf("yaml unmarshal: %v", err)
	}
	if cfg["uri"] != "http://catalog:8181" {
		t.Fatalf("uri=%v want http://catalog:8181", cfg["uri"])
	}
	if cfg["bearerToken"] != "token" {
		t.Fatalf("bearerToken=%v want token", cfg["bearerToken"])
	}
	if cfg["httpCacheDir"] != "data/ice/http/cache" {
		t.Fatalf("httpCacheDir=%v want data/ice/http/cache", cfg["httpCacheDir"])
	}
	s3Cfg, ok := cfg["s3"].(map[string]any)
	if !ok {
		t.Fatalf("missing s3 config: %#v", cfg["s3"])
	}
	if s3Cfg["endpoint"] != "http://minio:9000" {
		t.Fatalf("s3.endpoint=%v want http://minio:9000", s3Cfg["endpoint"])
	}
	if s3Cfg["region"] != "us-east-1" {
		t.Fatalf("s3.region=%v want us-east-1", s3Cfg["region"])
	}
	if s3Cfg["pathStyleAccess"] != true {
		t.Fatalf("s3.pathStyleAccess=%v want true", s3Cfg["pathStyleAccess"])
	}
	if s3Cfg["accessKeyID"] != "minioadmin" {
		t.Fatalf("s3.accessKeyID=%v want minioadmin", s3Cfg["accessKeyID"])
	}
	if s3Cfg["secretAccessKey"] != "minioadmin" {
		t.Fatalf("s3.secretAccessKey=%v want minioadmin", s3Cfg["secretAccessKey"])
	}
	props, ok := cfg["icebergProperties"].(map[string]any)
	if !ok {
		t.Fatalf("missing icebergProperties: %#v", cfg["icebergProperties"])
	}
	if props["ice.io.default.s3.endpoint"] != "http://minio:9000" {
		t.Fatalf("ice.io.default.s3.endpoint=%v", props["ice.io.default.s3.endpoint"])
	}
	if props["s3.path-style-access"] != "true" {
		t.Fatalf("s3.path-style-access=%v want true", props["s3.path-style-access"])
	}

	envRaw, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("read env: %v", err)
	}
	envText := string(envRaw)
	for _, want := range []string{
		"AWS_ACCESS_KEY_ID=minioadmin",
		"AWS_SECRET_ACCESS_KEY=minioadmin",
		"AWS_REGION=us-east-1",
		"AWS_DEFAULT_REGION=us-east-1",
		"AWS_SESSION_TOKEN=session-token",
		"AWS_EC2_METADATA_DISABLED=true",
		"AWS_ENDPOINT_URL_S3=http://minio:9000",
		"JAVA_TOOL_OPTIONS=-Daws.endpointUrlS3=http://minio:9000",
	} {
		if !strings.Contains(envText, want) {
			t.Fatalf("env missing %q:\n%s", want, envText)
		}
	}
}
