package icebergreg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		{key: "exports/orders/part-000001-000.parquet"},
		{key: "exports/orders/part-000002-000.parquet"},
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
		"s3://bucket1/exports/orders/part-000001-000.parquet\n" +
		"s3://bucket1/exports/orders/part-000002-000.parquet\n"
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

const iceTestReturnTimeout = 10 * time.Second

func runFakeIce(t *testing.T, ctx context.Context, script string, regS3 s3io.Config, reg RunConfig, objs []icebergObj) error {
	t.Helper()
	scriptPath := filepath.Join(t.TempDir(), "ice")
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	req := RunRequest{RunID: "run-ice", Registration: reg, DatasetS3: s3io.Config{Bucket: "bucket1"}}
	done := make(chan error, 1)
	go func() {
		done <- runIceCLIRegister(ctx, exec.CommandContext, scriptPath, req, reg, "mssql.orders", regS3, objs)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(iceTestReturnTimeout):
		t.Fatal("runIceCLIRegister hung")
		return nil
	}
}

func iceTestRunConfig() RunConfig {
	return RunConfig{Enabled: true, Engine: "ice", Table: "mssql.orders", BearerToken: "bearer-secret-value", ConfigYAML: "uri: http://catalog:8181\n"}
}

func TestRunIceCLIRegisterReturnsStderrWhenIceExitsWithoutReadingStdin(t *testing.T) {
	objs := make([]icebergObj, 4000)
	for i := range objs {
		objs[i] = icebergObj{key: fmt.Sprintf("exports/orders/part-%06d-000-%s.parquet", i, strings.Repeat("x", 40))}
	}
	script := "#!/bin/sh\necho 'catalog says no: boom' >&2\nexit 1\n"
	regS3 := s3io.Config{AccessKeyID: "AKIATESTKEY", SecretAccessKey: "super-secret-key", Region: "us-east-1"}

	err := runFakeIce(t, context.Background(), script, regS3, iceTestRunConfig(), objs)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "catalog says no: boom") {
		t.Fatalf("error lacks ice stderr: %v", err)
	}
}

func TestRunIceCLIRegisterRedactsSecretsFromStderrTail(t *testing.T) {
	script := "#!/bin/sh\necho \"leak $AWS_SECRET_ACCESS_KEY $AWS_ACCESS_KEY_ID bearer-secret-value\" >&2\nexit 1\n"
	regS3 := s3io.Config{AccessKeyID: "AKIATESTKEY", SecretAccessKey: "super-secret-key", Region: "us-east-1"}

	err := runFakeIce(t, context.Background(), script, regS3, iceTestRunConfig(), []icebergObj{{key: "a.parquet"}})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, secret := range []string{"super-secret-key", "AKIATESTKEY", "bearer-secret-value"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, err)
		}
	}
	if !strings.Contains(err.Error(), "leak") {
		t.Fatalf("error lacks stderr tail: %v", err)
	}
}

func TestRunIceCLIRegisterBoundsStderrTail(t *testing.T) {
	script := "#!/bin/sh\nhead -c 100000 /dev/zero | tr '\\0' 'a' >&2\necho ' END-MARKER' >&2\nexit 1\n"
	err := runFakeIce(t, context.Background(), script, s3io.Config{Region: "us-east-1"}, iceTestRunConfig(), []icebergObj{{key: "a.parquet"}})
	if err == nil || !strings.Contains(err.Error(), "END-MARKER") {
		t.Fatalf("want tail with marker, got %v", err)
	}
	if len(err.Error()) > 2*iceStderrTailBytes {
		t.Fatalf("error not bounded: %d bytes", len(err.Error()))
	}
}

func TestRunIceCLIRegisterHonoursContextDeadlineWhenIceHangs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	script := "#!/bin/sh\nexec sleep 600\n"

	err := runFakeIce(t, ctx, script, s3io.Config{Region: "us-east-1"}, iceTestRunConfig(), []icebergObj{{key: "a.parquet"}})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error should wrap context.DeadlineExceeded for classification: %v", err)
	}
}
