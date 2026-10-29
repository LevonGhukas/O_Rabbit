package icebergreg

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"

	"gopkg.in/yaml.v3"
)

const DefaultIceBinary = "ice"

// iceStderrTailBytes bounds the ice stderr kept for error reporting.
const iceStderrTailBytes = 4096

// iceWaitDelay bounds how long Wait lingers on I/O after ice is killed.
const iceWaitDelay = 5 * time.Second

type iceCommandFactory func(context.Context, string, ...string) *exec.Cmd

func renderIceConfigForInsert(baseConfigYAML string, regS3 s3io.Config) ([]byte, error) {
	baseConfigYAML = strings.TrimSpace(baseConfigYAML)
	if baseConfigYAML == "" {
		return nil, fmt.Errorf("missing persisted ice config content in run registration config")
	}

	var root map[string]any
	if err := yaml.Unmarshal([]byte(baseConfigYAML), &root); err != nil {
		return nil, fmt.Errorf("parse persisted ice config: %w", err)
	}
	if root == nil {
		root = map[string]any{}
	}

	props := map[string]any{}
	if v, ok := root["icebergProperties"]; ok {
		if m, ok := v.(map[string]any); ok && m != nil {
			props = m
		}
	}
	s3Root := map[string]any{}
	if v, ok := root["s3"]; ok {
		if m, ok := v.(map[string]any); ok && m != nil {
			s3Root = m
		}
	}

	if ep := strings.TrimSuffix(strings.TrimSpace(regS3.Endpoint), "/"); ep != "" {
		s3Root["endpoint"] = ep
		props["ice.io.default.s3.endpoint"] = ep
		props["s3.endpoint"] = ep
	}
	if region := strings.TrimSpace(regS3.Region); region != "" {
		s3Root["region"] = region
		props["ice.io.default.client.region"] = region
		props["client.region"] = region
	}
	s3Root["pathStyleAccess"] = regS3.ForcePathStyle
	props["ice.io.default.s3.path-style-access"] = fmt.Sprintf("%t", regS3.ForcePathStyle)
	props["s3.path-style-access"] = fmt.Sprintf("%t", regS3.ForcePathStyle)
	if accessKeyID := strings.TrimSpace(regS3.AccessKeyID); accessKeyID != "" {
		s3Root["accessKeyID"] = accessKeyID
		props["s3.access-key-id"] = accessKeyID
	}
	if secretAccessKey := strings.TrimSpace(regS3.SecretAccessKey); secretAccessKey != "" {
		s3Root["secretAccessKey"] = secretAccessKey
		props["s3.secret-access-key"] = secretAccessKey
	}

	root["s3"] = s3Root
	root["icebergProperties"] = props

	out, err := yaml.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("render persisted ice config: %w", err)
	}
	return out, nil
}

func WriteTempIceConfigForInsert(baseConfigYAML string, regS3 s3io.Config) (string, func(), error) {
	out, err := renderIceConfigForInsert(baseConfigYAML, regS3)
	if err != nil {
		return "", nil, err
	}

	f, err := os.CreateTemp("", "orabbit-master_ice_*.yaml")
	if err != nil {
		return "", nil, err
	}
	tmpPath := f.Name()
	if _, err := f.Write(out); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return "", nil, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", nil, err
	}
	cleanup := func() { _ = os.Remove(tmpPath) }
	return tmpPath, cleanup, nil
}

func buildBaseIceConfigYAML(reg RunConfig) (string, error) {
	root := map[string]any{}
	if strings.TrimSpace(reg.ConfigYAML) != "" {
		if err := yaml.Unmarshal([]byte(reg.ConfigYAML), &root); err != nil {
			return "", fmt.Errorf("parse persisted ice config: %w", err)
		}
		if root == nil {
			root = map[string]any{}
		}
	}
	hasResolvedCatalog := strings.TrimSpace(reg.URI) != ""
	uri := strings.TrimSpace(reg.URI)
	if uri == "" {
		if persistedURI, ok := root["uri"].(string); ok {
			uri = strings.TrimSpace(persistedURI)
		}
	}
	if uri == "" {
		return "", fmt.Errorf("missing persisted ice config content and missing iceberg rest uri in run registration config")
	}
	root["uri"] = uri
	if token := strings.TrimSpace(reg.BearerToken); token != "" {
		root["bearerToken"] = token
	} else if hasResolvedCatalog {
		delete(root, "bearerToken")
	}

	if _, ok := root["s3"]; !ok {
		root["s3"] = map[string]any{}
	}

	out, err := yaml.Marshal(root)
	if err != nil {
		return "", fmt.Errorf("render fallback ice config: %w", err)
	}
	return string(out), nil
}

func buildIceCLIEnv(base []string, regS3 s3io.Config) []string {
	filtered := append([]string{}, base...)
	filter := func(in []string, key string) []string {
		prefix := key + "="
		out := in[:0]
		for _, kv := range in {
			if strings.HasPrefix(kv, prefix) {
				continue
			}
			out = append(out, kv)
		}
		return out
	}
	for _, key := range []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_REGION",
		"AWS_DEFAULT_REGION",
		"AWS_PROFILE",
		"AWS_DEFAULT_PROFILE",
		"AWS_SESSION_TOKEN",
		"AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_ROLE_ARN",
		"AWS_ENDPOINT_URL",
		"AWS_ENDPOINT_URL_S3",
		"JAVA_TOOL_OPTIONS",
	} {
		filtered = filter(filtered, key)
	}

	filtered = append(filtered,
		"AWS_ACCESS_KEY_ID="+regS3.AccessKeyID,
		"AWS_SECRET_ACCESS_KEY="+regS3.SecretAccessKey,
		"AWS_REGION="+regS3.Region,
		"AWS_DEFAULT_REGION="+regS3.Region,
		"AWS_EC2_METADATA_DISABLED=true",
	)
	if token := strings.TrimSpace(regS3.SessionToken); token != "" {
		filtered = append(filtered, "AWS_SESSION_TOKEN="+token)
	}
	if ep := strings.TrimSuffix(strings.TrimSpace(regS3.Endpoint), "/"); ep != "" {
		ep = normalizeLocalhost(ep)
		filtered = append(filtered,
			"AWS_ENDPOINT_URL_S3="+ep,
			"JAVA_TOOL_OPTIONS=-Daws.endpointUrlS3="+ep,
		)
	}
	return filtered
}

func runIceCLIRegister(ctx context.Context, cmdFactory iceCommandFactory, iceBinary string, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, objs []icebergObj) error {
	if strings.TrimSpace(iceBinary) == "" {
		iceBinary = DefaultIceBinary
	}

	baseConfigYAML, err := buildBaseIceConfigYAML(reg)
	if err != nil {
		return err
	}

	iceCfgPath, cleanup, err := WriteTempIceConfigForInsert(baseConfigYAML, regS3)
	if err != nil {
		return err
	}
	defer cleanup()

	// Do not pass -p here. Altinity Ice's auto-create path reads the first S3
	// object with a raw AWS SDK client that cannot be forced path-style without
	// credential vending; the manager creates/verifies the table before insert.
	args := []string{
		"-c", iceCfgPath,
		"insert", table,
		"--force-no-copy",
		"-",
	}

	var stdin strings.Builder
	for _, obj := range objs {
		fmt.Fprintf(&stdin, "s3://%s/%s\n", req.DatasetS3.Bucket, obj.key)
	}

	cmd := cmdFactory(ctx, iceBinary, args...)
	stderrTail := newTailBuffer(iceStderrTailBytes)
	cmd.Stdout = os.Stdout
	cmd.Stderr = io.MultiWriter(os.Stderr, stderrTail)
	cmd.Stdin = strings.NewReader(stdin.String())
	cmd.Env = buildIceCLIEnv(os.Environ(), regS3)
	// A killed JVM can leave grandchildren holding the stderr pipe open; do not
	// let Wait block on them.
	cmd.WaitDelay = iceWaitDelay
	killProcessGroupOnCancel(cmd)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start ice insert for table %s using %q: %w", table, iceBinary, err)
	}
	if err := cmd.Wait(); err != nil {
		detail := stderrTail.String(regS3.SecretAccessKey, regS3.AccessKeyID, regS3.SessionToken, reg.BearerToken)
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = fmt.Errorf("%w: %w", ctxErr, err)
		}
		if detail == "" {
			return fmt.Errorf("ice insert failed for table %s: %w", table, err)
		}
		return fmt.Errorf("ice insert failed for table %s: %w: %s", table, err, detail)
	}
	return nil
}
