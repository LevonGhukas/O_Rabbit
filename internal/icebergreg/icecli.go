package icebergreg

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"

	"gopkg.in/yaml.v3"
)

const DefaultIceBinary = "ice"

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
	if strings.TrimSpace(reg.ConfigYAML) != "" {
		return reg.ConfigYAML, nil
	}

	if strings.TrimSpace(reg.URI) == "" {
		return "", fmt.Errorf("missing persisted ice config content and missing iceberg rest uri in run registration config")
	}

	root := map[string]any{
		"uri": strings.TrimSpace(reg.URI),
	}
	if token := strings.TrimSpace(reg.BearerToken); token != "" {
		root["bearerToken"] = token
	}

	s3Cfg := map[string]any{}
	if endpoint := strings.TrimSpace(reg.S3.Endpoint); endpoint != "" {
		s3Cfg["endpoint"] = endpoint
	}
	if region := strings.TrimSpace(reg.S3.Region); region != "" {
		s3Cfg["region"] = region
	}
	s3Cfg["pathStyleAccess"] = reg.S3.PathStyleAccess
	if accessKeyID := strings.TrimSpace(reg.S3.AccessKeyID); accessKeyID != "" {
		s3Cfg["accessKeyID"] = accessKeyID
	}
	if secretAccessKey := strings.TrimSpace(reg.S3.SecretAccessKey); secretAccessKey != "" {
		s3Cfg["secretAccessKey"] = secretAccessKey
	}
	root["s3"] = s3Cfg

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

	r, w := io.Pipe()
	defer r.Close()

	// Do not pass -p here. Altinity Ice's auto-create path reads the first S3
	// object with a raw AWS SDK client that cannot be forced path-style without
	// credential vending; the manager creates/verifies the table before insert.
	args := []string{
		"-c", iceCfgPath,
		"insert", table,
		"--force-no-copy",
		"-",
	}

	cmd := cmdFactory(ctx, iceBinary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = r
	cmd.Env = buildIceCLIEnv(os.Environ(), regS3)

	errCh := make(chan error, 1)
	go func() {
		defer w.Close()
		for _, obj := range objs {
			uri := fmt.Sprintf("s3://%s/%s\n", req.DatasetS3.Bucket, obj.key)
			if _, err := io.WriteString(w, uri); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start ice insert for table %s using %q: %w", table, iceBinary, err)
	}
	if werr := <-errCh; werr != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("stream parquet objects to ice insert for table %s: %w", table, werr)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("ice insert failed for table %s: %w", table, err)
	}
	return nil
}
