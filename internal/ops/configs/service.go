package configs

import (
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	opscommon "github.com/LevonGhukas/O_Rabbit/internal/ops/common"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

type CommandExecutor interface {
	ExecuteCommand(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error)
}

type CommandExecutorFunc func(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error)

func (f CommandExecutorFunc) ExecuteCommand(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error) {
	return f(ctx, target, command, onStream)
}

type Service struct {
	exec    CommandExecutor
	timeout time.Duration
}

type Definition struct {
	ID           string `json:"id"`
	RelativePath string `json:"relative_path"`
	Format       string `json:"format"`
	Description  string `json:"description"`
}

type ListedConfig struct {
	ID           string `json:"id"`
	Path         string `json:"path"`
	RelativePath string `json:"relative_path"`
	Format       string `json:"format"`
	Exists       bool   `json:"exists"`
}

type ReadResult struct {
	ConfigID string `json:"config_id"`
	Path     string `json:"path"`
	Format   string `json:"format"`
	Content  string `json:"content"`
}

type ValidationResult struct {
	OK       bool     `json:"ok"`
	Errors   []string `json:"errors"`
	Warnings []string `json:"warnings"`
}

type envValidationRule struct {
	requiredKeys []string
	knownKeys    map[string]struct{}
}

var (
	secretKeyPattern = regexp.MustCompile(`(?i)(password|secret|token|key|access|private)`)
	definitions      = []Definition{
		{ID: "master-env", RelativePath: ".env.master", Format: "env", Description: "Master environment"},
		{ID: "worker-env", RelativePath: ".env.worker", Format: "env", Description: "Worker environment"},
		{ID: "minio-env", RelativePath: ".env.minio", Format: "env", Description: "MinIO environment"},
		{ID: "postgres-env", RelativePath: ".env.postgres", Format: "env", Description: "PostgreSQL environment"},
		{ID: "ice-rest-catalog-yaml", RelativePath: "ice-rest-catalog.yaml", Format: "yaml", Description: "Iceberg REST catalog config"},
	}
	definitionByID = func() map[string]Definition {
		out := make(map[string]Definition, len(definitions))
		for _, def := range definitions {
			out[def.ID] = def
		}
		return out
	}()
	envRules = map[string]envValidationRule{
		"master-env": {
			requiredKeys: []string{
				"ORABBIT_DB_PATH",
				"ORABBIT_HTTP_ADDR",
				"ORABBIT_GRPC_ADDR",
				"ORABBIT_GRPC_INSECURE",
				"ORABBIT_HTTP_AUTH_TOKEN",
				"ORABBIT_WORKER_AUTH_TOKEN",
				"ORABBIT_MASTER_KEY",
				"ORABBIT_LOG_LEVEL",
				"ORABBIT_LOG_FORMAT",
				"ORABBIT_HTTP_PUBLISHED_PORT",
				"AWS_EC2_METADATA_DISABLED",
			},
		},
		"worker-env": {
			requiredKeys: []string{
				"ORABBIT_MASTER_GRPC_ADDR",
				"ORABBIT_GRPC_INSECURE",
				"ORABBIT_WORKER_AUTH_TOKEN",
				"ORABBIT_WORKER_ID",
				"ORABBIT_WORKER_ADVERTISE_ADDR",
				"ORABBIT_WORKER_POLL",
				"ORABBIT_LOG_LEVEL",
				"ORABBIT_LOG_FORMAT",
				"AWS_EC2_METADATA_DISABLED",
			},
		},
		"minio-env": {
			requiredKeys: []string{
				"MINIO_IMAGE",
				"MINIO_MC_IMAGE",
				"MINIO_ROOT_USER",
				"MINIO_ROOT_PASSWORD",
				"ORABBIT_S3_BUCKET",
				"MINIO_API_PUBLISHED_PORT",
				"MINIO_CONSOLE_PUBLISHED_PORT",
			},
		},
		"postgres-env": {
			requiredKeys: nil,
		},
	}
)

func init() {
	for id, rule := range envRules {
		known := make(map[string]struct{}, len(rule.requiredKeys))
		for _, key := range rule.requiredKeys {
			known[key] = struct{}{}
		}
		rule.knownKeys = known
		envRules[id] = rule
	}
}

func NewService(exec CommandExecutor, timeout time.Duration) *Service {
	if exec == nil {
		exec = CommandExecutorFunc(sshops.ExecuteCommand)
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Service{exec: exec, timeout: timeout}
}

func Definitions() []Definition {
	out := make([]Definition, len(definitions))
	copy(out, definitions)
	return out
}

func ValidateConfigID(configID string) (Definition, error) {
	def, ok := definitionByID[strings.TrimSpace(configID)]
	if !ok {
		return Definition{}, fmt.Errorf("unsupported config_id %q", configID)
	}
	return def, nil
}

func ResolveConfigPath(projectDir string, configID string) (Definition, string, error) {
	def, err := ValidateConfigID(configID)
	if err != nil {
		return Definition{}, "", err
	}
	resolved, err := opscommon.ResolveUnderProject(projectDir, def.RelativePath)
	if err != nil {
		return Definition{}, "", err
	}
	return def, resolved, nil
}

func (s *Service) ListEditableConfigs(ctx context.Context, target sshops.SSHTarget, projectDir string) ([]ListedConfig, error) {
	items := Definitions()
	paths := make([]string, 0, len(items))
	for _, item := range items {
		resolved, err := opscommon.ResolveUnderProject(projectDir, item.RelativePath)
		if err != nil {
			return nil, err
		}
		paths = append(paths, resolved)
	}

	lines := make([]string, 0, len(items))
	for idx, item := range items {
		lines = append(lines, fmt.Sprintf("if [ -f %s ]; then echo %s; else echo %s; fi",
			opscommon.ShellQuote(paths[idx]),
			opscommon.ShellQuote(item.ID+"=1"),
			opscommon.ShellQuote(item.ID+"=0"),
		))
	}
	command := "sh -lc " + opscommon.ShellQuote(strings.Join(append([]string{"set -eu"}, lines...), "; "))
	result, err := s.execWithTimeout(ctx, target, command)
	if err != nil {
		return nil, fmt.Errorf("list editable configs: %w", err)
	}
	if result.ExitCode != 0 {
		return nil, fmt.Errorf("list editable configs failed with exit code %d: %s", result.ExitCode, firstNonEmpty(result.StderrTail, result.StdoutTail))
	}
	existsByID := make(map[string]bool, len(items))
	for _, line := range splitNonEmptyLines(result.StdoutTail) {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		existsByID[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1]) == "1"
	}

	out := make([]ListedConfig, 0, len(items))
	for idx, item := range items {
		out = append(out, ListedConfig{
			ID:           item.ID,
			Path:         paths[idx],
			RelativePath: item.RelativePath,
			Format:       item.Format,
			Exists:       existsByID[item.ID],
		})
	}
	return out, nil
}

func (s *Service) ReadConfig(ctx context.Context, target sshops.SSHTarget, projectDir string, configID string) (ReadResult, error) {
	def, resolved, err := ResolveConfigPath(projectDir, configID)
	if err != nil {
		return ReadResult{}, err
	}
	command := "cat " + opscommon.ShellQuote(resolved)
	result, err := s.execWithTimeout(ctx, target, command)
	if err != nil {
		return ReadResult{}, fmt.Errorf("read config %s: %w", configID, err)
	}
	if result.ExitCode != 0 {
		return ReadResult{}, fmt.Errorf("read config %s failed with exit code %d: %s", configID, result.ExitCode, firstNonEmpty(result.StderrTail, result.StdoutTail))
	}
	return ReadResult{
		ConfigID: def.ID,
		Path:     resolved,
		Format:   def.Format,
		Content:  MaskConfigContent(def.ID, result.StdoutTail),
	}, nil
}

func (s *Service) UpdateConfig(ctx context.Context, target sshops.SSHTarget, projectDir string, configID string, content string) (ValidationResult, error) {
	def, resolved, err := ResolveConfigPath(projectDir, configID)
	if err != nil {
		return ValidationResult{}, err
	}
	validation := ValidateConfig(def.ID, content)
	if !validation.OK {
		return validation, nil
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(content))
	tmpPath := path.Clean(resolved + ".orabbit-tmp")
	script := strings.Join([]string{
		"set -eu",
		fmt.Sprintf("tmp=%s", opscommon.ShellQuote(tmpPath)),
		fmt.Sprintf("target=%s", opscommon.ShellQuote(resolved)),
		fmt.Sprintf("printf %%s %s | base64 -d > \"$tmp\"", opscommon.ShellQuote(encoded)),
		"mv \"$tmp\" \"$target\"",
	}, "; ")
	command := "sh -lc " + opscommon.ShellQuote(script)
	result, err := s.execWithTimeout(ctx, target, command)
	if err != nil {
		return ValidationResult{}, fmt.Errorf("write config %s: %w", configID, err)
	}
	if result.ExitCode != 0 {
		return ValidationResult{}, fmt.Errorf("write config %s failed with exit code %d: %s", configID, result.ExitCode, firstNonEmpty(result.StderrTail, result.StdoutTail))
	}
	return validation, nil
}

func ValidateConfig(configID string, content string) ValidationResult {
	def, err := ValidateConfigID(configID)
	if err != nil {
		return ValidationResult{OK: false, Errors: []string{err.Error()}}
	}
	switch def.Format {
	case "env":
		return validateEnvConfig(def.ID, content)
	case "yaml":
		return validateIceRESTCatalogConfig(content)
	default:
		return ValidationResult{OK: false, Errors: []string{"unsupported config format"}}
	}
}

func MaskConfigContent(configID string, content string) string {
	def, err := ValidateConfigID(configID)
	if err != nil {
		return ""
	}
	switch def.Format {
	case "env":
		return maskEnvContent(content)
	case "yaml":
		return maskYAMLContent(content)
	default:
		return content
	}
}

func validateEnvConfig(configID string, content string) ValidationResult {
	result := ValidationResult{OK: true}
	entries, parseErrors := parseEnvContent(content)
	if len(parseErrors) > 0 {
		result.OK = false
		result.Errors = append(result.Errors, parseErrors...)
	}
	rule := envRules[configID]
	for _, key := range rule.requiredKeys {
		if _, ok := entries[key]; !ok {
			result.OK = false
			result.Errors = append(result.Errors, fmt.Sprintf("missing required key %s", key))
		}
	}
	if len(rule.knownKeys) == 0 {
		result.Warnings = append(result.Warnings, "no validation template is defined for this config yet")
	} else {
		for key := range entries {
			if _, ok := rule.knownKeys[key]; !ok {
				result.Warnings = append(result.Warnings, fmt.Sprintf("unknown key %s", key))
			}
		}
	}
	sort.Strings(result.Errors)
	sort.Strings(result.Warnings)
	return result
}

func validateIceRESTCatalogConfig(content string) ValidationResult {
	result := ValidationResult{OK: true}
	var payload map[string]any
	if err := yaml.Unmarshal([]byte(content), &payload); err != nil {
		result.OK = false
		result.Errors = append(result.Errors, fmt.Sprintf("invalid YAML: %v", err))
		return result
	}

	requiredTopLevel := []string{"uri", "warehouse", "s3"}
	for _, key := range requiredTopLevel {
		if _, ok := payload[key]; !ok {
			result.OK = false
			result.Errors = append(result.Errors, fmt.Sprintf("missing required key %s", key))
		}
	}
	if s3Value, ok := payload["s3"].(map[string]any); ok {
		for _, key := range []string{"endpoint", "accessKeyID", "secretAccessKey", "region"} {
			if _, exists := s3Value[key]; !exists {
				result.OK = false
				result.Errors = append(result.Errors, fmt.Sprintf("missing required key s3.%s", key))
			}
		}
	}
	if anonymousAccess, ok := payload["anonymousAccess"].(map[string]any); ok {
		if enabled, exists := anonymousAccess["enabled"].(bool); exists && enabled {
			result.Warnings = append(result.Warnings, "anonymousAccess.enabled=true is not recommended")
		}
	}
	if tokens, ok := payload["bearerTokens"].([]any); ok && len(tokens) == 0 {
		result.Warnings = append(result.Warnings, "bearerTokens is empty")
	}
	sort.Strings(result.Errors)
	sort.Strings(result.Warnings)
	return result
}

func parseEnvContent(content string) (map[string]string, []string) {
	out := make(map[string]string)
	var errs []string
	for idx, raw := range strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n") {
		lineNo := idx + 1
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(raw, "=", 2)
		if len(parts) != 2 {
			errs = append(errs, fmt.Sprintf("line %d: expected KEY=value", lineNo))
			continue
		}
		key := strings.TrimSpace(parts[0])
		if !isValidEnvKey(key) {
			errs = append(errs, fmt.Sprintf("line %d: invalid key %q", lineNo, key))
			continue
		}
		out[key] = parts[1]
	}
	return out, errs
}

func maskEnvContent(content string) string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	for idx, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(raw, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if !shouldMaskKey(key) {
			continue
		}
		lines[idx] = key + "=********"
	}
	return strings.Join(lines, "\n")
}

func maskYAMLContent(content string) string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	for idx, raw := range lines {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		colon := strings.Index(raw, ":")
		if colon <= 0 {
			continue
		}
		key := strings.TrimSpace(raw[:colon])
		if strings.HasPrefix(key, "-") {
			key = strings.TrimSpace(strings.TrimPrefix(key, "-"))
		}
		if !shouldMaskKey(key) {
			continue
		}
		indent := raw[:colon+1]
		lines[idx] = indent + " ********"
	}
	return strings.Join(lines, "\n")
}

func isValidEnvKey(key string) bool {
	if key == "" {
		return false
	}
	for idx, r := range key {
		switch {
		case idx == 0 && ((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_'):
		case idx > 0 && ((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'):
		default:
			return false
		}
	}
	return true
}

func shouldMaskKey(key string) bool {
	return secretKeyPattern.MatchString(strings.TrimSpace(key))
}

func splitNonEmptyLines(content string) []string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *Service) execWithTimeout(ctx context.Context, target sshops.SSHTarget, command string) (sshops.CommandResult, error) {
	if _, ok := ctx.Deadline(); ok {
		return s.exec.ExecuteCommand(ctx, target, command, nil)
	}
	runCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.exec.ExecuteCommand(runCtx, target, command, nil)
}
