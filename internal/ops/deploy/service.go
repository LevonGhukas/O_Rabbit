package deploy

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	opscommon "github.com/LevonGhukas/O_Rabbit/internal/ops/common"
	opsdocker "github.com/LevonGhukas/O_Rabbit/internal/ops/docker"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

var ErrNotImplemented = errors.New("deployment component is not implemented")

type CommandExecutor interface {
	ExecuteCommand(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error)
}

type CommandExecutorFunc func(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error)

func (f CommandExecutorFunc) ExecuteCommand(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error) {
	return f(ctx, target, command, onStream)
}

type DockerChecker interface {
	CheckDocker(ctx context.Context, target sshops.SSHTarget) (opsdocker.DockerStatus, error)
}

type SSHTester interface {
	TestConnection(ctx context.Context, target sshops.SSHTarget) (sshops.TestResult, error)
}

type DockerCheckerFunc func(ctx context.Context, target sshops.SSHTarget) (opsdocker.DockerStatus, error)

func (f DockerCheckerFunc) CheckDocker(ctx context.Context, target sshops.SSHTarget) (opsdocker.DockerStatus, error) {
	return f(ctx, target)
}

type SSHTesterFunc func(ctx context.Context, target sshops.SSHTarget) (sshops.TestResult, error)

func (f SSHTesterFunc) TestConnection(ctx context.Context, target sshops.SSHTarget) (sshops.TestResult, error) {
	return f(ctx, target)
}

type Service struct {
	exec    CommandExecutor
	docker  DockerChecker
	ssh     SSHTester
	timeout time.Duration
}

type DeploymentParams struct {
	Scale *int `json:"scale,omitempty"`
}

type PreparedDeployment struct {
	Component   string `json:"component"`
	ScriptID    string `json:"script_id"`
	AllowlistID string `json:"allowlist_id"`
	Command     string `json:"-"`
}

type ProjectValidation struct {
	OK         bool            `json:"ok"`
	ProjectDir string          `json:"project_dir"`
	Files      map[string]bool `json:"files"`
	Errors     []string        `json:"errors"`
}

var componentFiles = map[string][]string{
	"master": {"deploy-master.sh", "docker-compose.master.yml", "Dockerfile.orabbit"},
	"worker": {"deploy-worker.sh", "docker-compose.worker.yml", "Dockerfile.orabbit"},
	"minio":  {"deploy-minio.sh", "docker-compose.minio.yml"},
}

var validationFiles = []string{
	"deploy-master.sh",
	"deploy-worker.sh",
	"deploy-minio.sh",
	"docker-compose.master.yml",
	"docker-compose.worker.yml",
	"docker-compose.minio.yml",
	"Dockerfile.orabbit",
}

func NewService(exec CommandExecutor, docker DockerChecker, ssh SSHTester, timeout time.Duration) *Service {
	if exec == nil {
		exec = CommandExecutorFunc(sshops.ExecuteCommand)
	}
	if docker == nil {
		docker = DockerCheckerFunc(opsdocker.CheckDocker)
	}
	if ssh == nil {
		ssh = SSHTesterFunc(sshops.TestConnection)
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Service{exec: exec, docker: docker, ssh: ssh, timeout: timeout}
}

func ValidateComponent(component string) error {
	switch strings.TrimSpace(component) {
	case "master", "worker", "minio":
		return nil
	case "postgres", "ice-rest-catalog":
		return fmt.Errorf("%w: %s", ErrNotImplemented, component)
	default:
		return fmt.Errorf("unsupported deployment component %q", component)
	}
}

func ValidateScale(component string, scale *int) error {
	switch strings.TrimSpace(component) {
	case "worker":
		if scale == nil {
			return nil
		}
		if *scale < 1 || *scale > 50 {
			return fmt.Errorf("worker scale must be between 1 and 50")
		}
		return nil
	case "master", "minio", "postgres", "ice-rest-catalog":
		if scale != nil {
			return fmt.Errorf("scale is only supported for worker deployments")
		}
		return nil
	default:
		return fmt.Errorf("unsupported deployment component %q", component)
	}
}

func (s *Service) ValidateProject(ctx context.Context, target sshops.SSHTarget, projectDir string) (ProjectValidation, error) {
	projectDir = strings.TrimSpace(projectDir)
	if projectDir == "" {
		return ProjectValidation{}, fmt.Errorf("missing project_dir")
	}
	if _, err := opscommon.ResolveUnderProject(projectDir, "Dockerfile.orabbit"); err != nil {
		return ProjectValidation{}, err
	}

	lines := []string{
		"set -eu",
		fmt.Sprintf("if [ ! -d %s ]; then echo %s; exit 0; fi", opscommon.ShellQuote(projectDir), opscommon.ShellQuote("__project_dir_missing__")),
		fmt.Sprintf("cd %s", opscommon.ShellQuote(projectDir)),
	}
	for _, file := range validationFiles {
		lines = append(lines, fmt.Sprintf("if [ -e %s ]; then echo %s; else echo %s; fi",
			opscommon.ShellQuote(file),
			opscommon.ShellQuote(file+"=1"),
			opscommon.ShellQuote(file+"=0"),
		))
	}
	command := "sh -lc " + opscommon.ShellQuote(strings.Join(lines, "; "))
	result, err := s.execWithTimeout(ctx, target, command, nil)
	if err != nil {
		return ProjectValidation{}, fmt.Errorf("validate project directory: %w", err)
	}
	if result.ExitCode != 0 {
		return ProjectValidation{}, fmt.Errorf("validate project directory failed with exit code %d: %s", result.ExitCode, firstNonEmpty(result.StderrTail, result.StdoutTail))
	}
	out := ProjectValidation{
		OK:         true,
		ProjectDir: projectDir,
		Files:      make(map[string]bool, len(validationFiles)),
	}
	for _, line := range splitNonEmptyLines(result.StdoutTail) {
		if line == "__project_dir_missing__" {
			out.OK = false
			out.Errors = append(out.Errors, "project directory does not exist")
			return out, nil
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		exists := strings.TrimSpace(parts[1]) == "1"
		out.Files[strings.TrimSpace(parts[0])] = exists
	}
	for _, file := range validationFiles {
		if !out.Files[file] {
			out.OK = false
			out.Errors = append(out.Errors, fmt.Sprintf("missing %s", file))
		}
	}
	return out, nil
}

func (s *Service) PrepareDeployment(ctx context.Context, target sshops.SSHTarget, projectDir string, component string, params DeploymentParams) (PreparedDeployment, error) {
	component = strings.TrimSpace(component)
	if err := ValidateComponent(component); err != nil {
		return PreparedDeployment{}, err
	}
	if err := ValidateScale(component, params.Scale); err != nil {
		return PreparedDeployment{}, err
	}
	if _, err := s.ssh.TestConnection(ctx, target); err != nil {
		return PreparedDeployment{}, fmt.Errorf("ssh validation failed: %w", err)
	}
	if _, err := s.docker.CheckDocker(ctx, target); err != nil {
		return PreparedDeployment{}, fmt.Errorf("docker validation failed: %w", err)
	}
	projectCheck, err := s.ValidateProject(ctx, target, projectDir)
	if err != nil {
		return PreparedDeployment{}, err
	}
	if !projectCheck.OK {
		return PreparedDeployment{}, fmt.Errorf("project validation failed: %s", strings.Join(projectCheck.Errors, "; "))
	}
	for _, file := range componentFiles[component] {
		if !projectCheck.Files[file] {
			return PreparedDeployment{}, fmt.Errorf("project validation failed: missing %s", file)
		}
	}

	plan := PreparedDeployment{
		Component:   component,
		AllowlistID: "deploy-" + component,
	}
	switch component {
	case "master":
		plan.ScriptID = "deploy-master.sh"
		plan.Command = buildScriptCommand(projectDir, ".env.master", plan.ScriptID, nil)
	case "worker":
		plan.ScriptID = "deploy-worker.sh"
		scale := 1
		if params.Scale != nil {
			scale = *params.Scale
		}
		plan.Command = buildScriptCommand(projectDir, ".env.worker", plan.ScriptID, map[string]string{"WORKER_SCALE": fmt.Sprintf("%d", scale)})
	case "minio":
		plan.ScriptID = "deploy-minio.sh"
		plan.Command = buildScriptCommand(projectDir, ".env.minio", plan.ScriptID, nil)
	default:
		return PreparedDeployment{}, fmt.Errorf("%w: %s", ErrNotImplemented, component)
	}
	return plan, nil
}

func buildScriptCommand(projectDir string, envFile string, scriptID string, extraEnv map[string]string) string {
	scriptLines := []string{
		"set -eu",
		fmt.Sprintf("cd %s", opscommon.ShellQuote(projectDir)),
	}
	envAssignments := []string{
		fmt.Sprintf("ORABBIT_ENV_FILE=%s", opscommon.ShellQuote(envFile)),
	}
	keys := make([]string, 0, len(extraEnv))
	for key := range extraEnv {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		envAssignments = append(envAssignments, fmt.Sprintf("%s=%s", key, opscommon.ShellQuote(extraEnv[key])))
	}
	scriptLines = append(scriptLines, strings.Join(envAssignments, " ")+" ./"+scriptID)
	return "sh -lc " + opscommon.ShellQuote(strings.Join(scriptLines, "; "))
}

func (s *Service) execWithTimeout(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error) {
	if _, ok := ctx.Deadline(); ok {
		return s.exec.ExecuteCommand(ctx, target, command, onStream)
	}
	runCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.exec.ExecuteCommand(runCtx, target, command, onStream)
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
