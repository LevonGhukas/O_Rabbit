package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	opscommon "github.com/LevonGhukas/O_Rabbit/internal/ops/common"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

var containerRefPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

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

type DockerStatus struct {
	Installed      bool   `json:"installed"`
	DaemonOK       bool   `json:"daemon_ok"`
	Version        string `json:"version,omitempty"`
	ComposeVersion string `json:"compose_version,omitempty"`
}

type ContainerInfo struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Image     string `json:"image"`
	State     string `json:"state"`
	Status    string `json:"status"`
	Health    string `json:"health"`
	Ports     string `json:"ports,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

type LogLine struct {
	Line string `json:"line"`
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

func CheckDocker(ctx context.Context, target sshops.SSHTarget) (DockerStatus, error) {
	return NewService(nil, 30*time.Second).CheckDocker(ctx, target)
}

func (s *Service) CheckDocker(ctx context.Context, target sshops.SSHTarget) (DockerStatus, error) {
	var status DockerStatus

	versionResult, err := s.execWithTimeout(ctx, target, `docker version --format '{{.Server.Version}}'`, nil)
	if err != nil {
		return DockerStatus{}, fmt.Errorf("docker version failed: %w", err)
	}
	if versionResult.ExitCode != 0 {
		return DockerStatus{}, fmt.Errorf("docker version failed with exit code %d: %s", versionResult.ExitCode, firstNonEmpty(versionResult.StderrTail, versionResult.StdoutTail))
	}
	version := strings.TrimSpace(firstNonEmpty(versionResult.StdoutTail, versionResult.StderrTail))
	if version == "" || version == "<no value>" {
		return DockerStatus{}, fmt.Errorf("docker version returned no server version")
	}

	composeResult, err := s.execWithTimeout(ctx, target, `docker compose version --short`, nil)
	if err != nil {
		return DockerStatus{}, fmt.Errorf("docker compose version failed: %w", err)
	}
	if composeResult.ExitCode != 0 {
		return DockerStatus{}, fmt.Errorf("docker compose version failed with exit code %d: %s", composeResult.ExitCode, firstNonEmpty(composeResult.StderrTail, composeResult.StdoutTail))
	}
	composeVersion := strings.TrimSpace(firstNonEmpty(composeResult.StdoutTail, composeResult.StderrTail))

	status.Installed = true
	status.DaemonOK = true
	status.Version = version
	status.ComposeVersion = composeVersion
	return status, nil
}

func (s *Service) ListContainers(ctx context.Context, target sshops.SSHTarget) ([]ContainerInfo, error) {
	result, err := s.execWithTimeout(ctx, target, `docker ps -a --no-trunc --format '{{json .}}'`, nil)
	if err != nil {
		return nil, fmt.Errorf("list docker containers: %w", err)
	}
	if result.ExitCode != 0 {
		return nil, fmt.Errorf("list docker containers failed with exit code %d: %s", result.ExitCode, firstNonEmpty(result.StderrTail, result.StdoutTail))
	}

	lines := splitNonEmptyLines(result.StdoutTail)
	out := make([]ContainerInfo, 0, len(lines))
	for _, line := range lines {
		var raw struct {
			ID        string `json:"ID"`
			Names     string `json:"Names"`
			Image     string `json:"Image"`
			State     string `json:"State"`
			Status    string `json:"Status"`
			Ports     string `json:"Ports"`
			CreatedAt string `json:"CreatedAt"`
		}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return nil, fmt.Errorf("parse docker ps output: %w", err)
		}
		out = append(out, ContainerInfo{
			ID:        raw.ID,
			Name:      raw.Names,
			Image:     raw.Image,
			State:     strings.ToLower(strings.TrimSpace(raw.State)),
			Status:    raw.Status,
			Health:    inferHealth(raw.Status),
			Ports:     raw.Ports,
			CreatedAt: raw.CreatedAt,
		})
	}
	return out, nil
}

func (s *Service) InspectContainer(ctx context.Context, target sshops.SSHTarget, containerRef string) (ContainerInfo, error) {
	if err := ValidateContainerRef(containerRef); err != nil {
		return ContainerInfo{}, err
	}
	command := fmt.Sprintf("docker inspect --type container %s", opscommon.ShellQuote(containerRef))
	result, err := s.execWithTimeout(ctx, target, command, nil)
	if err != nil {
		return ContainerInfo{}, fmt.Errorf("inspect container: %w", err)
	}
	if result.ExitCode != 0 {
		return ContainerInfo{}, fmt.Errorf("inspect container failed with exit code %d: %s", result.ExitCode, firstNonEmpty(result.StderrTail, result.StdoutTail))
	}
	var payload []struct {
		ID     string `json:"Id"`
		Name   string `json:"Name"`
		Config struct {
			Image string `json:"Image"`
		} `json:"Config"`
		State struct {
			Status string `json:"Status"`
			Health *struct {
				Status string `json:"Status"`
			} `json:"Health"`
		} `json:"State"`
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"Ports"`
		} `json:"NetworkSettings"`
		Created string `json:"Created"`
	}
	if err := json.Unmarshal([]byte(result.StdoutTail), &payload); err != nil {
		return ContainerInfo{}, fmt.Errorf("parse docker inspect output: %w", err)
	}
	if len(payload) == 0 {
		return ContainerInfo{}, fmt.Errorf("docker inspect returned no containers")
	}
	item := payload[0]
	ports := make([]string, 0, len(item.NetworkSettings.Ports))
	for containerPort, binds := range item.NetworkSettings.Ports {
		if len(binds) == 0 {
			ports = append(ports, containerPort)
			continue
		}
		for _, bind := range binds {
			ports = append(ports, bind.HostIP+":"+bind.HostPort+"->"+containerPort)
		}
	}
	health := "none"
	if item.State.Health != nil {
		health = strings.ToLower(strings.TrimSpace(item.State.Health.Status))
	}
	return ContainerInfo{
		ID:        item.ID,
		Name:      strings.TrimPrefix(item.Name, "/"),
		Image:     item.Config.Image,
		State:     strings.ToLower(strings.TrimSpace(item.State.Status)),
		Status:    strings.ToLower(strings.TrimSpace(item.State.Status)),
		Health:    health,
		Ports:     strings.Join(ports, ", "),
		CreatedAt: item.Created,
	}, nil
}

func (s *Service) StartContainer(ctx context.Context, target sshops.SSHTarget, containerRef string) (sshops.CommandResult, error) {
	return s.containerAction(ctx, target, "start", containerRef)
}

func (s *Service) StopContainer(ctx context.Context, target sshops.SSHTarget, containerRef string) (sshops.CommandResult, error) {
	return s.containerAction(ctx, target, "stop", containerRef)
}

func (s *Service) RestartContainer(ctx context.Context, target sshops.SSHTarget, containerRef string) (sshops.CommandResult, error) {
	return s.containerAction(ctx, target, "restart", containerRef)
}

func (s *Service) TailContainerLogs(ctx context.Context, target sshops.SSHTarget, containerRef string, tail int) ([]LogLine, error) {
	if err := ValidateContainerRef(containerRef); err != nil {
		return nil, err
	}
	tail = normalizeTail(tail)
	command := fmt.Sprintf("docker logs --timestamps --tail %d %s 2>&1", tail, opscommon.ShellQuote(containerRef))
	result, err := s.execWithTimeout(ctx, target, command, nil)
	if err != nil {
		return nil, fmt.Errorf("tail container logs: %w", err)
	}
	if result.ExitCode != 0 {
		return nil, fmt.Errorf("tail container logs failed with exit code %d: %s", result.ExitCode, firstNonEmpty(result.StderrTail, result.StdoutTail))
	}
	lines := splitNonEmptyLines(firstNonEmpty(result.StdoutTail, result.StderrTail, result.StdoutTail+"\n"+result.StderrTail))
	out := make([]LogLine, 0, len(lines))
	for _, line := range lines {
		out = append(out, LogLine{Line: line})
	}
	return out, nil
}

func (s *Service) StreamContainerLogs(ctx context.Context, target sshops.SSHTarget, containerRef string, tail int, onStream func(LogLine)) (sshops.CommandResult, error) {
	if err := ValidateContainerRef(containerRef); err != nil {
		return sshops.CommandResult{}, err
	}
	tail = normalizeTail(tail)
	command := fmt.Sprintf("docker logs --timestamps -f --tail %d %s 2>&1", tail, opscommon.ShellQuote(containerRef))
	return s.exec.ExecuteCommand(ctx, target, command, func(chunk sshops.StreamChunk) {
		if onStream == nil {
			return
		}
		for _, line := range splitNonEmptyLines(chunk.Data) {
			onStream(LogLine{Line: line})
		}
	})
}

func ValidateContainerRef(containerRef string) error {
	ref := strings.TrimSpace(containerRef)
	if ref == "" {
		return fmt.Errorf("missing container reference")
	}
	if !containerRefPattern.MatchString(ref) {
		return fmt.Errorf("invalid container reference")
	}
	return nil
}

func (s *Service) containerAction(ctx context.Context, target sshops.SSHTarget, action string, containerRef string) (sshops.CommandResult, error) {
	if err := ValidateContainerRef(containerRef); err != nil {
		return sshops.CommandResult{}, err
	}
	switch action {
	case "start", "stop", "restart":
	default:
		return sshops.CommandResult{}, fmt.Errorf("unsupported container action %q", action)
	}
	command := fmt.Sprintf("docker %s %s", action, opscommon.ShellQuote(containerRef))
	result, err := s.execWithTimeout(ctx, target, command, nil)
	if err != nil {
		return sshops.CommandResult{}, fmt.Errorf("%s container: %w", action, err)
	}
	if result.ExitCode != 0 {
		return result, fmt.Errorf("%s container failed with exit code %d: %s", action, result.ExitCode, firstNonEmpty(result.StderrTail, result.StdoutTail))
	}
	return result, nil
}

func (s *Service) execWithTimeout(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error) {
	if _, ok := ctx.Deadline(); ok {
		return s.exec.ExecuteCommand(ctx, target, command, onStream)
	}
	runCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.exec.ExecuteCommand(runCtx, target, command, onStream)
}

func inferHealth(status string) string {
	lower := strings.ToLower(status)
	switch {
	case strings.Contains(lower, "(healthy)") || strings.Contains(lower, "healthy"):
		return "healthy"
	case strings.Contains(lower, "(unhealthy)") || strings.Contains(lower, "unhealthy"):
		return "unhealthy"
	case strings.Contains(lower, "starting"):
		return "unknown"
	default:
		return "none"
	}
}

func splitNonEmptyLines(content string) []string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
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

func normalizeTail(tail int) int {
	if tail <= 0 {
		return 200
	}
	if tail > 10000 {
		return 10000
	}
	return tail
}

func ParseTailParam(raw string, def int) int {
	if strings.TrimSpace(raw) == "" {
		return normalizeTail(def)
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return normalizeTail(def)
	}
	return normalizeTail(n)
}
