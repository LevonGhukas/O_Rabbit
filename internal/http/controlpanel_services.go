package httpapi

import (
	"context"
	"time"

	opsconfigs "github.com/LevonGhukas/O_Rabbit/internal/ops/configs"
	opsdeploy "github.com/LevonGhukas/O_Rabbit/internal/ops/deploy"
	opsdocker "github.com/LevonGhukas/O_Rabbit/internal/ops/docker"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

type sshTester interface {
	TestConnection(ctx context.Context, target sshops.SSHTarget) (sshops.TestResult, error)
}

type sshCommandExecutor interface {
	ExecuteCommand(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error)
}

type dockerService interface {
	CheckDocker(ctx context.Context, target sshops.SSHTarget) (opsdocker.DockerStatus, error)
	ListContainers(ctx context.Context, target sshops.SSHTarget) ([]opsdocker.ContainerInfo, error)
	InspectContainer(ctx context.Context, target sshops.SSHTarget, containerRef string) (opsdocker.ContainerInfo, error)
	StartContainer(ctx context.Context, target sshops.SSHTarget, containerRef string) (sshops.CommandResult, error)
	StopContainer(ctx context.Context, target sshops.SSHTarget, containerRef string) (sshops.CommandResult, error)
	RestartContainer(ctx context.Context, target sshops.SSHTarget, containerRef string) (sshops.CommandResult, error)
	TailContainerLogs(ctx context.Context, target sshops.SSHTarget, containerRef string, tail int) ([]opsdocker.LogLine, error)
	StreamContainerLogs(ctx context.Context, target sshops.SSHTarget, containerRef string, tail int, onStream func(opsdocker.LogLine)) (sshops.CommandResult, error)
}

type configService interface {
	ListEditableConfigs(ctx context.Context, target sshops.SSHTarget, projectDir string) ([]opsconfigs.ListedConfig, error)
	ReadConfig(ctx context.Context, target sshops.SSHTarget, projectDir string, configID string) (opsconfigs.ReadResult, error)
	UpdateConfig(ctx context.Context, target sshops.SSHTarget, projectDir string, configID string, content string) (opsconfigs.ValidationResult, error)
}

type deployService interface {
	ValidateProject(ctx context.Context, target sshops.SSHTarget, projectDir string) (opsdeploy.ProjectValidation, error)
	PrepareDeployment(ctx context.Context, target sshops.SSHTarget, projectDir string, component string, params opsdeploy.DeploymentParams) (opsdeploy.PreparedDeployment, error)
}

type sshTesterFunc func(ctx context.Context, target sshops.SSHTarget) (sshops.TestResult, error)

func (f sshTesterFunc) TestConnection(ctx context.Context, target sshops.SSHTarget) (sshops.TestResult, error) {
	return f(ctx, target)
}

type sshCommandExecutorFunc func(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error)

func (f sshCommandExecutorFunc) ExecuteCommand(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error) {
	return f(ctx, target, command, onStream)
}

func newDockerService(exec sshCommandExecutor) dockerService {
	var cmdExec opsdocker.CommandExecutor
	if exec != nil {
		cmdExec = opsdocker.CommandExecutorFunc(exec.ExecuteCommand)
	}
	return opsdocker.NewService(cmdExec, 30*time.Second)
}

func newConfigService(exec sshCommandExecutor) configService {
	var cmdExec opsconfigs.CommandExecutor
	if exec != nil {
		cmdExec = opsconfigs.CommandExecutorFunc(exec.ExecuteCommand)
	}
	return opsconfigs.NewService(cmdExec, 30*time.Second)
}

func newDeployService(exec sshCommandExecutor, docker dockerService, tester sshTester) deployService {
	var cmdExec opsdeploy.CommandExecutor
	if exec != nil {
		cmdExec = opsdeploy.CommandExecutorFunc(exec.ExecuteCommand)
	}
	var dockerChecker opsdeploy.DockerChecker
	if docker != nil {
		dockerChecker = docker
	}
	var sshConnTester opsdeploy.SSHTester
	if tester != nil {
		sshConnTester = tester
	}
	return opsdeploy.NewService(cmdExec, dockerChecker, sshConnTester, 30*time.Second)
}
