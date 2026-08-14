package deploy

import (
	"context"
	"errors"
	"strings"
	"testing"

	opsdocker "github.com/LevonGhukas/O_Rabbit/internal/ops/docker"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

func TestValidateComponentAllowlist(t *testing.T) {
	for _, component := range []string{"master", "worker", "minio"} {
		if err := ValidateComponent(component); err != nil {
			t.Fatalf("expected supported component %q: %v", component, err)
		}
	}
	if err := ValidateComponent("postgres"); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected not implemented error for postgres, got %v", err)
	}
	if err := ValidateComponent("bad"); err == nil {
		t.Fatal("expected unsupported component error")
	}
}

func TestValidateScaleWorker(t *testing.T) {
	scale := 3
	if err := ValidateScale("worker", &scale); err != nil {
		t.Fatalf("expected valid worker scale: %v", err)
	}
	tooLarge := 51
	if err := ValidateScale("worker", &tooLarge); err == nil {
		t.Fatal("expected worker scale validation failure")
	}
	if err := ValidateScale("master", &scale); err == nil {
		t.Fatal("expected non-worker scale rejection")
	}
}

func TestPrepareDeploymentBuildsAllowlistedWorkerCommand(t *testing.T) {
	scale := 3
	svc := NewService(
		CommandExecutorFunc(func(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error) {
			return sshops.CommandResult{
				ExitCode:   0,
				StdoutTail: "deploy-master.sh=1\ndeploy-worker.sh=1\ndeploy-minio.sh=1\ndocker-compose.master.yml=1\ndocker-compose.worker.yml=1\ndocker-compose.minio.yml=1\nDockerfile.orabbit=1\n",
			}, nil
		}),
		DockerCheckerFunc(func(ctx context.Context, target sshops.SSHTarget) (opsdocker.DockerStatus, error) {
			return opsdocker.DockerStatus{Installed: true, DaemonOK: true, Version: "26.0.0"}, nil
		}),
		SSHTesterFunc(func(ctx context.Context, target sshops.SSHTarget) (sshops.TestResult, error) {
			return sshops.TestResult{HostKeyFingerprint: "SHA256:abc"}, nil
		}),
		0,
	)

	plan, err := svc.PrepareDeployment(context.Background(), sshops.SSHTarget{Host: "127.0.0.1", User: "deploy"}, "/root/O_Rabbit", "worker", DeploymentParams{Scale: &scale})
	if err != nil {
		t.Fatalf("PrepareDeployment: %v", err)
	}
	if plan.ScriptID != "deploy-worker.sh" {
		t.Fatalf("script_id=%q want deploy-worker.sh", plan.ScriptID)
	}
	if !strings.Contains(plan.Command, "WORKER_SCALE") || !strings.Contains(plan.Command, "3") {
		t.Fatalf("expected scale in command: %s", plan.Command)
	}
}
